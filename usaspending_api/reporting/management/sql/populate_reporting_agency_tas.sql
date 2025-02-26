DELETE FROM public.reporting_agency_tas;
ALTER SEQUENCE reporting_agency_tas_reporting_agency_tas_id_seq RESTART WITH 1;

INSERT INTO public.reporting_agency_tas (
    fiscal_period,
    fiscal_year,
    tas_rendering_label,
    toptier_code,
    appropriation_obligated_amount,
    object_class_pa_obligated_amount,
    diff_approp_ocpa_obligated_amounts
)
SELECT *
FROM (
    WITH summed_appropriation AS (
        SELECT
            sa.reporting_fiscal_period AS fiscal_period,
            sa.reporting_fiscal_year AS fiscal_year,
            treasury_account_identifier AS tas_id,
            SUM(obligations_incurred_total_by_tas_cpe) AS appropriation_obligated_amount
        FROM appropriation_account_balances AS aab
        INNER JOIN submission_attributes AS sa
            ON sa.submission_id = aab.submission_id
        INNER JOIN dabs_submission_window_schedule AS dsws
            ON sa.submission_window_id = dsws.id
            AND dsws.submission_reveal_date <= now()
        GROUP BY sa.reporting_fiscal_period, sa.reporting_fiscal_year, treasury_account_identifier),
    summed_object_class_program_activity AS (
        SELECT
            sa.reporting_fiscal_period AS fiscal_period,
            sa.reporting_fiscal_year AS fiscal_year,
            treasury_account_id AS tas_id,
            SUM(
                CASE
                    WHEN prior_year_adjustment = 'X' OR prior_year_adjustment IS NULL
                        THEN obligations_incurred_by_program_object_class_cpe
                    ELSE 0
                END
            ) AS object_class_pa_obligated_amount
        FROM financial_accounts_by_program_activity_object_class AS fapaoc
        INNER JOIN submission_attributes AS sa
            ON sa.submission_id = fapaoc.submission_id
        INNER JOIN dabs_submission_window_schedule AS dsws
            ON sa.submission_window_id = dsws.id
            AND dsws.submission_reveal_date <= now()
        GROUP BY sa.reporting_fiscal_period, sa.reporting_fiscal_year, treasury_account_id)
    SELECT
        sa.fiscal_period,
        sa.fiscal_year,
        taa.tas_rendering_label,
        toptier_code,
        appropriation_obligated_amount,
        object_class_pa_obligated_amount,
        (appropriation_obligated_amount - object_class_pa_obligated_amount) AS diff_approp_ocpa_obligated_amounts
    FROM summed_appropriation AS sa
    INNER JOIN summed_object_class_program_activity AS socpa
        ON socpa.tas_id = sa.tas_id
        AND socpa.fiscal_period = sa.fiscal_period
        AND socpa.fiscal_year = sa.fiscal_year
    INNER JOIN treasury_appropriation_account AS taa
        ON sa.tas_id = taa.treasury_account_identifier
    INNER JOIN toptier_agency AS ta
        ON taa.funding_toptier_agency_id = ta.toptier_agency_id
) AS reporting_agency_tas_content;
