BEGIN;
DELETE FROM public.reporting_agency_overview;
ALTER SEQUENCE reporting_agency_overview_reporting_agency_overview_id_seq RESTART WITH 1;

INSERT INTO public.reporting_agency_overview (
    fiscal_period,
    fiscal_year,
    toptier_code,
    total_dollars_obligated_gtas,
    total_budgetary_resources,
    total_diff_approp_ocpa_obligated_amounts
)
SELECT *
FROM (
    WITH sum_reporting_agency_tas AS (
        SELECT
            fiscal_period,
            fiscal_year,
            toptier_code,
            SUM(diff_approp_ocpa_obligated_amounts) AS total_diff_approp_ocpa_obligated_amounts
        FROM reporting_agency_tas
        GROUP BY fiscal_period, fiscal_year, toptier_code),
    sum_budgetary_resources AS (
        SELECT
            reporting_fiscal_period,
            reporting_fiscal_year,
            ta.toptier_code,
            SUM(total_budgetary_resources_amount_cpe) AS total_budgetary_resources
        FROM appropriation_account_balances AS aab
        JOIN submission_attributes AS sa
            ON sa.submission_id = aab.submission_id
        JOIN treasury_appropriation_account AS taa
            ON aab.treasury_account_identifier = taa.treasury_account_identifier
        JOIN toptier_agency AS ta
            ON taa.funding_toptier_agency_id = ta.toptier_agency_id
        GROUP BY reporting_fiscal_year, reporting_fiscal_period, ta.toptier_code),
    sum_gtas_obligations AS (
        SELECT
            gtas.fiscal_year,
            gtas.fiscal_period,
            toptier_code,
            SUM(obligations_incurred_total_cpe) AS total_dollars_obligated_gtas
        FROM gtas_sf133_balances AS gtas
        JOIN treasury_appropriation_account AS taa
            ON gtas.treasury_account_identifier = taa.treasury_account_identifier
        JOIN toptier_agency AS ta
            ON taa.funding_toptier_agency_id = ta.toptier_agency_id
        GROUP BY fiscal_year, fiscal_period, toptier_code)
    SELECT
        srat.fiscal_period,
        srat.fiscal_year,
        srat.toptier_code,
        COALESCE(total_dollars_obligated_gtas, 0) AS total_dollars_obligated_gtas,
        total_budgetary_resources,
        total_diff_approp_ocpa_obligated_amounts
    FROM sum_reporting_agency_tas AS srat
    JOIN sum_budgetary_resources AS sbr
        ON sbr.reporting_fiscal_year = srat.fiscal_year
        AND sbr.reporting_fiscal_period = srat.fiscal_period
        AND sbr.toptier_code = srat.toptier_code
    LEFT OUTER JOIN sum_gtas_obligations AS sgo
        ON sgo.fiscal_year = srat.fiscal_year
        AND sgo.fiscal_period = srat.fiscal_period
        AND sgo.toptier_code = srat.toptier_code
) AS reporting_agency_overview_content;
COMMIT;
