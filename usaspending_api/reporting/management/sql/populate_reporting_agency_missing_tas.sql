DELETE FROM public.reporting_agency_missing_tas;
ALTER SEQUENCE reporting_agency_missing_tas_reporting_agency_missing_tas_i_seq RESTART WITH 1;

INSERT INTO public.reporting_agency_missing_tas (
    toptier_code,
    fiscal_year,
    fiscal_period,
    tas_rendering_label,
    obligated_amount
)

WITH missing AS (
    SELECT
        limited_gtas.id
    FROM appropriation_account_balances AS aab
    /*
        Don't limit submissions here so that we can make sure to pair
        GTAS with Submissions. Since we use a RIGHT OUTER JOIN below
        the limiting factor is on the GTAS selected.
    */
    INNER JOIN submission_attributes AS sa
        ON aab.submission_id = sa.submission_id
    RIGHT OUTER JOIN (
        SELECT
            gtas.id,
            gtas.fiscal_year,
            gtas.fiscal_period,
            gtas.treasury_account_identifier
        FROM
            gtas_sf133_balances AS gtas
        WHERE
            /*
                ----- For GTAS that are not associated to a submission. -----
                Check that there is a submission_reveal_date that is prior to the current
                date for the fiscal_year and fiscal_period on the GTAS record. Since GTAS
                are submitted each period and we have no way to tie them back to a
                submission window this is a work around to try and limit them to only
                closed submissions.
            */
            EXISTS (
                SELECT 1
                FROM dabs_submission_window_schedule dsws
                WHERE dsws.submission_reveal_date <= now()
                    AND (
                        gtas.fiscal_year < dsws.submission_fiscal_year
                        OR (
                            gtas.fiscal_year = dsws.submission_fiscal_year
                            AND gtas.fiscal_period <= dsws.submission_fiscal_month
                        )
                    )
            )
    ) AS limited_gtas ON (
        sa.reporting_fiscal_period = limited_gtas.fiscal_period
        AND sa.reporting_fiscal_year = limited_gtas.fiscal_year
        AND aab.treasury_account_identifier = limited_gtas.treasury_account_identifier
    )
    WHERE
        aab.submission_id IS NULL
)
SELECT
    ta.toptier_code,
    gtas.fiscal_year,
    gtas.fiscal_period,
    taa.tas_rendering_label,
    SUM(gtas.obligations_incurred_total_cpe) AS obligated_amount
FROM gtas_sf133_balances AS gtas
INNER JOIN missing
    ON gtas.id = missing.id
INNER JOIN treasury_appropriation_account AS taa
    ON gtas.treasury_account_identifier = taa.treasury_account_identifier
INNER JOIN toptier_agency AS ta
    ON taa.funding_toptier_agency_id = ta.toptier_agency_id
GROUP BY ta.toptier_code,
    gtas.fiscal_year,
    gtas.fiscal_period,
    taa.tas_rendering_label
;
