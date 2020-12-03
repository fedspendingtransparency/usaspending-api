DELETE FROM public.reporting_agency_missing_tas;
ALTER SEQUENCE reporting_agency_missing_tas_reporting_agency_missing_tas_id_seq RESTART WITH 1;

INSERT INTO public.reporting_agency_missing_tas (
    toptier_code,
    fiscal_year,
    fiscal_period,
    tas_rendering_label,
    obligated_amount
)
WITH missing AS (
    SELECT
        gtas.id
    FROM appropriation_account_balances AS aab 
    INNER JOIN submission_attributes AS sa
        ON aab.submission_id = sa.submission_id
    RIGHT OUTER JOIN gtas_sf133_balances AS gtas
        ON sa.reporting_fiscal_period = gtas.fiscal_period
        AND sa.reporting_fiscal_year = gtas.fiscal_year
        AND aab.treasury_account_identifier = gtas.treasury_account_identifier
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
