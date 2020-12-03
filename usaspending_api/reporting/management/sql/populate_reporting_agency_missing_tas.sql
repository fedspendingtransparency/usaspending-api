DELETE FROM public.reporting_agency_missing_tas;
ALTER SEQUENCE reporting_agency_missing_tas_reporting_agency_missing_tas_id_seq RESTART WITH 1;

INSERT INTO public.reporting_agency_missing_tas (
    toptier_code,
    fiscal_year,
    fiscal_period,
    tas_rendering_label,
    obligated_amount
)
WITH app AS (
    SELECT
        aab.treasury_account_identifier,
        sa.reporting_fiscal_year AS fiscal_year,
        sa.reporting_fiscal_period AS fiscal_period
    FROM appropriation_account_balances AS aab 
    INNER JOIN submission_attributes AS sa
        ON aab.submission_id = sa.submission_id
)
SELECT
    ta.toptier_code,
    app.fiscal_year,
    app.fiscal_period,
    taa.tas_rendering_label,
    SUM(gtas.obligations_incurred_total_cpe) AS obligated_amount
FROM gtas_sf133_balances AS gtas
INNER JOIN treasury_appropriation_account AS taa
    ON gtas.treasury_account_identifier = taa.treasury_account_identifier
INNER JOIN toptier_agency AS ta
    ON taa.funding_toptier_agency_id = ta.toptier_agency_id
LEFT OUTER JOIN app
    ON app.fiscal_period = gtas.fiscal_period
    AND app.fiscal_year = gtas.fiscal_year
    AND app.treasury_account_identifier = gtas.treasury_account_identifier
WHERE
    app.treasury_account_identifier IS NULL
GROUP BY ta.toptier_code,
    app.fiscal_year,
    app.fiscal_period,
    taa.tas_rendering_label
;
