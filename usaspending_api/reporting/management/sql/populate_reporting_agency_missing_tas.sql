DELETE FROM public.reporting_agency_missing_tas;
ALTER SEQUENCE reporting_agency_missing_tas_reporting_agency_missing_tas_id_seq RESTART WITH 1;

INSERT INTO public.reporting_agency_missing_tas (
    toptier_code,
    fiscal_year,
    fiscal_period,
    tas_rendering_label,
    obligated_amount
)
SELECT
    ta.toptier_code,
    sa.reporting_fiscal_year AS fiscal_year,
    sa.reporting_fiscal_period AS fiscal_period,
    taa.tas_rendering_label,
    SUM(obligations_incurred_total_by_tas_cpe) AS obligated_amount
FROM appropriation_account_balances AS aab
INNER JOIN submission_attributes AS sa
    ON aab.submission_id = sa.submission_id
INNER JOIN treasury_appropriation_account AS taa
    ON aab.treasury_account_identifier = taa.treasury_account_identifier
INNER JOIN toptier_agency AS ta
    ON taa.funding_toptier_agency_id = ta.toptier_agency_id
RIGHT OUTER JOIN gtas_sf133_balances AS gsb
    ON sa.reporting_fiscal_period = gsb.fiscal_period
    AND sa.reporting_fiscal_year = gsb.fiscal_year
    AND aab.treasury_account_identifier = gsb.treasury_account_identifier
WHERE
    aab.treasury_account_identifier IS NULL
GROUP BY sa.reporting_fiscal_period, sa.reporting_fiscal_year, taa.tas_rendering_label, ta.toptier_code
;
