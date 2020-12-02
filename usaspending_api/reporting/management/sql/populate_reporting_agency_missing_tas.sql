DELETE FROM public.reporting_agency_missing_tas;
ALTER SEQUENCE reporting_agency_missing_tas_reporting_agency_missing_tas_id_seq RESTART WITH 1;

INSERT INTO public.reporting_agency_missing_tas (
    fiscal_period,
    fiscal_year,
    tas_rendering_label,
    toptier_code,
    obligated_amount
)
SELECT
    sa.reporting_fiscal_period AS fiscal_period,
    sa.reporting_fiscal_year AS fiscal_year,
    aab.treasury_account_identifier AS tas_id,
    SUM(obligations_incurred_total_by_tas_cpe) AS obligated_amount
FROM appropriation_account_balances AS aab
INNER JOIN submission_attributes AS sa
    ON sa.submission_id = aab.submission_id
RIGHT OUTER JOIN gtas_sf133_balances AS gsb
    ON sa.reporting_fiscal_period = gsb.fiscal_period
    AND sa.reporting_fiscal_year = gsb.fiscal_year
    AND aab.treasury_account_identifier = gsb.treasury_account_identifier
WHERE
    aab.treasury_account_identifier IS NULL
GROUP BY sa.reporting_fiscal_period, sa.reporting_fiscal_year, aab.treasury_account_identifier
;
