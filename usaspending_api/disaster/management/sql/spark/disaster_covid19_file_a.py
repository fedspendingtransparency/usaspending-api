file_a_sql_string = """
SELECT
    agency.name AS owning_agency_name,
    CONCAT('FY', gtas.fiscal_year, 'P', LPAD(STRING(gtas.fiscal_period), 2, '0')) AS submission_period,
    COALESCE(taa.allocation_transfer_agency_id,
        CASE WHEN SIZE(SPLIT(gtas.tas_rendering_label, '-')) = 5
        THEN ELEMENT_AT(SPLIT(gtas.tas_rendering_label, '-'), 1)
        ELSE NULL END
    ) AS allocation_transfer_agency_identifier_code,
    COALESCE(taa.agency_id,
        CASE WHEN SIZE(SPLIT(gtas.tas_rendering_label, '-')) = 5
        THEN ELEMENT_AT(SPLIT(gtas.tas_rendering_label, '-'), 2)
        ELSE ELEMENT_AT(SPLIT(gtas.tas_rendering_label, '-'), 1) END
    ) AS agency_identifier_code,
    COALESCE(taa.beginning_period_of_availability,
        CASE WHEN ELEMENT_AT(SPLIT(REVERSE(gtas.tas_rendering_label), '-'), 3) != 'X'
        THEN REVERSE(ELEMENT_AT(SPLIT(ELEMENT_AT(SPLIT(REVERSE(gtas.tas_rendering_label), '-'), 3), '/'), 2))
        ELSE NULL END
    ) AS beginning_period_of_availability,
    COALESCE(taa.ending_period_of_availability,
        CASE WHEN ELEMENT_AT(SPLIT(REVERSE(gtas.tas_rendering_label), '-'), 3) != 'X'
        THEN REVERSE(ELEMENT_AT(SPLIT(ELEMENT_AT(SPLIT(REVERSE(gtas.tas_rendering_label), '-'), 3), '/'), 1))
        ELSE NULL END
    ) AS ending_period_of_availability,
    COALESCE(taa.availability_type_code,
        CASE WHEN ELEMENT_AT(SPLIT(REVERSE(gtas.tas_rendering_label), '-'), 3) = 'X'
        THEN 'X'
        ELSE NULL END
    ) AS availability_type_code,
    COALESCE(taa.main_account_code, REVERSE(ELEMENT_AT(SPLIT(REVERSE(gtas.tas_rendering_label), '-'), 2))) AS main_account_code,
    COALESCE(taa.sub_account_code, REVERSE(ELEMENT_AT(SPLIT(REVERSE(gtas.tas_rendering_label), '-'), 1))) AS sub_account_code,
    gtas.tas_rendering_label AS treasury_account_symbol,
    taa.account_title AS treasury_account_name,
    cgac_aid.agency_name AS agency_identifier_name,
    cgac_ata.agency_name AS allocation_transfer_agency_identifier_name,
    taa.budget_function_title AS budget_function,
    taa.budget_subfunction_title AS budget_subfunction,
    fa.federal_account_code AS federal_account_symbol,
    fa.account_title AS federal_account_name,
    gtas.disaster_emergency_fund_code AS disaster_emergency_fund_code,
    defc.public_law AS disaster_emergency_fund_name,
    gtas.budget_authority_unobligated_balance_brought_forward_cpe AS budget_authority_unobligated_balance_brought_forward,
    gtas.adjustments_to_unobligated_balance_brought_forward_fyb AS adjustments_to_unobligated_balance_brought_forward_fyb,
    gtas.adjustments_to_unobligated_balance_brought_forward_cpe AS adjustments_to_unobligated_balance_brought_forward_cpe,
    gtas.budget_authority_appropriation_amount_cpe AS budget_authority_appropriated_amount,
    gtas.borrowing_authority_amount AS borrowing_authority_amount,
    gtas.contract_authority_amount AS contract_authority_amount,
    gtas.spending_authority_from_offsetting_collections_amount AS spending_authority_from_offsetting_collections_amount,
    gtas.other_budgetary_resources_amount_cpe AS total_other_budgetary_resources_amount,
    gtas.total_budgetary_resources_cpe AS total_budgetary_resources,
    gtas.prior_year_paid_obligation_recoveries AS prior_year_paid_obligation_recoveries,
    gtas.anticipated_prior_year_obligation_recoveries AS anticipated_prior_year_obligation_recoveries,
    gtas.obligations_incurred_total_cpe AS obligations_incurred,
    gtas.deobligations_or_recoveries_or_refunds_from_prior_year_cpe AS deobligations_or_recoveries_or_refunds_from_prior_year,
    gtas.unobligated_balance_cpe AS unobligated_balance,
    gtas.gross_outlay_amount_by_tas_cpe AS gross_outlay_amount,
    gtas.status_of_budgetary_resources_total_cpe AS status_of_budgetary_resources_total
FROM global_temp.gtas_sf133_balances gtas
INNER JOIN (
    SELECT
        dabs_submission_window_schedule.submission_fiscal_year,
        dabs_submission_window_schedule.is_quarter,
        MAX(dabs_submission_window_schedule.submission_fiscal_month) AS submission_fiscal_month
    FROM global_temp.dabs_submission_window_schedule
    WHERE
        dabs_submission_window_schedule.submission_reveal_date <= now()
        AND dabs_submission_window_schedule.is_quarter = False
    GROUP BY
        dabs_submission_window_schedule.submission_fiscal_year,
        dabs_submission_window_schedule.is_quarter
) latest_submissions_of_fy
ON (gtas.fiscal_year = latest_submissions_of_fy.submission_fiscal_year AND gtas.fiscal_period = latest_submissions_of_fy.submission_fiscal_month AND latest_submissions_of_fy.is_quarter = False)
INNER JOIN global_temp.disaster_emergency_fund_code AS defc ON (gtas.disaster_emergency_fund_code = defc.code)
LEFT OUTER JOIN global_temp.treasury_appropriation_account AS taa ON (gtas.treasury_account_identifier = taa.treasury_account_identifier)
LEFT OUTER JOIN global_temp.federal_account AS fa ON (taa.federal_account_id = fa.id)
LEFT OUTER JOIN global_temp.toptier_agency AS agency ON (fa.parent_toptier_agency_id = agency.toptier_agency_id)
LEFT OUTER JOIN global_temp.cgac AS cgac_aid ON (
    COALESCE(
        taa.agency_id,
        CASE
            WHEN SIZE(SPLIT(gtas.tas_rendering_label, '-')) = 5
            THEN ELEMENT_AT(SPLIT(gtas.tas_rendering_label, '-'), 2)
            ELSE ELEMENT_AT(SPLIT(gtas.tas_rendering_label, '-'), 1)
        END
    ) = cgac_aid.cgac_code
)
LEFT OUTER JOIN global_temp.cgac AS cgac_ata ON (
    COALESCE(
        taa.allocation_transfer_agency_id,
        CASE
            WHEN SIZE(SPLIT(gtas.tas_rendering_label, '-')) = 5
            THEN ELEMENT_AT(SPLIT(gtas.tas_rendering_label, '-'), 1)
            ELSE NULL
        END
    ) = cgac_ata.cgac_code
)
WHERE defc.group_name = 'covid_19'
"""
