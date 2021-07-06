SELECT
    "toptier_agency"."name" AS "owning_agency_name",
    "submission_attributes"."reporting_agency_name" AS "reporting_agency_name",
    CASE
        WHEN "submission_attributes"."quarter_format_flag" = True THEN
            CONCAT('FY', EXTRACT(YEAR FROM ("financial_accounts_by_program_activity_object_class"."reporting_period_end") + INTERVAL '3 months'), 'Q', EXTRACT(QUARTER FROM ("financial_accounts_by_program_activity_object_class"."reporting_period_end") + INTERVAL '3 months'))
        ELSE
            CONCAT('FY', EXTRACT(YEAR FROM ("financial_accounts_by_program_activity_object_class"."reporting_period_end") + INTERVAL '3 months'), 'P', lpad(EXTRACT(MONTH FROM ("financial_accounts_by_program_activity_object_class"."reporting_period_end") + INTERVAL '3 months')::text, 2, '0'))
    END AS "submission_period",
    "treasury_appropriation_account"."allocation_transfer_agency_id" AS "allocation_transfer_agency_identifier_code",
    "treasury_appropriation_account"."agency_id" AS "agency_identifier_code",
    "treasury_appropriation_account"."beginning_period_of_availability" AS "beginning_period_of_availability",
    "treasury_appropriation_account"."ending_period_of_availability" AS "ending_period_of_availability",
    "treasury_appropriation_account"."availability_type_code" AS "availability_type_code",
    "treasury_appropriation_account"."main_account_code" AS "main_account_code",
    "treasury_appropriation_account"."sub_account_code" AS "sub_account_code",
    "treasury_appropriation_account"."tas_rendering_label" AS "treasury_account_symbol",
    "treasury_appropriation_account"."account_title" AS "treasury_account_name",
    "cgac_aid"."agency_name" AS "agency_identifier_name",
    "cgac_ata"."agency_name" AS "allocation_transfer_agency_identifier_name",
    "treasury_appropriation_account"."budget_function_title" AS "budget_function",
    "treasury_appropriation_account"."budget_subfunction_title" AS "budget_subfunction",
    "federal_account"."federal_account_code" AS "federal_account_symbol",
    "federal_account"."account_title" AS "federal_account_name",
    "disaster_emergency_fund_code"."code" AS "disaster_emergency_fund_code",
    "disaster_emergency_fund_code"."public_law" AS "disaster_emergency_fund_name",
    "ref_program_activity"."program_activity_code" AS "program_activity_code",
    "ref_program_activity"."program_activity_name" AS "program_activity_name",
    "object_class"."object_class" AS "object_class_code",
    "object_class"."object_class_name" AS "object_class_name",
    "object_class"."direct_reimbursable" AS "direct_or_reimbursable_funding_source",
    "financial_accounts_by_program_activity_object_class"."obligations_incurred_by_program_object_class_cpe" AS "obligations_incurred",
    "financial_accounts_by_program_activity_object_class"."deobligations_recoveries_refund_pri_program_object_class_cpe" AS "deobligations_or_recoveries_or_refunds_from_prior_year",
    "financial_accounts_by_program_activity_object_class"."ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe" AS "downward_adj_prior_yr_ppaid_undeliv_orders_oblig_refunds_cpe",
    "financial_accounts_by_program_activity_object_class"."ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe" AS "downward_adj_prior_yr_paid_delivered_orders_oblig_refunds_cpe",
    "financial_accounts_by_program_activity_object_class"."gross_outlay_amount_by_program_object_class_cpe" AS "gross_outlay_amount",
    (MAX("submission_attributes"."published_date")) ::date AS "last_modified_date"
FROM "financial_accounts_by_program_activity_object_class"
LEFT OUTER JOIN "treasury_appropriation_account" ON ("financial_accounts_by_program_activity_object_class"."treasury_account_id" = "treasury_appropriation_account"."treasury_account_identifier")
INNER JOIN "federal_account" ON ("treasury_appropriation_account"."federal_account_id" = "federal_account"."id")
INNER JOIN "submission_attributes" ON ("financial_accounts_by_program_activity_object_class"."submission_id" = "submission_attributes"."submission_id")
INNER JOIN "disaster_emergency_fund_code" ON ("financial_accounts_by_program_activity_object_class"."disaster_emergency_fund_code" = "disaster_emergency_fund_code"."code")
INNER JOIN "ref_program_activity" ON ("financial_accounts_by_program_activity_object_class"."program_activity_id" = "ref_program_activity"."id")
INNER JOIN "object_class" ON ("financial_accounts_by_program_activity_object_class"."object_class_id" = "object_class"."id")
LEFT OUTER JOIN "toptier_agency" ON ("treasury_appropriation_account"."funding_toptier_agency_id" = "toptier_agency"."toptier_agency_id")
LEFT OUTER JOIN "cgac" AS "cgac_aid" ON ("treasury_appropriation_account"."agency_id" = "cgac_aid"."cgac_code")
LEFT OUTER JOIN "cgac" AS "cgac_ata" ON ("treasury_appropriation_account"."allocation_transfer_agency_id" = "cgac_ata"."cgac_code")
WHERE (
    "disaster_emergency_fund_code"."group_name" = 'covid_19'
    AND "submission_attributes"."is_final_balances_for_fy" = TRUE
)
GROUP BY
    "financial_accounts_by_program_activity_object_class"."financial_accounts_by_program_activity_object_class_id",
    CASE
        WHEN "submission_attributes"."quarter_format_flag" = True THEN
            CONCAT('FY', EXTRACT(YEAR FROM ("financial_accounts_by_program_activity_object_class"."reporting_period_end") + INTERVAL '3 months'), 'Q', EXTRACT(QUARTER FROM ("financial_accounts_by_program_activity_object_class"."reporting_period_end") + INTERVAL '3 months'))
        ELSE
            CONCAT('FY', EXTRACT(YEAR FROM ("financial_accounts_by_program_activity_object_class"."reporting_period_end") + INTERVAL '3 months'), 'P', lpad(EXTRACT(MONTH FROM ("financial_accounts_by_program_activity_object_class"."reporting_period_end") + INTERVAL '3 months')::text, 2, '0'))
    END,
    "cgac_aid"."agency_name",
    "cgac_ata"."agency_name",
    "toptier_agency"."name",
    "submission_attributes"."reporting_agency_name",
    "treasury_appropriation_account"."allocation_transfer_agency_id",
    "treasury_appropriation_account"."agency_id",
    "treasury_appropriation_account"."beginning_period_of_availability",
    "treasury_appropriation_account"."ending_period_of_availability",
    "treasury_appropriation_account"."availability_type_code",
    "treasury_appropriation_account"."main_account_code",
    "treasury_appropriation_account"."sub_account_code",
    "treasury_appropriation_account"."tas_rendering_label",
    "treasury_appropriation_account"."account_title",
    "treasury_appropriation_account"."budget_function_title",
    "treasury_appropriation_account"."budget_subfunction_title",
    "federal_account"."federal_account_code",
    "federal_account"."account_title",
    "disaster_emergency_fund_code"."code",
    "disaster_emergency_fund_code"."title",
    "ref_program_activity"."program_activity_code",
    "ref_program_activity"."program_activity_name",
    "object_class"."object_class",
    "object_class"."object_class_name",
    "object_class"."direct_reimbursable"
