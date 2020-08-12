WITH recent_submission AS (
    SELECT
        "dabs_submission_window_schedule"."submission_fiscal_year",
        "dabs_submission_window_schedule"."is_quarter",
        "dabs_submission_window_schedule"."submission_fiscal_month"
    FROM "dabs_submission_window_schedule"
    WHERE
        "dabs_submission_window_schedule"."submission_reveal_date" <= now()
        AND "dabs_submission_window_schedule"."is_quarter" = False
    ORDER BY "dabs_submission_window_schedule"."submission_fiscal_year" DESC, "dabs_submission_window_schedule"."submission_fiscal_month" DESC
    LIMIT 1
)

SELECT
    agency."name" AS "owning_agency_name",
    CONCAT('FY', gtas."fiscal_year", 'P', lpad(gtas."fiscal_period"::text, 2, '0')) AS "submission_period",
    COALESCE(taa."allocation_transfer_agency_id",
        CASE WHEN array_upper(string_to_array(gtas."tas_rendering_label", '-'), 1) = 5
        THEN SPLIT_PART(gtas."tas_rendering_label", '-', 1)
        ELSE NULL END
    ) AS "allocation_transfer_agency_identifer_code",
    COALESCE(taa."agency_id",
        CASE WHEN array_upper(string_to_array(gtas."tas_rendering_label", '-'), 1) = 5
        THEN SPLIT_PART(gtas."tas_rendering_label", '-', 2)
        ELSE SPLIT_PART(gtas."tas_rendering_label", '-', 1) END
    ) AS "agency_identifier_code",
    COALESCE(taa."beginning_period_of_availability",
        CASE WHEN SPLIT_PART(REVERSE(gtas."tas_rendering_label"), '-', 3) != 'X'
        THEN REVERSE(SPLIT_PART(SPLIT_PART(REVERSE(gtas."tas_rendering_label"), '-', 3), '/', 2))
        ELSE NULL END
    ) AS "beginning_period_of_availability",
    COALESCE(taa."ending_period_of_availability",
        CASE WHEN SPLIT_PART(REVERSE(gtas."tas_rendering_label"), '-', 3) != 'X'
        THEN REVERSE(SPLIT_PART(SPLIT_PART(REVERSE(gtas."tas_rendering_label"), '-', 3), '/', 1))
        ELSE NULL END
    ) AS "ending_period_of_availability",
    COALESCE(taa."availability_type_code",
        CASE WHEN SPLIT_PART(REVERSE(gtas."tas_rendering_label"), '-', 3) = 'X'
        THEN 'X'
        ELSE NULL END
    ) AS "availability_type_code",
    COALESCE(taa."main_account_code", REVERSE(SPLIT_PART(REVERSE(gtas."tas_rendering_label"), '-', 2))) AS main_account_code,
    COALESCE(taa."sub_account_code", REVERSE(SPLIT_PART(REVERSE(gtas."tas_rendering_label"), '-', 1))) AS sub_account_code,
    gtas."tas_rendering_label" AS "treasury_account_symbol",
    taa."account_title" AS "treasury_account_name",
    (SELECT U0."agency_name" FROM "cgac" U0 WHERE U0."cgac_code" = (
        COALESCE(
            taa."agency_id",
                CASE WHEN array_upper(string_to_array(gtas."tas_rendering_label", '-'), 1) = 5
                THEN SPLIT_PART(gtas."tas_rendering_label", '-', 2)
                ELSE SPLIT_PART(gtas."tas_rendering_label", '-', 1) END
        )
    )) AS "agency_identifier_name",
    (SELECT U0."agency_name" FROM "cgac" U0 WHERE U0."cgac_code" = (
    COALESCE(taa."allocation_transfer_agency_id",
        CASE WHEN array_upper(string_to_array(gtas."tas_rendering_label", '-'), 1) = 5
        THEN SPLIT_PART(gtas."tas_rendering_label", '-', 1)
        ELSE NULL END
        )
    )) AS "allocation_transfer_agency_identifer_name",
    taa."budget_function_title" AS "budget_function",
    taa."budget_subfunction_title" AS "budget_subfunction",
    fa."federal_account_code" AS "federal_account_symbol",
    fa."account_title" AS "federal_account_name",
    gtas."disaster_emergency_fund_code" AS "disaster_emergency_fund_code",
    defc."public_law" AS "disaster_emergency_fund_name",

    gtas."budget_authority_appropriation_amount_cpe" AS "budget_authority_appropriated_amount",
    gtas."other_budgetary_resources_amount_cpe" AS "total_other_budgetary_resources_amount",
    gtas."total_budgetary_resources_cpe" AS "total_budgetary_resources",
    gtas."obligations_incurred_total_cpe" AS "obligations_incurred",
    gtas."unobligated_balance_cpe" AS "unobligated_balance",
    gtas."gross_outlay_amount_by_tas_cpe" AS "gross_outlay_amount"
FROM gtas_sf133_balances gtas
INNER JOIN recent_submission sub ON (gtas."fiscal_year" = sub."submission_fiscal_year" AND gtas."fiscal_period" = sub."submission_fiscal_month" AND sub."is_quarter" = False)
INNER JOIN disaster_emergency_fund_code defc ON (gtas."disaster_emergency_fund_code" = defc."code")
LEFT OUTER JOIN treasury_appropriation_account taa ON (gtas."treasury_account_identifier" = taa."treasury_account_identifier")
LEFT OUTER JOIN federal_account fa ON (taa."federal_account_id" = fa."id")
LEFT OUTER JOIN toptier_agency agency ON (fa."parent_toptier_agency_id" = agency."toptier_agency_id")
WHERE defc."group_name" = 'covid_19'
