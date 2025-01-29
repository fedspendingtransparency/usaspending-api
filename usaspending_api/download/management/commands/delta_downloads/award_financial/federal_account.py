DOWNLOAD_QUERY = """
    SELECT
        toptier_agency.name AS owning_agency_name,
        CONCAT_WS(
            '; ',
            COLLECT_SET(submission_attributes.reporting_agency_name)
        ) AS reporting_agency_name,
        CASE
            WHEN submission_attributes.quarter_format_flag = TRUE
                THEN
                    CONCAT(
                        CAST('FY' AS STRING), CAST(submission_attributes.reporting_fiscal_year AS STRING),
                        CAST('Q' AS STRING), CAST(submission_attributes.reporting_fiscal_quarter AS STRING)
                    )
            ELSE
                CONCAT(
                    CAST('FY' AS STRING), CAST(submission_attributes.reporting_fiscal_year AS STRING),
                    CAST('P' AS STRING), LPAD(CAST(submission_attributes.reporting_fiscal_period AS STRING), 2, '0')
                )
        END AS submission_period,
        federal_account.federal_account_code AS federal_account_symbol,
        federal_account.account_title AS federal_account_name,
        CGAC_AID.agency_name AS agency_identifier_name,
        CONCAT_WS(
            '; ',
            COLLECT_SET(treasury_appropriation_account.budget_function_title)
        ) AS budget_function,
        CONCAT_WS(
            '; ',
            COLLECT_SET(treasury_appropriation_account.budget_subfunction_title)
        ) AS budget_subfunction,
        ref_program_activity.program_activity_code AS program_activity_code,
        ref_program_activity.program_activity_name AS program_activity_name,
        object_class.object_class AS object_class_code,
        object_class.object_class_name AS object_class_name,
        object_class.direct_reimbursable AS direct_or_reimbursable_funding_source,
        financial_accounts_by_awards.disaster_emergency_fund_code AS disaster_emergency_fund_code,
        disaster_emergency_fund_code.title AS disaster_emergency_fund_name,
        SUM(financial_accounts_by_awards.transaction_obligated_amount) AS transaction_obligated_amount,
        SUM(
            CASE
                WHEN
                    (
                        (submission_attributes.quarter_format_flag = TRUE AND submission_attributes.reporting_fiscal_quarter = 4)
                        OR (submission_attributes.quarter_format_flag = FALSE AND submission_attributes.reporting_fiscal_period = 12)
                    ) AND submission_attributes.reporting_fiscal_year = 2021
                    THEN financial_accounts_by_awards.gross_outlay_amount_by_award_cpe
                ELSE CAST(NULL AS NUMERIC(23, 2))
            END
        ) AS gross_outlay_amount_FYB_to_period_end,
        SUM(
            CASE
                WHEN
                    (
                        (submission_attributes.quarter_format_flag = TRUE AND submission_attributes.reporting_fiscal_quarter = 4)
                        OR (submission_attributes.quarter_format_flag = FALSE AND submission_attributes.reporting_fiscal_period = 12)
                    ) AND submission_attributes.reporting_fiscal_year = 2021
                    THEN financial_accounts_by_awards.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe
                ELSE CAST(NULL AS NUMERIC(23, 2))
            END
        ) AS USSGL487200_downward_adj_prior_year_prepaid_undeliv_order_oblig,
        SUM(
            CASE
                WHEN
                    (
                        (submission_attributes.quarter_format_flag = TRUE AND submission_attributes.reporting_fiscal_quarter = 4)
                        OR (submission_attributes.quarter_format_flag = FALSE AND submission_attributes.reporting_fiscal_period = 12)
                    ) AND submission_attributes.reporting_fiscal_year = 2021
                    THEN financial_accounts_by_awards.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe
                ELSE CAST(NULL AS NUMERIC(23, 2))
            END
        ) AS USSGL497200_downward_adj_of_prior_year_paid_deliv_orders_oblig,
        award_search.generated_unique_award_id AS award_unique_key,
        financial_accounts_by_awards.piid AS award_id_piid,
        financial_accounts_by_awards.parent_award_id AS parent_award_id_piid,
        financial_accounts_by_awards.fain AS award_id_fain,
        financial_accounts_by_awards.uri AS award_id_uri,
        award_search.date_signed AS award_base_action_date,
        EXTRACT(
            YEAR FROM (award_search.date_signed) + INTERVAL '3 months'
        ) AS award_base_action_date_fiscal_year,
        award_search.certified_date AS award_latest_action_date,
        EXTRACT(
            YEAR FROM (award_search.certified_date) + INTERVAL '3 months'
        ) AS award_latest_action_date_fiscal_year,
        award_search.period_of_performance_start_date AS period_of_performance_start_date,
        award_search.period_of_performance_current_end_date AS period_of_performance_current_end_date,
        transaction_search.ordering_period_end_date AS ordering_period_end_date,
        COALESCE(
            transaction_search.contract_award_type,
             transaction_search.type
        ) AS award_type_code,
        COALESCE(
            transaction_search.contract_award_type_desc,
            transaction_search.type_description
        ) AS award_type,
        transaction_search.idv_type AS idv_type_code,
        transaction_search.idv_type_description AS idv_type,
        award_search.description AS prime_award_base_transaction_description,
        transaction_search.awarding_agency_code AS awarding_agency_code,
        transaction_search.awarding_toptier_agency_name_raw AS awarding_agency_name,
        transaction_search.awarding_sub_tier_agency_c AS awarding_subagency_code,
        transaction_search.awarding_subtier_agency_name_raw AS awarding_subagency_name,
        transaction_search.awarding_office_code AS awarding_office_code,
        transaction_search.awarding_office_name AS awarding_office_name,
        transaction_search.funding_agency_code AS funding_agency_code,
        transaction_search.funding_toptier_agency_name_raw AS funding_agency_name,
        transaction_search.funding_sub_tier_agency_co AS funding_sub_agency_code,
        transaction_search.funding_subtier_agency_name_raw AS funding_sub_agency_name,
        transaction_search.funding_office_code AS funding_office_code,
        transaction_search.funding_office_name AS funding_office_name,
        transaction_search.recipient_uei AS recipient_uei,
        transaction_search.recipient_unique_id AS recipient_duns,
        transaction_search.recipient_name AS recipient_name,
        transaction_search.recipient_name_raw AS recipient_name_raw,
        transaction_search.parent_uei AS recipient_parent_uei,
        transaction_search.parent_uei AS recipient_parent_duns,
        transaction_search.parent_recipient_name AS recipient_parent_name,
        transaction_search.parent_recipient_name_raw AS recipient_parent_name_raw,
        transaction_search.recipient_location_country_code AS recipient_country,
        transaction_search.recipient_location_state_code AS recipient_state,
        transaction_search.recipient_location_county_name AS recipient_county,
        transaction_search.recipient_location_city_name AS recipient_city,
        CASE
            WHEN
                transaction_search.recipient_location_state_code IS NOT NULL
                AND transaction_search.recipient_location_congressional_code IS NOT NULL
                AND NOT (
                    transaction_search.recipient_location_state_code = ''
                    AND transaction_search.recipient_location_state_code IS NOT NULL
                )
                THEN
                    CONCAT(
                        transaction_search.recipient_location_state_code, '-',
                        transaction_search.recipient_location_congressional_code
                    )
            ELSE transaction_search.recipient_location_congressional_code
        END AS prime_award_summary_recipient_cd_original,
        CASE
            WHEN
                transaction_search.recipient_location_state_code IS NOT NULL
                AND transaction_search.recipient_location_congressional_code_current IS NOT NULL
                AND NOT (
                    transaction_search.recipient_location_state_code = ''
                    AND transaction_search.recipient_location_state_code IS NOT NULL
                )
                THEN
                    CONCAT(
                        transaction_search.recipient_location_state_code, '-',
                        transaction_search.recipient_location_congressional_code_current
                    )
            ELSE transaction_search.recipient_location_congressional_code_current
        END AS prime_award_summary_recipient_cd_current,
        COALESCE(
            transaction_search.legal_entity_zip4,
            CONCAT(
                CAST(transaction_search.recipient_location_zip5 AS STRING),
                CAST(transaction_search.legal_entity_zip_last4 AS STRING)
            )
        ) AS recipient_zip_code,
        transaction_search.pop_country_name AS primary_place_of_performance_country,
        transaction_search.pop_state_name AS primary_place_of_performance_state,
        transaction_search.pop_county_name AS primary_place_of_performance_county,
        CASE
            WHEN
                transaction_search.pop_state_code IS NOT NULL
                AND transaction_search.pop_congressional_code IS NOT NULL
                AND NOT (
                    transaction_search.pop_state_code = ''
                    AND transaction_search.pop_state_code IS NOT NULL
                )
                THEN
                    CONCAT(
                        transaction_search.pop_state_code, '-', transaction_search.pop_congressional_code
                    )
            ELSE transaction_search.pop_congressional_code
        END AS prime_award_summary_place_of_performance_cd_original,
        CASE
            WHEN
                transaction_search.pop_state_code IS NOT NULL
                AND transaction_search.pop_congressional_code_current IS NOT NULL
                AND NOT (
                    transaction_search.pop_state_code = ''
                    AND transaction_search.pop_state_code IS NOT NULL)
                THEN
                    CONCAT(
                        transaction_search.pop_state_code, '-', transaction_search.pop_congressional_code_current
                    )
            ELSE transaction_search.pop_congressional_code_current
        END AS prime_award_summary_place_of_performance_cd_current,
        transaction_search.place_of_performance_zip4a AS primary_place_of_performance_zip_code,
        transaction_search.cfda_number AS cfda_number,
        transaction_search.cfda_title AS cfda_title,
        transaction_search.product_or_service_code AS product_or_service_code,
        transaction_search.product_or_service_description AS product_or_service_code_description,
        transaction_search.naics_code AS naics_code,
        transaction_search.naics_description AS naics_description,
        transaction_search.national_interest_action AS national_interest_action_code,
        transaction_search.national_interest_desc AS national_interest_action,
        -- This was generated on localhost; the address below would reflect the specific environment
        CASE
            WHEN award_search.generated_unique_award_id IS NOT NULL
                THEN CONCAT('localhost:3000/award/', url_encode(award_search.generated_unique_award_id), '/')
            ELSE ''
        END AS usaspending_permalink,
        CAST(MAX(submission_attributes.published_date) AS DATE) AS last_modified_date
    FROM raw.financial_accounts_by_awards
        INNER JOIN global_temp.submission_attributes AS submission_attributes ON (
            financial_accounts_by_awards.submission_id = submission_attributes.submission_id
        )
        LEFT OUTER JOIN global_temp.treasury_appropriation_account ON (
            financial_accounts_by_awards.treasury_account_id = treasury_appropriation_account.treasury_account_identifier
        )
        LEFT OUTER JOIN rpt.award_search ON (
            financial_accounts_by_awards.award_id = award_search.award_id
        )
        LEFT OUTER JOIN rpt.transaction_search ON (
            award_search.latest_transaction_search_id = transaction_search.transaction_id
        )
        LEFT OUTER JOIN global_temp.ref_program_activity ON (
            financial_accounts_by_awards.program_activity_id = ref_program_activity.id
        )
        LEFT OUTER JOIN global_temp.object_class ON (
            financial_accounts_by_awards.object_class_id = object_class.id
        )
        LEFT OUTER JOIN global_temp.disaster_emergency_fund_code ON (
            financial_accounts_by_awards.disaster_emergency_fund_code =disaster_emergency_fund_code.code
        )
        LEFT OUTER JOIN global_temp.federal_account ON (
            treasury_appropriation_account.federal_account_id = federal_account.id
        )
        LEFT OUTER JOIN global_temp.toptier_agency ON (
            federal_account.parent_toptier_agency_id = toptier_agency.toptier_agency_id
        )
        -- Both of the cgac JOINS occur in the VIEW used for the File C download. For the purpose of this baseline
        -- the VIEW was broken out to include specific table names instead of referencing the VIEW.
        LEFT OUTER JOIN global_temp.cgac AS CGAC_AID ON (
            treasury_appropriation_account.agency_id = CGAC_AID.cgac_code
        )
        LEFT OUTER JOIN global_temp.cgac AS CGAC_ATA ON (
            treasury_appropriation_account.allocation_transfer_agency_id = CGAC_ATA.cgac_code
        )
    WHERE
        (
            -- This logic was previously added to make sure that all necessary submissions are selected for aggregating
            -- the outlay results. The filter below should also include all of these submissions and we handle the specific
            -- filtering down for outlay aggregations via a CASE statement in the SELECT. As a result, this may not be
            -- needed when generating the File C download at least.
            -- financial_accounts_by_awards.submission_id IN (
            --     43601, 43898, 44151, 43780, 43963, 44188, 44134, 44148, 43472, 43948, 43273, 43290, 43308, 43322, 43373,
            --     43390, 43697, 43489, 43499, 43503, 43520, 43604, 43606, 43716, 43742, 43775, 43796, 43824, 43901, 43910,
            --     44032, 44035, 44067, 44073, 44096, 44102, 44131, 43590, 44150, 43159, 43556, 43583, 43593, 43270, 43527,
            --     43599, 43598, 43732, 43307, 43318, 43474, 43455, 43302, 43243, 43478, 43303, 43385, 43513, 43269, 43310,
            --     43563, 43381, 43778, 43733, 43658, 43309, 43721, 43306, 44010, 43629, 44091, 43695, 43326, 43395, 43398,
            --     43471, 44008, 43976, 43205, 43234, 43325, 43377, 43479, 43741, 43744, 43745, 43748, 43977, 44027, 44056,
            --     44118, 43866, 43940, 43603, 44038, 43532, 43609, 44111, 43524, 43562, 44076, 43592, 43607, 43594, 43988
            -- )
            financial_accounts_by_awards.submission_id IN {}
            -- This logic should contain all of the submissions mentioned above and then more. The two filters can be
            -- viewed as outlay and obligation filters respectively.
            OR (
                (
                    (
                        submission_attributes.reporting_fiscal_period <= 12
                        AND NOT submission_attributes.quarter_format_flag)
                    OR (
                        submission_attributes.reporting_fiscal_quarter <= 4
                        AND submission_attributes.quarter_format_flag
                    )
                )
                AND submission_attributes.reporting_fiscal_year = 2021
            )
        )
        -- To help performance this filter was added so that the D1 and D2 partitions of transaction_search can be used
        -- directly instead of the larger transaction_search table. The actual download that is returned was thus updated
        -- to return both the Contract and Assistance as separate files in a single zip. Moving to Spark we could look
        -- to remove that logic and return to the previous implementation of a single file.
        -- AND transaction_search.is_fpds
    GROUP BY
        toptier_agency.name,
        federal_account.federal_account_code,
        federal_account.account_title,
        CGAC_AID.agency_name,
        ref_program_activity.program_activity_code,
        ref_program_activity.program_activity_name,
        object_class.object_class,
        object_class.object_class_name,
        object_class.direct_reimbursable,
        financial_accounts_by_awards.disaster_emergency_fund_code,
        disaster_emergency_fund_code.title,
        award_search.generated_unique_award_id,
        financial_accounts_by_awards.piid,
        financial_accounts_by_awards.parent_award_id,
        financial_accounts_by_awards.fain,
        financial_accounts_by_awards.uri,
        award_search.date_signed,
        award_search.certified_date,
        award_search.period_of_performance_start_date,
        award_search.period_of_performance_current_end_date,
        transaction_search.ordering_period_end_date,
        transaction_search.idv_type,
        transaction_search.idv_type_description,
        award_search.description,
        transaction_search.awarding_agency_code,
        transaction_search.awarding_toptier_agency_name_raw,
        transaction_search.awarding_sub_tier_agency_c,
        transaction_search.awarding_subtier_agency_name_raw,
        transaction_search.awarding_office_code,
        transaction_search.awarding_office_name,
        transaction_search.funding_agency_code,
        transaction_search.funding_toptier_agency_name_raw,
        transaction_search.funding_sub_tier_agency_co,
        transaction_search.funding_subtier_agency_name_raw,
        transaction_search.funding_office_code,
        transaction_search.funding_office_name,
        transaction_search.recipient_uei,
        transaction_search.recipient_unique_id,
        transaction_search.recipient_name,
        transaction_search.recipient_name_raw,
        transaction_search.parent_uei,
        transaction_search.parent_recipient_name,
        transaction_search.parent_recipient_name_raw,
        transaction_search.recipient_location_country_code,
        transaction_search.recipient_location_state_code,
        transaction_search.recipient_location_county_name,
        transaction_search.recipient_location_city_name,
        transaction_search.pop_country_name,
        transaction_search.pop_state_name,
        transaction_search.pop_county_name,
        transaction_search.place_of_performance_zip4a,
        transaction_search.cfda_number,
        transaction_search.cfda_title,
        transaction_search.product_or_service_code,
        transaction_search.product_or_service_description,
        transaction_search.naics_code,
        transaction_search.naics_description,
        transaction_search.national_interest_action,
        transaction_search.national_interest_desc,
        CASE
            WHEN submission_attributes.quarter_format_flag = TRUE
                THEN
                    CONCAT(
                        CAST('FY' AS STRING), CAST(submission_attributes.reporting_fiscal_year AS STRING),
                        CAST('Q' AS STRING), CAST(submission_attributes.reporting_fiscal_quarter AS STRING)
                    )
            ELSE
                CONCAT(
                    CAST('FY' AS STRING), CAST(submission_attributes.reporting_fiscal_year AS STRING),
                    CAST('P' AS STRING), LPAD(CAST(submission_attributes.reporting_fiscal_period AS STRING), 2, '0')
                )
        END,
        COALESCE(transaction_search.contract_award_type, transaction_search.type),
        COALESCE(transaction_search.contract_award_type_desc, transaction_search.type_description),
        COALESCE(
            transaction_search.legal_entity_zip4,
            CONCAT(
                CAST(transaction_search.recipient_location_zip5 AS STRING),
                CAST(transaction_search.legal_entity_zip_last4 AS STRING)
            )
        ),
        EXTRACT(YEAR FROM (award_search.date_signed) + INTERVAL '3 months'),
        EXTRACT(YEAR FROM (award_search.certified_date) + INTERVAL '3 months'),
        -- This was generated on localhost; the address below would reflect the specific environment
        CASE
            WHEN award_search.generated_unique_award_id IS NOT NULL
                THEN CONCAT('localhost:3000/award/', url_encode(award_search.generated_unique_award_id), '/')
            ELSE ''
        END,
        CASE
            WHEN
                transaction_search.recipient_location_state_code IS NOT NULL
                AND transaction_search.recipient_location_congressional_code IS NOT NULL
                AND NOT (
                    transaction_search.recipient_location_state_code = ''
                    AND transaction_search.recipient_location_state_code IS NOT NULL
                )
                THEN
                    CONCAT(
                        transaction_search.recipient_location_state_code, '-',
                        transaction_search.recipient_location_congressional_code
                    )
            ELSE transaction_search.recipient_location_congressional_code
        END,
        CASE
            WHEN
                transaction_search.recipient_location_state_code IS NOT NULL
                AND transaction_search.recipient_location_congressional_code_current IS NOT NULL
                AND NOT (
                    transaction_search.recipient_location_state_code = ''
                    AND transaction_search.recipient_location_state_code IS NOT NULL
                )
                THEN
                    CONCAT(
                        transaction_search.recipient_location_state_code, '-',
                        transaction_search.recipient_location_congressional_code_current
                    )
            ELSE transaction_search.recipient_location_congressional_code_current
        END,
        CASE
            WHEN
                transaction_search.pop_state_code IS NOT NULL
                AND transaction_search.pop_congressional_code IS NOT NULL
                AND NOT (
                    transaction_search.pop_state_code = ''
                    AND transaction_search.pop_state_code IS NOT NULL
                )
                THEN
                    CONCAT(
                        transaction_search.pop_state_code, '-', transaction_search.pop_congressional_code
                    )
            ELSE transaction_search.pop_congressional_code
        END,
        CASE
            WHEN
                transaction_search.pop_state_code IS NOT NULL
                AND transaction_search.pop_congressional_code_current IS NOT NULL
                AND NOT (
                    transaction_search.pop_state_code = ''
                    AND transaction_search.pop_state_code IS NOT NULL)
                THEN
                    CONCAT(
                        transaction_search.pop_state_code, '-', transaction_search.pop_congressional_code_current
                    )
            ELSE transaction_search.pop_congressional_code_current
        END
    HAVING
        -- All of the HAVING statements below ensure we return only non-zero sum records
        SUM(
            CASE
                WHEN
                    (
                        (
                            submission_attributes.quarter_format_flag = TRUE
                            AND submission_attributes.reporting_fiscal_quarter = 4
                        ) OR (
                            submission_attributes.quarter_format_flag = FALSE
                            AND submission_attributes.reporting_fiscal_period = 12
                        )
                    ) AND submission_attributes.reporting_fiscal_year = 2021
                    THEN
                        financial_accounts_by_awards.gross_outlay_amount_by_award_cpe
                ELSE
                    CAST(NULL AS NUMERIC(23, 2))
                END
        ) > 0
        OR SUM(
            CASE
                WHEN
                    (
                        (
                            submission_attributes.quarter_format_flag = TRUE
                            AND submission_attributes.reporting_fiscal_quarter = 4
                        ) OR (
                            submission_attributes.quarter_format_flag = FALSE
                            AND submission_attributes.reporting_fiscal_period = 12
                        )
                    ) AND submission_attributes.reporting_fiscal_year = 2021
                    THEN
                        financial_accounts_by_awards.gross_outlay_amount_by_award_cpe
                ELSE
                    CAST(NULL AS NUMERIC(23, 2))
                END
        ) < 0
        OR SUM(
            CASE
                WHEN
                    (
                        (
                            submission_attributes.quarter_format_flag = TRUE
                            AND submission_attributes.reporting_fiscal_quarter = 4
                        ) OR (
                            submission_attributes.quarter_format_flag = FALSE
                            AND submission_attributes.reporting_fiscal_period = 12
                        )
                    ) AND submission_attributes.reporting_fiscal_year = 2021
                    THEN
                        financial_accounts_by_awards.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe
                ELSE
                    CAST(NULL AS NUMERIC(23, 2))
                END
        ) < 0
        OR SUM(
            CASE
                WHEN
                    (
                        (
                            submission_attributes.quarter_format_flag = TRUE
                            AND submission_attributes.reporting_fiscal_quarter = 4
                        ) OR (
                            submission_attributes.quarter_format_flag = FALSE
                            AND submission_attributes.reporting_fiscal_period = 12
                        )
                    ) AND submission_attributes.reporting_fiscal_year = 2021
                    THEN
                        financial_accounts_by_awards.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe
                ELSE
                    CAST(NULL AS NUMERIC(23, 2))
                END
        ) > 0
        OR SUM(
            CASE
                WHEN
                    (
                        (
                            submission_attributes.quarter_format_flag = TRUE
                            AND submission_attributes.reporting_fiscal_quarter = 4
                        ) OR (
                            submission_attributes.quarter_format_flag = FALSE
                            AND submission_attributes.reporting_fiscal_period = 12
                        )
                    ) AND submission_attributes.reporting_fiscal_year = 2021
                    THEN
                        financial_accounts_by_awards.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe
                ELSE
                    CAST(NULL AS NUMERIC(23, 2))
                END
        ) < 0
        OR SUM(
            CASE
                WHEN
                    (
                        (
                            submission_attributes.quarter_format_flag = TRUE
                            AND submission_attributes.reporting_fiscal_quarter = 4
                        ) OR (
                            submission_attributes.quarter_format_flag = FALSE
                            AND submission_attributes.reporting_fiscal_period = 12
                        )
                    ) AND submission_attributes.reporting_fiscal_year = 2021
                    THEN
                        financial_accounts_by_awards.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe
                ELSE
                    CAST(NULL AS NUMERIC(23, 2))
                END
        ) > 0
        OR SUM(financial_accounts_by_awards.transaction_obligated_amount) > 0
        OR SUM(financial_accounts_by_awards.transaction_obligated_amount) < 0
"""

# This may not be needed when going to Production, but it is added for a 1:1 comparison
# with the current functionality in Production.
SUBMISSION_ID_QUERY = """
    SELECT  submission_id
    FROM    global_temp.submission_attributes
    WHERE   (toptier_code, reporting_fiscal_year, reporting_fiscal_period) IN (
                SELECT  toptier_code, reporting_fiscal_year, reporting_fiscal_period
                FROM    global_temp.submission_attributes
                WHERE   reporting_fiscal_year = 2021 AND
                        (
                            (reporting_fiscal_quarter <= 4 AND quarter_format_flag is true) OR
                            (reporting_fiscal_period <= 12 AND quarter_format_flag is false)
                        )
                ORDER   BY toptier_code, reporting_fiscal_period desc
            ) AND
            (
                (reporting_fiscal_quarter = 4 AND quarter_format_flag IS TRUE) OR
                (reporting_fiscal_period = 12 AND quarter_format_flag IS FALSE)
            )
"""
