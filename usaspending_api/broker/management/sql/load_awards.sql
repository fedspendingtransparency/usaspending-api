DROP TABLE IF EXISTS awards_new;

CREATE TABLE awards_new AS (
    SELECT
        nextval('public.awards_id_seq') AS id,
        *
    FROM
    (
        (
            -- TRANSACTION NORMALIZED: FPDS
            SELECT
                DISTINCT ON (tfn.piid, tfn.parent_award_id, tfn.agency_id, tfn.referenced_idv_agency_iden)
                'cont_aw_' ||
                    coalesce(tfn.agency_id,'-none-') || '_' ||
                    coalesce(tfn.referenced_idv_agency_iden,'-none-') || '_' ||
                    coalesce(tfn.piid,'-none-') || '_' ||
                    coalesce(tfn.parent_award_id,'-none-') AS generated_unique_award_id,
                TRUE AS is_fpds,
                tfn.detached_award_proc_unique AS transaction_unique_id,
                'DBR'::TEXT AS data_source,
                tfn.contract_award_type AS type,
                tfn.contract_award_type_desc AS type_description,
                tfn.piid AS piid,
                tfn.parent_award_id AS parent_award_piid,
                NULL::TEXT AS fain,
                NULL::TEXT AS uri,
                SUM(COALESCE(tfn.federal_action_obligation::DOUBLE PRECISION, 0::DOUBLE PRECISION)) over w AS total_obligation,
                SUM(COALESCE(tfn.base_and_all_options_value::DOUBLE PRECISION, 0::DOUBLE PRECISION)) over w AS base_and_all_options_value,
                NULL::FLOAT AS total_subsidy_cost,
                NULL::FLOAT AS total_outlay,
                MIN(tfn.action_date) over w AS date_signed,
                tfn.award_description AS description,
                MIN(tfn.period_of_performance_star::date) over w AS period_of_performance_start_date,
                MAX(tfn.period_of_performance_curr::date) over w AS period_of_performance_current_end_date,
                NULL::double precision AS potential_total_value_of_award,
                tfn.last_modified::date AS last_modified_date,
                MAX(tfn.action_date) over w AS certified_date,
                NOW() AS create_date,
                NOW() AS update_date,
                0::DOUBLE PRECISION AS total_subaward_amount,
                0::INT AS subaward_count,
                awarding_agency.agency_id AS awarding_agency_id,
                funding_agency.agency_id AS funding_agency_id,
                tfn.transaction_id AS latest_transaction_id,
                NULL::INT AS parent_award_id,
                pop_location.location_id AS place_of_performance_id,
                recipient.legal_entity_id AS recipient_id,
                ac.type_name AS category,
                EXTRACT(YEAR FROM (CAST(action_date AS DATE) + INTERVAL '3 month')) AS fiscal_year
            FROM
                transaction_fpds_new AS tfn
                LEFT OUTER JOIN
                award_category AS ac ON ac.type_code = tfn.contract_award_type
                INNER JOIN
                references_location_new AS pop_location ON tfn.detached_award_proc_unique = pop_location.transaction_unique_id AND pop_location.is_fpds = TRUE AND pop_location.place_of_performance_flag = TRUE
                INNER JOIN
                legal_entity_new AS recipient ON tfn.detached_award_proc_unique = recipient.transaction_unique_id AND recipient.is_fpds = TRUE
                INNER JOIN
                agency_lookup AS awarding_agency ON awarding_agency.subtier_code = tfn.awarding_sub_tier_agency_c
                LEFT OUTER JOIN
                agency_lookup AS funding_agency ON funding_agency.subtier_code = tfn.funding_sub_tier_agency_co
            window w AS (partition BY tfn.piid, tfn.parent_award_id, tfn.agency_id, tfn.referenced_idv_agency_iden)
            ORDER BY
                tfn.piid,
                tfn.parent_award_id,
                tfn.agency_id,
                tfn.referenced_idv_agency_iden,
                tfn.action_date DESC,
                tfn.award_modification_amendme DESC,
                tfn.transaction_number DESC
        )

        UNION ALL

        (
            -- TRANSACTION NORMALIZED: FABS - FAIN
            SELECT
                DISTINCT ON (tfn.fain, tfn.awarding_sub_tier_agency_c)
                'asst_aw_' ||
                    coalesce(tfn.awarding_sub_tier_agency_c,'-none-') || '_' ||
                    coalesce(tfn.fain, '-none-') || '_' ||
                    '-none-' AS generated_unique_award_id,
                FALSE as is_fpds,
                tfn.afa_generated_unique AS transaction_unique_id,
                'DBR'::TEXT AS data_source,
                tfn.assistance_type AS type,
                CASE
                    WHEN tfn.assistance_type = '02' THEN 'Block Grant'
                    WHEN tfn.assistance_type = '03' THEN 'Formula Grant'
                    WHEN tfn.assistance_type = '04' THEN 'Project Grant'
                    WHEN tfn.assistance_type = '05' THEN 'Cooperative Agreement'
                    WHEN tfn.assistance_type = '06' THEN 'Direct Payment for Specified Use'
                    WHEN tfn.assistance_type = '07' THEN 'Direct Loan'
                    WHEN tfn.assistance_type = '08' THEN 'Guaranteed/Insured Loan'
                    WHEN tfn.assistance_type = '09' THEN 'Insurance'
                    WHEN tfn.assistance_type = '10' THEN 'Direct Payment with Unrestricted Use'
                    WHEN tfn.assistance_type = '11' THEN 'Other Financial Assistance'
                END AS type_description,
                NULL::TEXT AS piid,
                NULL::TEXST AS parent_award_piid,
                tfn.fain AS fain,
                NULL::TEXT AS uri,
                SUM(COALESCE(tfn.federal_action_obligation::DOUBLE PRECISION, 0::DOUBLE PRECISION)) over w AS total_obligation,
                NULL::FLOAT AS base_and_all_options_value,
                SUM(COALESCE(tfn.original_loan_subsidy_cost::DOUBLE PRECISION, 0::DOUBLE PRECISION)) over w AS total_subsidy_cost,
                NULL::float AS total_outlay,
                MIN(tfn.action_date) over w AS date_signed,
                tfn.award_description AS description,
                MIN(tfn.period_of_performance_star::date) over w AS period_of_performance_start_date,
                MAX(tfn.period_of_performance_curr::date) over w AS period_of_performance_current_end_date,
                NULL::FLOAT AS potential_total_value_of_award,
                tfn.modified_at::date AS last_modified_date,
                MAX(tfn.action_date) over w AS certified_date,
                NOW() AS create_date,
                NOW() AS update_date,
                0::DOUBLE PRECISION AS total_subaward_amount,
                0::INT AS subaward_count,
                awarding_agency.agency_id AS awarding_agency_id,
                funding_agency.agency_id AS funding_agency_id,
                tfn.transaction_id AS latest_transaction_id,
                NULL::INT AS parent_award_id,
                pop_location.location_id AS place_of_performance_id,
                recipient.legal_entity_id AS recipient_id,
                ac.type_name AS category,
                EXTRACT(YEAR FROM (CAST(action_date AS DATE) + INTERVAL '3 month')) AS fiscal_year
            FROM
                transaction_fabs_new AS tfn
                LEFT OUTER JOIN
                award_category AS ac ON ac.type_code = tfn.assistance_type
                INNER JOIN
                references_location_new AS pop_location ON tfn.afa_generated_unique = pop_location.transaction_unique_id AND pop_location.is_fpds = FALSE AND pop_location.place_of_performance_flag = TRUE
                INNER JOIN
                legal_entity_new AS recipient ON tfn.afa_generated_unique = recipient.transaction_unique_id AND recipient.is_fpds = FALSE
                INNER JOIN
                agency_lookup AS awarding_agency ON awarding_agency.subtier_code = tfn.awarding_sub_tier_agency_c
                LEFT OUTER JOIN
                agency_lookup AS funding_agency ON funding_agency.subtier_code = tfn.funding_sub_tier_agency_co
            WHERE tfn.record_type = '2'
            window w AS (partition BY tfn.fain, tfn.awarding_sub_tier_agency_c)
            ORDER BY
                tfn.fain,
                tfn.awarding_sub_tier_agency_c,
                tfn.action_date DESC,
                tfn.award_modification_amendme DESC
        )

        UNION ALL

        (
            -- TRANSACTION NORMALIZED: FABS - URI
            SELECT
                DISTINCT ON (tfn.uri, tfn.awarding_sub_tier_agency_c)
                'asst_aw_' ||
                    coalesce(tfn.awarding_sub_tier_agency_c,'-none-') || '_' ||
                    '-none-' || '_' ||
                    coalesce(tfn.uri, '-none-') AS generated_unique_award_id,
                FALSE as is_fpds,
                tfn.afa_generated_unique AS transaction_unique_id,
                'DBR'::TEXT AS data_source,
                tfn.assistance_type AS type,
                CASE
                    WHEN tfn.assistance_type = '02' THEN 'Block Grant'
                    WHEN tfn.assistance_type = '03' THEN 'Formula Grant'
                    WHEN tfn.assistance_type = '04' THEN 'Project Grant'
                    WHEN tfn.assistance_type = '05' THEN 'Cooperative Agreement'
                    WHEN tfn.assistance_type = '06' THEN 'Direct Payment for Specified Use'
                    WHEN tfn.assistance_type = '07' THEN 'Direct Loan'
                    WHEN tfn.assistance_type = '08' THEN 'Guaranteed/Insured Loan'
                    WHEN tfn.assistance_type = '09' THEN 'Insurance'
                    WHEN tfn.assistance_type = '10' THEN 'Direct Payment with Unrestricted Use'
                    WHEN tfn.assistance_type = '11' THEN 'Other Financial Assistance'
                END AS type_description,
                NULL::TEXT AS piid,
                NULL::TEXST AS parent_award_piid,
                NULL::TEXT AS fain,
                tfn.uri AS uri,
                SUM(COALESCE(tfn.federal_action_obligation::DOUBLE PRECISION, 0::DOUBLE PRECISION)) over w AS total_obligation,
                NULL::FLOAT AS base_and_all_options_value,
                SUM(COALESCE(tfn.original_loan_subsidy_cost::DOUBLE PRECISION, 0::DOUBLE PRECISION)) over w AS total_subsidy_cost,
                NULL::float AS total_outlay,
                MIN(tfn.action_date) over w AS date_signed,
                tfn.award_description AS description,
                MIN(tfn.period_of_performance_star::date) over w AS period_of_performance_start_date,
                MAX(tfn.period_of_performance_curr::date) over w AS period_of_performance_current_end_date,
                NULL::FLOAT AS potential_total_value_of_award,
                tfn.modified_at::date AS last_modified_date,
                MAX(tfn.action_date) over w AS certified_date,
                NOW() AS create_date,
                NOW() AS update_date,
                0::DOUBLE PRECISION AS total_subaward_amount,
                0::INT AS subaward_count,
                awarding_agency.agency_id AS awarding_agency_id,
                funding_agency.agency_id AS funding_agency_id,
                tfn.transaction_id AS latest_transaction_id,
                NULL::INT AS parent_award_id,
                pop_location.location_id AS place_of_performance_id,
                recipient.legal_entity_id AS recipient_id,
                ac.type_name AS category,
                EXTRACT(YEAR FROM (CAST(action_date AS DATE) + INTERVAL '3 month')) AS fiscal_year
            FROM
                transaction_fabs_new AS tfn
                LEFT OUTER JOIN
                award_category AS ac ON ac.type_code = tfn.assistance_type
                INNER JOIN
                references_location_new AS pop_location ON tfn.afa_generated_unique = pop_location.transaction_unique_id AND pop_location.is_fpds = FALSE AND pop_location.place_of_performance_flag = TRUE
                INNER JOIN
                legal_entity_new AS recipient ON tfn.afa_generated_unique = recipient.transaction_unique_id AND recipient.is_fpds = FALSE
                INNER JOIN
                agency_lookup AS awarding_agency ON awarding_agency.subtier_code = tfn.awarding_sub_tier_agency_c
                LEFT OUTER JOIN
                agency_lookup AS funding_agency ON funding_agency.subtier_code = tfn.funding_sub_tier_agency_co
            WHERE tfn.record_type = '1'
            window w AS (partition BY tfn.uri, tfn.awarding_sub_tier_agency_c)
            ORDER BY
                tfn.uri,
                tfn.awarding_sub_tier_agency_c,
                tfn.action_date DESC,
                tfn.award_modification_amendme DESC
        )
    ) AS awards
);