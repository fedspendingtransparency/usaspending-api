DROP TABLE IF EXISTS transaction_normalized_new;
DROP SEQUENCE IF EXISTS tx_norm_id_seq CASCADE;

CREATE SEQUENCE tx_norm_id_seq;
CREATE TABLE transaction_normalized_new AS (
    SELECT
        NEXTVAL('tx_norm_id_seq') AS id,
        *
    FROM
    (
        (
            -- TRANSACTION NORMALIZED: FPDS
            SELECT
                TRUE AS is_fpds,
                transaction_fpds_new.detached_award_proc_unique AS transaction_unique_id,
                transaction_fpds_new.unique_award_key,
                NULL AS usaspending_unique_transaction_id,
                contract_award_type AS type,
                contract_award_type_desc AS type_description,
                period_of_performance_star::DATE AS period_of_performance_start_date,
                period_of_performance_curr::DATE AS period_of_performance_current_end_date,
                action_date::DATE AS action_date,
                action_type,
                action_type_description,
                federal_action_obligation::NUMERIC AS federal_action_obligation,
                award_modification_amendme AS modification_number,
                award_description AS description,
                NULL AS drv_award_transaction_usaspend,
                NULL AS drv_current_total_award_value_amount_adjustment,
                NULL AS drv_potential_total_award_value_amount_adjustment,
                last_modified::DATE AS last_modified_date,
                NULL::DATE AS certified_date,
                CURRENT_TIMESTAMP AS create_date,
                CURRENT_TIMESTAMP AS update_date,
                EXTRACT(YEAR FROM (CAST(action_date AS DATE) + INTERVAL '3 month'))::INT AS fiscal_year,
                NULL::BIGINT AS award_id,
                awarding_agency.agency_id AS awarding_agency_id,
                funding_agency.agency_id AS funding_agency_id,
                pop_location.location_id AS place_of_performance_id,
                recipient.legal_entity_id AS recipient_id
            FROM
                transaction_fpds_new
                INNER JOIN
                references_location_new AS pop_location ON transaction_fpds_new.detached_award_proc_unique = pop_location.transaction_unique_id AND pop_location.is_fpds = TRUE AND pop_location.place_of_performance_flag = TRUE
                INNER JOIN
                legal_entity_new AS recipient ON transaction_fpds_new.detached_award_proc_unique = recipient.transaction_unique_id AND recipient.is_fpds = TRUE
                INNER JOIN
                agency_lookup AS awarding_agency ON awarding_agency.subtier_code = awarding_sub_tier_agency_c
                LEFT OUTER JOIN
                agency_lookup AS funding_agency ON funding_agency.subtier_code = funding_sub_tier_agency_co
        )

        UNION ALL

        (
            -- TRANSACTION NORMALIZED: FABS
            SELECT
                FALSE as is_fpds,
                transaction_fabs_new.afa_generated_unique AS transaction_unique_id,
                transaction_fabs_new.unique_award_key,
                NULL AS usaspending_unique_transaction_id,
                assistance_type AS type,
                assistance_type_desc AS type_description,
                period_of_performance_star AS period_of_performance_start_date,
                period_of_performance_curr AS period_of_performance_current_end_date,
                action_date::DATE AS action_date,
                action_type,
                action_type_description AS action_type_description,
                federal_action_obligation::NUMERIC AS federal_action_obligation,
                award_modification_amendme AS modification_number,
                award_description AS description,
                NULL AS drv_award_transaction_usaspend,
                NULL AS drv_current_total_award_value_amount_adjustment,
                NULL AS drv_potential_total_award_value_amount_adjustment,
                modified_at::DATE AS last_modified_date,
                NULL::DATE AS certified_date,
                CURRENT_TIMESTAMP AS create_date,
                CURRENT_TIMESTAMP AS update_date,
                EXTRACT(YEAR FROM (CAST(action_date AS DATE) + INTERVAL '3 month'))::INT AS fiscal_year,
                NULL::BIGINT AS award_id,
                awarding_agency.agency_id AS awarding_agency_id,
                funding_agency.agency_id AS funding_agency_id,
                pop_location.location_id AS place_of_performance_id,
                recipient.legal_entity_id AS recipient_id
            FROM
                transaction_fabs_new
                INNER JOIN
                references_location_new AS pop_location ON transaction_fabs_new.afa_generated_unique = pop_location.transaction_unique_id AND pop_location.is_fpds = FALSE AND pop_location.place_of_performance_flag = TRUE
                INNER JOIN
                legal_entity_new AS recipient ON transaction_fabs_new.afa_generated_unique = recipient.transaction_unique_id AND recipient.is_fpds = FALSE
                INNER JOIN
                agency_lookup AS awarding_agency ON awarding_agency.subtier_code = awarding_sub_tier_agency_c
                LEFT OUTER JOIN
                agency_lookup AS funding_agency ON funding_agency.subtier_code = funding_sub_tier_agency_co
        )
    ) AS transaction_normalized
);
