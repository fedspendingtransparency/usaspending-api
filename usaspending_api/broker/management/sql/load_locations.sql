DROP TABLE IF EXISTS references_location_new;

CREATE TABLE references_location_new AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY 1) AS location_id,
        *
    FROM
    (
        -- RECIPIENT LOCATION: FPDS
        (
            SELECT
                 legal_entity_country_code AS location_country_code,
                 legal_entity_country_name AS country_name,
                 legal_entity_state_code AS state_code,
                 legal_entity_state_descrip AS state_name,
                 NULL AS state_description,
                 legal_entity_city_name AS city_name,
                 NULL AS city_code,
                 legal_entity_county_name AS county_name,
                 legal_entity_county_code AS county_code,
                 legal_entity_address_line1 AS address_line1,
                 legal_entity_address_line2 AS address_line2,
                 legal_entity_address_line3 AS address_line3,
                 NULL AS foreign_location_description,
                 legal_entity_zip4 AS zip4,
                 NULL AS zip_4a,
                 legal_entity_congressional AS congressional_code,
                 NULL AS performance_code,
                 legal_entity_zip_last4 AS zip_last4,
                 legal_entity_zip5 AS zip5,
                 NULL AS foreign_postal_code,
                 NULL AS foreign_province,
                 NULL AS foreign_city_name,
                 NULL AS reporting_period_start,
                 NULL AS reporting_period_end,
                 NULLIF(last_modified, '')::DATE AS last_modified_date,
                 NULLIF(action_date, '')::DATE AS certified_date,
                 CURRENT_TIMESTAMP AS create_date,
                 CURRENT_TIMESTAMP AS update_date,
                 'DBR'::TEXT AS data_source,
                 FALSE AS place_of_performance_flag,
                 TRUE AS recipient_flag,
                 TRUE AS is_fpds,
                 detached_award_proc_unique AS transaction_unique_id
            FROM
                transaction_fpds_new
        )

        UNION ALL

        -- PPOP LOCATION: FPDS
        (
            SELECT
                 place_of_perform_country_c AS location_country_code,
                 place_of_perf_country_desc AS country_name,
                 place_of_performance_state AS state_code,
                 place_of_perfor_state_desc AS state_name,
                 NULL AS state_description,
                 place_of_perform_city_name AS city_name,
                 NULL AS city_code,
                 place_of_perform_county_na AS county_name,
                 place_of_perform_county_co AS county_code,
                 NULL AS address_line1,
                 NULL AS address_line2,
                 NULL AS address_line3,
                 NULL AS foreign_location_description,
                 NULL AS zip4,
                 place_of_performance_zip4a AS zip_4a,
                 place_of_performance_congr AS congressional_code,
                 NULL AS performance_code,
                 place_of_perform_zip_last4 AS zip_last4,
                 place_of_performance_zip5 AS zip5,
                 NULL AS foreign_postal_code,
                 NULL AS foreign_province,
                 NULL AS foreign_city_name,
                 NULL AS reporting_period_start,
                 NULL AS reporting_period_end,
                 NULLIF(last_modified, '')::DATE AS last_modified_date,
                 NULLIF(action_date, '')::DATE AS certified_date,
                 CURRENT_TIMESTAMP AS create_date,
                 CURRENT_TIMESTAMP AS update_date,
                 'DBR'::TEXT AS data_source,
                 TRUE AS place_of_performance_flag,
                 FALSE AS recipient_flag,
                 TRUE AS is_fpds,
                 detached_award_proc_unique AS transaction_unique_id
            FROM
                transaction_fpds_new
        )

        UNION ALL

        -- RECIPIENT LOCATION: FABS
        (
            SELECT
                 legal_entity_country_code AS location_country_code,
                 legal_entity_country_name AS country_name,
                 legal_entity_state_code AS state_code,
                 legal_entity_state_name AS state_name,
                 NULL AS state_description,
                 legal_entity_city_name AS city_name,
                 legal_entity_city_code AS city_code,
                 legal_entity_county_name AS county_name,
                 legal_entity_county_code AS county_code,
                 legal_entity_address_line1 AS address_line1,
                 legal_entity_address_line2 AS address_line2,
                 legal_entity_address_line3 AS address_line3,
                 legal_entity_foreign_descr AS foreign_location_description,
                 NULL AS zip4,
                 NULL AS zip_4a,
                 legal_entity_congressional AS congressional_code,
                 NULL AS performance_code,
                 legal_entity_zip_last4 AS zip_last4,
                 legal_entity_zip5 AS zip5,
                 legal_entity_foreign_posta AS foreign_postal_code,
                 legal_entity_foreign_provi AS foreign_province,
                 legal_entity_foreign_city AS foreign_city_name,
                 NULL AS reporting_period_start,
                 NULL AS reporting_period_end,
                 modified_at::DATE AS last_modified_date,
                 action_date::DATE AS certified_date,
                 CURRENT_TIMESTAMP AS create_date,
                 CURRENT_TIMESTAMP AS update_date,
                 'DBR'::TEXT AS data_source,
                 FALSE AS place_of_performance_flag,
                 TRUE AS recipient_flag,
                 FALSE AS is_fpds,
                 afa_generated_unique AS transaction_unique_id
            FROM
                transaction_fabs_new
        )

        UNION ALL

        -- PPOP LOCATION: FABS
        (
            SELECT
                 place_of_perform_country_c AS location_country_code,
                 place_of_perform_country_n AS country_name,
                 place_of_perfor_state_code AS state_code,
                 place_of_perform_state_nam AS state_name,
                 NULL AS state_description,
                 place_of_performance_city AS city_name,
                 NULL AS city_code,
                 place_of_perform_county_na AS county_name,
                 place_of_perform_county_co AS county_code,
                 NULL AS address_line1,
                 NULL AS address_line2,
                 NULL AS address_line3,
                 place_of_performance_forei AS foreign_location_description,
                 NULL AS zip4,
                 place_of_performance_zip4a AS zip_4a,
                 place_of_performance_congr AS congressional_code,
                 place_of_performance_code AS performance_code,
                 place_of_perform_zip_last4 AS zip_last4,
                 place_of_performance_zip5 AS zip5,
                 NULL AS foreign_postal_code,
                 NULL AS foreign_province,
                 NULL AS foreign_city_name,
                 NULL AS reporting_period_start,
                 NULL AS reporting_period_end,
                 modified_at::DATE AS last_modified_date,
                 action_date::DATE AS certified_date,
                 CURRENT_TIMESTAMP AS create_date,
                 CURRENT_TIMESTAMP AS update_date,
                 'DBR'::TEXT AS data_source,
                 TRUE AS place_of_performance_flag,
                 FALSE AS recipient_flag,
                 FALSE AS is_fpds,
                 afa_generated_unique AS transaction_unique_id
            FROM
                transaction_fabs_new
        )
    ) AS locations
);