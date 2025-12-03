RECIPIENT_LOOKUP_COLUMNS_WITHOUT_ID = {
    "recipient_hash": {"delta": "STRING", "postgres": "UUID"},
    "legal_business_name": {"delta": "STRING", "postgres": "TEXT"},
    "duns": {"delta": "STRING", "postgres": "TEXT"},
    "address_line_1": {"delta": "STRING", "postgres": "TEXT"},
    "address_line_2": {"delta": "STRING", "postgres": "TEXT"},
    "business_types_codes": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]"},
    "city": {"delta": "STRING", "postgres": "TEXT"},
    "congressional_district": {"delta": "STRING", "postgres": "TEXT"},
    "country_code": {"delta": "STRING", "postgres": "TEXT"},
    "parent_duns": {"delta": "STRING", "postgres": "TEXT"},
    "parent_legal_business_name": {"delta": "STRING", "postgres": "TEXT"},
    "state": {"delta": "STRING", "postgres": "TEXT"},
    "zip4": {"delta": "STRING", "postgres": "TEXT"},
    "zip5": {"delta": "STRING", "postgres": "TEXT"},
    "alternate_names": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]"},
    "source": {"delta": "STRING NOT NULL", "postgres": "TEXT NOT NULL"},
    "update_date": {"delta": "TIMESTAMP NOT NULL", "postgres": "TIMESTAMP NOT NULL"},
    "uei": {"delta": "STRING", "postgres": "TEXT"},
    "parent_uei": {"delta": "STRING", "postgres": "TEXT"},
}
RECIPIENT_LOOKUP_COLUMNS = {
    "id": {"delta": "LONG", "postgres": "BIGINT NOT NULL"},
    **RECIPIENT_LOOKUP_COLUMNS_WITHOUT_ID,
}

RECIPIENT_LOOKUP_DELTA_COLUMNS = {k: v["delta"] for k, v in RECIPIENT_LOOKUP_COLUMNS.items()}
RPT_RECIPIENT_LOOKUP_DELTA_COLUMNS = {k: v["delta"] for k, v in RECIPIENT_LOOKUP_COLUMNS_WITHOUT_ID.items()}
RECIPIENT_LOOKUP_POSTGRES_COLUMNS = {k: v["postgres"] for k, v in RECIPIENT_LOOKUP_COLUMNS_WITHOUT_ID.items()}

TEMP_RECIPIENT_LOOKUP_COLUMNS = {
    "duns_recipient_hash": "STRING",
    **{k: v for k, v in RPT_RECIPIENT_LOOKUP_DELTA_COLUMNS.items()},
}


recipient_lookup_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in RECIPIENT_LOOKUP_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """

rpt_recipient_lookup_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in RPT_RECIPIENT_LOOKUP_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """

recipient_lookup_load_sql_string_list = [
    # -----
    # Creation of a temporary view used to reference recipient information from transactions
    # -----
    r"""
    CREATE OR REPLACE TEMPORARY VIEW temp_transaction_recipients_view AS (
        SELECT
            tn.transaction_unique_id,
            tn.is_fpds,
            REGEXP_REPLACE(
                MD5(UPPER(
                    CASE
                        WHEN COALESCE(fpds.awardee_or_recipient_uei, fabs.uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(fpds.awardee_or_recipient_uei, fabs.uei))
                        WHEN COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu))
                        ELSE CONCAT('name-', COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal, '')) END
                )),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS recipient_hash,
            REGEXP_REPLACE(
                MD5(UPPER(
                    CASE
                        WHEN COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu))
                        ELSE CONCAT('name-', COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal, '')) END
                )),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS duns_recipient_hash,
            REGEXP_REPLACE(
                MD5(UPPER(
                    CASE
                        WHEN COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei))
                        WHEN COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide) IS NOT  NULL THEN CONCAT('duns-', COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide))
                        ELSE CONCAT('name-', COALESCE(fpds.ultimate_parent_legal_enti, fabs.ultimate_parent_legal_enti, '')) END
                )),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS parent_recipient_hash,
            REGEXP_REPLACE(
                MD5(UPPER(
                    CASE
                        WHEN COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide))
                        ELSE CONCAT('name-', COALESCE(fpds.ultimate_parent_legal_enti, fabs.ultimate_parent_legal_enti, '')) END
                )),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS duns_parent_recipient_hash,
            COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) AS awardee_or_recipient_uniqu,
            COALESCE(fpds.awardee_or_recipient_uei, fabs.uei) AS uei,
            COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei) AS ultimate_parent_uei,
            UPPER(COALESCE(fpds.ultimate_parent_legal_enti, fabs.ultimate_parent_legal_enti)) AS ultimate_parent_legal_enti,
            COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide) AS ultimate_parent_unique_ide,
            UPPER(COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal)) AS awardee_or_recipient_legal,
            COALESCE(fpds.legal_entity_city_name, fabs.legal_entity_city_name) AS city,
            COALESCE(fpds.legal_entity_state_code, fabs.legal_entity_state_code) AS state,
            COALESCE(fpds.legal_entity_zip5, fabs.legal_entity_zip5) AS zip5,
            COALESCE(fpds.legal_entity_zip_last4, fabs.legal_entity_zip_last4) AS zip4,
            COALESCE(fpds.legal_entity_congressional, fabs.legal_entity_congressional) AS congressional_district,
            COALESCE(fpds.legal_entity_address_line1, fabs.legal_entity_address_line1) AS address_line_1,
            COALESCE(fpds.legal_entity_address_line2, fabs.legal_entity_address_line1) AS address_line_2,
            COALESCE(fpds.legal_entity_country_code, fabs.legal_entity_country_code) AS country_code,
            tn.action_date,
            CASE
                WHEN tn.is_fpds = TRUE THEN CAST('fpds' AS STRING)
                ELSE CAST('fabs' AS STRING)
            END AS source
        FROM int.transaction_normalized AS tn
        LEFT OUTER JOIN int.transaction_fpds AS fpds ON (tn.id = fpds.transaction_id)
        LEFT OUTER JOIN int.transaction_fabs AS fabs ON (tn.id = fabs.transaction_id)
        WHERE tn.action_date >= '2007-10-01'
    )
    """,
    # -----
    # Identify recipients that should be ingested
    # -----
    rf""" CREATE OR REPLACE TEMPORARY VIEW temp_recipient_filter AS (
        SELECT
            DISTINCT
                REGEXP_REPLACE(
                    MD5(UPPER(
                        CASE WHEN sr.uei IS NOT NULL THEN CONCAT('uei-', sr.uei)
                        ELSE CONCAT('duns-', COALESCE(sr.awardee_or_recipient_uniqu, '')) END
                    )),
                    '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$',
                    '\$1-\$2-\$3-\$4-\$5'
                ) AS recipient_hash,
                REGEXP_REPLACE(
                    MD5(UPPER(
                        CASE WHEN sr.ultimate_parent_uei IS NOT NULL THEN CONCAT('uei-', sr.ultimate_parent_uei)
                        ELSE CONCAT('duns-', COALESCE(sr.ultimate_parent_unique_ide, '')) END
                    )),
                    '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$',
                    '\$1-\$2-\$3-\$4-\$5'
                ) AS parent_recipient_hash

            FROM int.sam_recipient sr
            WHERE
                /*
                    It is acknowledged that these EXISTS could be simplified. However, in testing on Databricks
                    it was found that using each "uei" check as a single WHERE clause in different EXISTS statements
                    allows for better parallelization during processing and drastically affects performance
                */
                EXISTS (
                    SELECT 1
                    FROM raw.published_fabs pf
                    WHERE sr.uei = pf.uei
                )
                OR EXISTS (
                    SELECT 1
                    FROM raw.published_fabs pf
                    WHERE sr.uei = pf.ultimate_parent_uei
                )
                OR EXISTS (
                    SELECT 1
                    FROM raw.detached_award_procurement dap
                    WHERE sr.uei = dap.awardee_or_recipient_uei
                )
                OR EXISTS (
                    SELECT 1
                    FROM raw.detached_award_procurement dap
                    WHERE sr.uei = dap.ultimate_parent_uei
                )
    )""",
    # -----
    # Populate the temporary_restock_recipient_lookup table
    # -----
    rf"""
    CREATE OR REPLACE TEMPORARY VIEW temp_collect_recipients_view AS (
        WITH latest_duns_sam AS (
            SELECT s.*
                FROM (
                    SELECT
                        1 AS priority,
                        -- This regex is used to create a UUID formatted STRING since Spark does not
                        -- have a UUID type; all UUIDs have the format of 8-4-4-4-12
                        REGEXP_REPLACE(
                            MD5(UPPER(
                                CASE WHEN sr.uei IS NOT NULL THEN CONCAT('uei-', sr.uei)
                                ELSE CONCAT('duns-', COALESCE(sr.awardee_or_recipient_uniqu, '')) END
                            )),
                            '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$',
                            '\$1-\$2-\$3-\$4-\$5'
                        ) AS recipient_hash,
                        REGEXP_REPLACE(
                            MD5(UPPER(CONCAT('duns-', COALESCE(sr.awardee_or_recipient_uniqu, '')))),
                            '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$',
                            '\$1-\$2-\$3-\$4-\$5'
                        ) AS duns_recipient_hash,
                        UPPER(sr.legal_business_name) AS legal_business_name,
                        sr.awardee_or_recipient_uniqu AS duns,
                        sr.uei,
                        'sam' AS source,
                        sr.address_line_1,
                        sr.address_line_2,
                        sr.business_types_codes,
                        sr.city,
                        sr.congressional_district,
                        sr.country_code,
                        sr.ultimate_parent_unique_ide AS parent_duns,
                        UPPER(sr.ultimate_parent_legal_enti) AS parent_legal_business_name,
                        sr.ultimate_parent_uei AS parent_uei,
                        sr.state,
                        sr.zip4,
                        sr.zip AS zip5,
                        sr.update_date,
                        ARRAY() AS alternate_names,
                        ROW_NUMBER() OVER (
                            PARTITION BY sr.uei, sr.awardee_or_recipient_uniqu
                            -- Spark and Postgres handle NULL values in sorting different ways by default
                            ORDER BY
                                sr.uei ASC NULLS LAST,
                                sr.awardee_or_recipient_uniqu ASC NULLS LAST,
                                sr.update_date DESC NULLS FIRST
                        ) AS row_num
                    FROM int.sam_recipient sr

                    WHERE COALESCE(sr.uei, sr.awardee_or_recipient_uniqu) IS NOT NULL
                        AND sr.legal_business_name IS NOT NULL
                ) s

            JOIN temp_recipient_filter rf
            ON rf.recipient_hash = s.recipient_hash
        ),
        latest_tx AS (
            SELECT
                2 AS priority,
                recipient_hash,
                duns_recipient_hash,
                awardee_or_recipient_legal AS legal_business_name,
                awardee_or_recipient_uniqu AS duns,
                uei,
                source,
                address_line_1,
                address_line_2,
                NULL AS business_types_codes,
                city,
                congressional_district,
                country_code,
                ultimate_parent_unique_ide AS parent_duns,
                ultimate_parent_legal_enti AS parent_legal_business_name,
                ultimate_parent_uei AS parent_uei,
                state,
                zip4,
                zip5,
                action_date AS update_date,
                ARRAY() AS alternate_names,
                ROW_NUMBER() OVER (
                    PARTITION BY recipient_hash
                    ORDER BY
                        action_date DESC NULLS FIRST,
                        is_fpds ASC NULLS LAST,
                        transaction_unique_id ASC NULLS LAST
                ) AS row_num
            FROM temp_transaction_recipients_view
            WHERE COALESCE(uei, awardee_or_recipient_uniqu) IS NOT NULL AND awardee_or_recipient_legal IS NOT NULL
        ),
        latest_duns_sam_parent AS (
            SELECT s.*
                FROM (
                    SELECT
                        3 AS priority,
                        REGEXP_REPLACE(
                            MD5(UPPER(
                                CASE WHEN sr.ultimate_parent_uei IS NOT NULL THEN CONCAT('uei-', sr.ultimate_parent_uei)
                                ELSE CONCAT('duns-', COALESCE(sr.ultimate_parent_unique_ide, '')) END
                            )),
                            '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$',
                            '\$1-\$2-\$3-\$4-\$5'
                        ) AS recipient_hash,
                        REGEXP_REPLACE(
                            MD5(UPPER(CONCAT('duns-', COALESCE(sr.ultimate_parent_unique_ide, '')))),
                            '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$',
                            '\$1-\$2-\$3-\$4-\$5'
                        ) AS duns_recipient_hash,
                        UPPER(sr.ultimate_parent_legal_enti) AS legal_business_name,
                        sr.ultimate_parent_unique_ide AS duns,
                        sr.ultimate_parent_uei AS uei,
                        'sam-parent' AS source,
                        NULL AS address_line_1,
                        NULL AS address_line_2,
                        NULL AS business_types_codes,
                        NULL AS city,
                        NULL AS congressional_district,
                        NULL AS country_code,
                        sr.ultimate_parent_unique_ide AS parent_duns,
                        UPPER(sr.ultimate_parent_legal_enti) AS parent_legal_business_name,
                        sr.ultimate_parent_uei AS parent_uei,
                        NULL AS state,
                        NULL AS zip4,
                        NULL AS zip5,
                        sr.update_date,
                        ARRAY() AS alternate_names,
                        ROW_NUMBER() OVER (
                            -- TODO: This should be swapped; left in place temporarily to match current
                            PARTITION BY sr.ultimate_parent_unique_ide, sr.ultimate_parent_uei
                            ORDER BY
                                sr.ultimate_parent_unique_ide ASC NULLS LAST,
                                sr.ultimate_parent_uei ASC NULLS LAST,
                                sr.update_date DESC NULLS LAST,
                                -- Order by the legal_business_name as a last resort for cases where all other columns
                                -- are the same within the same partition.
                                UPPER(sr.ultimate_parent_legal_enti)
                        ) AS row_num
                    FROM int.sam_recipient sr

                    WHERE COALESCE(sr.ultimate_parent_uei, sr.ultimate_parent_unique_ide) IS NOT NULL
                        AND sr.ultimate_parent_legal_enti IS NOT NULL
                ) s

                JOIN temp_recipient_filter rf
                ON rf.parent_recipient_hash = s.recipient_hash
        ),
        latest_tx_parent AS (
            SELECT
                4 AS priority,
                parent_recipient_hash AS recipient_hash,
                duns_parent_recipient_hash AS duns_recipient_hash,
                ultimate_parent_legal_enti AS legal_business_name,
                ultimate_parent_unique_ide AS duns,
                ultimate_parent_uei AS uei,
                CONCAT(source, '-parent') AS source,
                NULL AS address_line_1,
                NULL AS address_line_2,
                NULL AS business_types_codes,
                NULL AS city,
                NULL AS congressional_district,
                NULL AS country_code,
                ultimate_parent_unique_ide AS parent_duns,
                ultimate_parent_legal_enti AS parent_legal_business_name,
                ultimate_parent_uei AS parent_uei,
                NULL AS state,
                NULL AS zip4,
                NULL AS zip5,
                action_date AS update_date,
                ARRAY() AS alternate_names,
                ROW_NUMBER() OVER (
                    PARTITION BY parent_recipient_hash
                    ORDER BY
                        action_date DESC NULLS FIRST,
                        is_fpds ASC NULLS LAST,
                        transaction_unique_id ASC NULLS LAST
                ) AS row_num
            FROM temp_transaction_recipients_view
            WHERE COALESCE(ultimate_parent_uei, ultimate_parent_unique_ide) IS NOT NULL AND ultimate_parent_legal_enti IS NOT NULL
        ),
        latest_duns_sam_no_name AS (
            SELECT s.*
                FROM (
                    SELECT
                        5 AS priority,
                        REGEXP_REPLACE(
                            MD5(UPPER(
                                CASE WHEN sr.uei IS NOT NULL THEN CONCAT('uei-', sr.uei)
                                ELSE CONCAT('duns-', COALESCE(sr.awardee_or_recipient_uniqu, '')) END
                            )),
                            '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$',
                            '\$1-\$2-\$3-\$4-\$5'
                        ) AS recipient_hash,
                        REGEXP_REPLACE(
                            MD5(UPPER(CONCAT('duns-', COALESCE(sr.awardee_or_recipient_uniqu, '')))),
                            '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$',
                            '\$1-\$2-\$3-\$4-\$5'
                        ) AS duns_recipient_hash,
                        UPPER(sr.legal_business_name) AS legal_business_name,
                        sr.awardee_or_recipient_uniqu AS duns,
                        sr.uei,
                        'sam' AS source,
                        sr.address_line_1,
                        sr.address_line_2,
                        sr.business_types_codes,
                        sr.city,
                        sr.congressional_district,
                        sr.country_code,
                        sr.ultimate_parent_unique_ide AS parent_duns,
                        UPPER(sr.ultimate_parent_legal_enti) AS parent_legal_business_name,
                        sr.ultimate_parent_uei AS parent_uei,
                        sr.state,
                        sr.zip4,
                        sr.zip AS zip5,
                        sr.update_date,
                        ARRAY() AS alternate_names,
                        ROW_NUMBER() OVER (
                            PARTITION BY sr.uei, sr.awardee_or_recipient_uniqu
                            ORDER BY
                                sr.uei ASC NULLS LAST,
                                sr.awardee_or_recipient_uniqu ASC NULLS LAST,
                                sr.update_date DESC NULLS FIRST
                        ) AS row_num
                    FROM int.sam_recipient sr

                    WHERE COALESCE(sr.uei, sr.awardee_or_recipient_uniqu) IS NOT NULL
                        AND sr.legal_business_name IS NULL
                ) s

                JOIN temp_recipient_filter rf
                ON rf.recipient_hash = s.recipient_hash
        ),
        latest_tx_no_name AS (
            SELECT
                6 AS priority,
                recipient_hash,
                duns_recipient_hash,
                awardee_or_recipient_legal AS legal_business_name,
                awardee_or_recipient_uniqu AS duns,
                uei,
                source,
                address_line_1,
                address_line_2,
                NULL AS business_types_codes,  -- transactions only have business_categories, not the type codes
                city,
                congressional_district,
                country_code,
                ultimate_parent_unique_ide AS parent_duns,
                ultimate_parent_legal_enti AS parent_legal_business_name,
                ultimate_parent_uei AS parent_uei,
                state,
                zip4,
                zip5,
                action_date AS update_date,
                ARRAY() AS alternate_names,
                ROW_NUMBER() OVER (
                    PARTITION BY recipient_hash
                    ORDER BY
                        action_date DESC NULLS FIRST,
                        is_fpds ASC NULLS LAST,
                        transaction_unique_id ASC NULLS LAST
                ) AS row_num
            FROM temp_transaction_recipients_view
            WHERE COALESCE(uei, awardee_or_recipient_uniqu) IS NOT NULL AND awardee_or_recipient_legal IS NULL
        ),
        latest_duns_sam_parent_no_name AS (
            SELECT s.*
                FROM (
                    SELECT
                        7 AS priority,
                        REGEXP_REPLACE(
                            MD5(UPPER(
                                CASE WHEN sr.ultimate_parent_uei IS NOT NULL THEN CONCAT('uei-', sr.ultimate_parent_uei)
                                ELSE CONCAT('duns-', COALESCE(sr.ultimate_parent_unique_ide, '')) END
                            )),
                            '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$',
                            '\$1-\$2-\$3-\$4-\$5'
                        ) AS recipient_hash,
                        REGEXP_REPLACE(
                            MD5(UPPER(CONCAT('duns-', COALESCE(sr.ultimate_parent_unique_ide, '')))),
                            '^(\.{{{{8}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{4}}}})(\.{{{{12}}}})$',
                            '\$1-\$2-\$3-\$4-\$5'
                        ) AS duns_recipient_hash,
                        NULL AS legal_business_name,
                        sr.ultimate_parent_unique_ide AS duns,
                        sr.ultimate_parent_uei AS uei,
                        'sam-parent' AS source,
                        NULL AS address_line_1,
                        NULL AS address_line_2,
                        NULL AS business_types_codes,
                        NULL AS city,
                        NULL AS congressional_district,
                        NULL AS country_code,
                        sr.ultimate_parent_unique_ide AS parent_duns,
                        NULL AS parent_legal_business_name,
                        sr.ultimate_parent_uei AS parent_uei,
                        NULL AS state,
                        NULL AS zip4,
                        NULL AS zip5,
                        sr.update_date,
                        ARRAY() AS alternate_names,
                        ROW_NUMBER() OVER (
                            PARTITION BY sr.ultimate_parent_uei, sr.ultimate_parent_unique_ide
                            ORDER BY
                                sr.ultimate_parent_uei ASC NULLS LAST,
                                sr.ultimate_parent_unique_ide ASC NULLS LAST,
                                sr.update_date DESC NULLS FIRST
                        ) AS row_num
                    FROM int.sam_recipient sr

                    WHERE COALESCE(sr.ultimate_parent_uei, sr.ultimate_parent_unique_ide) IS NOT NULL
                        AND sr.ultimate_parent_legal_enti IS NULL
                ) s

                JOIN temp_recipient_filter rf
                ON rf.parent_recipient_hash = s.recipient_hash
        ),
        latest_tx_parent_no_name AS (
            SELECT
                8 AS priority,
                parent_recipient_hash AS recipient_hash,
                duns_parent_recipient_hash AS duns_recipient_hash,
                NULL AS legal_business_name,
                ultimate_parent_unique_ide AS duns,
                ultimate_parent_uei AS uei,
                CONCAT(source, '-parent') AS source,
                NULL AS address_line_1,
                NULL AS address_line_2,
                NULL AS business_types_codes,
                NULL AS city,
                NULL AS congressional_district,
                NULL AS country_code,
                ultimate_parent_unique_ide AS parent_duns,
                NULL AS parent_legal_business_name,
                ultimate_parent_uei AS parent_uei,
                NULL AS state,
                NULL AS zip4,
                NULL AS zip5,
                action_date AS update_date,
                ARRAY() AS alternate_names,
                ROW_NUMBER() OVER (
                    PARTITION BY parent_recipient_hash
                    ORDER BY
                        action_date DESC NULLS FIRST,
                        is_fpds ASC NULLS LAST,
                        transaction_unique_id ASC NULLS LAST
                ) AS row_num
            FROM temp_transaction_recipients_view
            WHERE COALESCE(ultimate_parent_uei, ultimate_parent_unique_ide) IS NOT NULL AND ultimate_parent_legal_enti IS NULL
        ),
        latest_tx_no_fabs_fpds AS (
            SELECT
                9 AS priority,
                recipient_hash,
                duns_recipient_hash,
                awardee_or_recipient_legal AS legal_business_name,
                NULL AS duns,
                NULL AS uei,
                source,
                address_line_1,
                address_line_2,
                NULL AS business_types_codes,
                city,
                congressional_district,
                country_code,
                ultimate_parent_unique_ide AS parent_duns,
                ultimate_parent_legal_enti AS parent_legal_business_name,
                ultimate_parent_uei AS parent_uei,
                state,
                zip4,
                zip5,
                action_date AS update_date,
                ARRAY() AS alternate_names,
                ROW_NUMBER() OVER (
                    PARTITION BY recipient_hash
                    ORDER BY
                        action_date DESC NULLS FIRST,
                        is_fpds ASC NULLS LAST,
                        transaction_unique_id ASC NULLS LAST
                ) AS row_num
            FROM temp_transaction_recipients_view
            WHERE COALESCE(uei, awardee_or_recipient_uniqu) IS NULL
        ),
        union_all AS (
            SELECT * FROM latest_duns_sam WHERE row_num = 1
            UNION
            SELECT * FROM latest_tx WHERE row_num = 1
            UNION
            SELECT * FROM latest_duns_sam_parent WHERE row_num = 1
            UNION
            SELECT * FROM latest_tx_parent WHERE row_num = 1
            UNION
            SELECT * FROM latest_duns_sam_no_name WHERE row_num = 1
            UNION
            SELECT * FROM latest_tx_no_name WHERE row_num = 1
            UNION
            SELECT * FROM latest_duns_sam_parent_no_name WHERE row_num = 1
            UNION
            SELECT * FROM latest_tx_parent_no_name WHERE row_num = 1
            UNION
            SELECT * FROM latest_tx_no_fabs_fpds WHERE row_num = 1
        ),
        union_all_priority AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY recipient_hash
                    ORDER BY
                        priority ASC,
                        uei ASC NULLS LAST,
                        duns ASC NULLS LAST,
                        update_date DESC NULLS LAST
                ) AS row_num_union
            FROM union_all
        )
        SELECT
            {", ".join([val for val in TEMP_RECIPIENT_LOOKUP_COLUMNS])}
        FROM union_all_priority
        WHERE row_num_union = 1
    )
    """,
    # -----
    # Delete any cases of old recipients from where the recipient now has a UEI
    # -----
    rf"""
    CREATE OR REPLACE TEMPORARY VIEW temp_stale_recipients_removed_view AS (
        WITH uei_and_duns_recipients AS (
            SELECT duns_recipient_hash
            FROM temp_collect_recipients_view
            WHERE
                uei IS NOT NULL
                AND duns IS NOT NULL
        )
        SELECT
            {", ".join(["temp_collect_recipients_view." + val for val in RPT_RECIPIENT_LOOKUP_DELTA_COLUMNS])}
        FROM
            temp_collect_recipients_view
        LEFT OUTER JOIN uei_and_duns_recipients ON (
            temp_collect_recipients_view.recipient_hash = uei_and_duns_recipients.duns_recipient_hash
            AND temp_collect_recipients_view.uei IS NULL
        )
        WHERE
            -- Limit to the "temp_collect_recipients_view" records that do not JOIN to another record
            uei_and_duns_recipients.duns_recipient_hash IS NULL
    )
    """,
    # -----
    # Update the temporary_restock_recipient_lookup table to include any alternate names
    # -----
    rf"""
    CREATE OR REPLACE TEMPORARY VIEW temp_recipients_with_alt_names_view AS (
        WITH recipient_names (
            WITH alt_names AS (
                SELECT
                    recipient_hash,
                    COLLECT_SET(awardee_or_recipient_legal) AS all_names
                FROM temp_transaction_recipients_view
                WHERE COALESCE(awardee_or_recipient_legal, '') != ''
                GROUP BY recipient_hash
            ),
            alt_parent_names AS (
                SELECT
                    parent_recipient_hash AS recipient_hash,
                    COLLECT_SET(ultimate_parent_legal_enti) AS all_names
                FROM temp_transaction_recipients_view
                WHERE COALESCE(ultimate_parent_legal_enti, '') != ''
                GROUP BY parent_recipient_hash
            )
            SELECT
                COALESCE(an.recipient_hash, apn.recipient_hash) AS recipient_hash,
                COALESCE(
                    ARRAY_SORT(ARRAY_UNION(COALESCE(an.all_names, ARRAY()), COALESCE(apn.all_names, ARRAY()))),
                    ARRAY()
                 ) AS all_names
            FROM alt_names AS an
            FULL OUTER JOIN alt_parent_names AS apn ON an.recipient_hash = apn.recipient_hash
        )
        SELECT
            {
                ", ".join(
                    [
                        "temp_stale_recipients_removed_view." + val
                        for val
                        in set(RPT_RECIPIENT_LOOKUP_DELTA_COLUMNS) - {"alternate_names"}
                    ]
                )
            },
            COALESCE(
                ARRAY_REMOVE(recipient_names.all_names, COALESCE(temp_stale_recipients_removed_view.legal_business_name, '')),
                ARRAY()
            ) AS alternate_names
        FROM temp_stale_recipients_removed_view
        LEFT OUTER JOIN recipient_names ON (
            temp_stale_recipients_removed_view.recipient_hash = recipient_names.recipient_hash
        )
    )
    """,
    # -----
    # Insert the temporary_restock_recipient_lookup table into recipient_lookup
    # -----
    rf"""
    INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}}
    (
        {",".join([col for col in RECIPIENT_LOOKUP_COLUMNS_WITHOUT_ID])}
    )
    SELECT
        {",".join([col for col in RECIPIENT_LOOKUP_COLUMNS_WITHOUT_ID])}
    FROM
        temp_recipients_with_alt_names_view
    """,
    # -----
    # Cleanup the temporary views;
    # this is mostly for test cases as the views are session based
    # -----
    "DROP VIEW temp_recipient_filter",
    "DROP VIEW temp_transaction_recipients_view",
    "DROP VIEW temp_collect_recipients_view",
    "DROP VIEW temp_stale_recipients_removed_view",
    "DROP VIEW temp_recipients_with_alt_names_view",
]
