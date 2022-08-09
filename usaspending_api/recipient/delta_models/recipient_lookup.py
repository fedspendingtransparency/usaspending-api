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
    "id": {"delta": "LONG NOT NULL", "postgres": "BIGINT NOT NULL"},
    **RECIPIENT_LOOKUP_COLUMNS_WITHOUT_ID,
}

RECIPIENT_LOOKUP_DELTA_COLUMNS = {k: v["delta"] for k, v in RECIPIENT_LOOKUP_COLUMNS.items()}
RECIPIENT_LOOKUP_POSTGRES_COLUMNS = {k: v["postgres"] for k, v in RECIPIENT_LOOKUP_COLUMNS.items()}

TEMP_RECIPIENT_LOOKUP_COLUMNS = {
    "duns_recipient_hash": "STRING",
    "row_num_union": "INTEGER",
    **{k: v["delta"] for k, v in RECIPIENT_LOOKUP_COLUMNS_WITHOUT_ID.items()},
}

recipient_lookup_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in RECIPIENT_LOOKUP_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """

recipient_lookup_load_sql_string = [
    # -----
    # Creation of a temporary view used to reference recipient information from transactions
    # -----
    r"""
    CREATE OR REPLACE TEMPORARY VIEW temporary_transaction_recipients_view AS (
        SELECT
            tn.transaction_unique_id,
            tn.is_fpds,
            REGEXP_REPLACE(
                MD5(UPPER(
                    CASE
                        WHEN COALESCE(fpds.awardee_or_recipient_uei, fabs.uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(fpds.awardee_or_recipient_uei, fabs.uei))
                        WHEN COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu))
                        ELSE CONCAT('name-', COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal)) END
                )),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS recipient_hash,
            REGEXP_REPLACE(
                MD5(UPPER(
                    CASE
                        WHEN COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu))
                        ELSE CONCAT('name-', COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal)) END
                )),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS duns_recipient_hash,
            REGEXP_REPLACE(
                MD5(UPPER(
                    CASE
                        WHEN COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei))
                        WHEN COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide) IS NOT  NULL THEN CONCAT('duns-', COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide))
                        ELSE CONCAT('name-', COALESCE(fpds.ultimate_parent_legal_enti, fabs.ultimate_parent_legal_enti)) END
                )),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS parent_recipient_hash,
            REGEXP_REPLACE(
                MD5(UPPER(
                    CASE
                        WHEN COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide))
                        ELSE CONCAT('name-', COALESCE(fpds.ultimate_parent_legal_enti, fabs.ultimate_parent_legal_enti)) END
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
        FROM raw.transaction_normalized AS tn
        LEFT OUTER JOIN raw.transaction_fpds AS fpds ON (tn.id = fpds.transaction_id)
        LEFT OUTER JOIN raw.transaction_fabs AS fabs ON (tn.id = fabs.transaction_id)
        WHERE tn.action_date >= '2007-10-01'
        ORDER BY tn.action_date DESC
    )
    """,
    # -----
    # Creation of the temporary table that is used to stage and merge updates to recipient_lookup
    # -----
    rf"""
    CREATE OR REPLACE TABLE temp.temporary_restock_recipient_lookup (
        {", ".join([f'{key} {val}' for key, val in TEMP_RECIPIENT_LOOKUP_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/temp/temporary_restock_recipient_lookup'
    """,
    # -----
    # Populate the temporary_restock_recipient_lookup table
    # -----
    r"""
    WITH latest_duns_sam AS (
        SELECT
            1 AS priority,
            REGEXP_REPLACE(
                MD5(UPPER(
                    CASE WHEN uei IS NOT NULL THEN CONCAT('uei-', uei)
                    ELSE CONCAT('duns-', awardee_or_recipient_uniqu) END
                )),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS recipient_hash,
            REGEXP_REPLACE(
                MD5(UPPER(CONCAT('duns-', awardee_or_recipient_uniqu))),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS duns_recipient_hash,
            UPPER(legal_business_name) AS legal_business_name,
            awardee_or_recipient_uniqu AS duns,
            uei,
            'sam' AS source,
            address_line_1,
            address_line_2,
            business_types_codes,
            city,
            congressional_district,
            country_code,
            ultimate_parent_unique_ide AS parent_duns,
            UPPER(ultimate_parent_legal_enti) AS parent_legal_business_name,
            ultimate_parent_uei AS parent_uei,
            state,
            zip4,
            zip AS zip5,
            update_date,
            NULL AS alternate_names,
            ROW_NUMBER() OVER (PARTITION BY uei, awardee_or_recipient_uniqu ORDER BY update_date DESC NULLS LAST) AS row_num
        FROM raw.sam_recipient
        WHERE COALESCE(uei, awardee_or_recipient_uniqu) IS NOT NULL AND legal_business_name IS NOT NULL
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
            NULL AS alternate_names,
            ROW_NUMBER() OVER (PARTITION BY recipient_hash ORDER BY action_date DESC, is_fpds, transaction_unique_id) AS row_num
        FROM temporary_transaction_recipients_view
        WHERE COALESCE(uei, awardee_or_recipient_uniqu) IS NOT NULL AND awardee_or_recipient_legal IS NOT NULL
    ),
    latest_duns_sam_parent AS (
        SELECT
            3 AS priority,
            REGEXP_REPLACE(
                MD5(UPPER(
                    CASE WHEN ultimate_parent_uei IS NOT NULL THEN CONCAT('uei-', ultimate_parent_uei)
                    ELSE CONCAT('duns-', ultimate_parent_unique_ide) END
                )),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS recipient_hash,
            REGEXP_REPLACE(
                MD5(UPPER(CONCAT('duns-', ultimate_parent_unique_ide))),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS duns_recipient_hash,
            UPPER(ultimate_parent_legal_enti) AS legal_business_name,
            ultimate_parent_unique_ide AS duns,
            ultimate_parent_uei AS uei,
            'sam-parent' AS source,
            NULL AS address_line_1,
            NULL AS address_line_2,
            NULL AS business_types_codes,
            NULL AS city,
            NULL AS congressional_district,
            NULL AS country_code,
            ultimate_parent_unique_ide AS parent_duns,
            UPPER(ultimate_parent_legal_enti) AS parent_legal_business_name,
            ultimate_parent_uei AS parent_uei,
            NULL AS state,
            NULL AS zip4,
            NULL AS zip5,
            update_date,
            NULL AS alternate_names,
            ROW_NUMBER() OVER (PARTITION BY ultimate_parent_uei, ultimate_parent_unique_ide ORDER BY update_date DESC NULLS LAST) AS row_num
        FROM raw.sam_recipient
        WHERE COALESCE(ultimate_parent_uei, ultimate_parent_unique_ide) IS NOT NULL AND ultimate_parent_legal_enti IS NOT NULL
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
            NULL AS alternate_names,
            ROW_NUMBER() OVER (PARTITION BY parent_recipient_hash ORDER BY action_date DESC, is_fpds, transaction_unique_id) AS row_num
        FROM temporary_transaction_recipients_view
        WHERE COALESCE(ultimate_parent_uei, ultimate_parent_unique_ide) IS NOT NULL AND ultimate_parent_legal_enti IS NOT NULL
    ),
    latest_duns_sam_no_name AS (
        SELECT
            5 AS priority,
            REGEXP_REPLACE(
                MD5(UPPER(
                    CASE WHEN uei IS NOT NULL THEN CONCAT('uei-', uei)
                    ELSE CONCAT('duns-', awardee_or_recipient_uniqu) END
                )),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS recipient_hash,
            REGEXP_REPLACE(
                MD5(UPPER(CONCAT('duns-', awardee_or_recipient_uniqu))),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS duns_recipient_hash,
            UPPER(legal_business_name) AS legal_business_name,
            awardee_or_recipient_uniqu AS duns,
            uei,
            'sam' AS source,
            address_line_1,
            address_line_2,
            business_types_codes,
            city,
            congressional_district,
            country_code,
            ultimate_parent_unique_ide AS parent_duns,
            UPPER(ultimate_parent_legal_enti) AS parent_legal_business_name,
            ultimate_parent_uei AS parent_uei,
            state,
            zip4,
            zip AS zip5,
            update_date,
            NULL AS alternate_names,
            ROW_NUMBER() OVER (PARTITION BY uei, awardee_or_recipient_uniqu ORDER BY update_date DESC NULLS LAST) AS row_num
        FROM raw.sam_recipient
        WHERE COALESCE(uei, awardee_or_recipient_uniqu) IS NOT NULL AND legal_business_name IS NULL
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
            NULL AS alternate_names,
            ROW_NUMBER() OVER (PARTITION BY recipient_hash ORDER BY action_date DESC, is_fpds, transaction_unique_id) AS row_num
        FROM temporary_transaction_recipients_view
        WHERE COALESCE(uei, awardee_or_recipient_uniqu) IS NOT NULL AND awardee_or_recipient_legal IS NULL
    ),
    latest_duns_sam_parent_no_name AS (
        SELECT
            7 AS priority,
            REGEXP_REPLACE(
                MD5(UPPER(
                    CASE WHEN ultimate_parent_uei IS NOT NULL THEN CONCAT('uei-', ultimate_parent_uei)
                    ELSE CONCAT('duns-', ultimate_parent_unique_ide) END
                )),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS recipient_hash,
            REGEXP_REPLACE(
                MD5(UPPER(CONCAT('duns-', ultimate_parent_unique_ide))),
                '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$',
                '\$1-\$2-\$3-\$4-\$5'
            ) AS duns_recipient_hash,
            NULL AS legal_business_name,
            ultimate_parent_unique_ide AS duns,
            ultimate_parent_uei AS uei,
            'sam-parent' AS source,
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
            update_date,
            NULL AS alternate_names,
            ROW_NUMBER() OVER (PARTITION BY ultimate_parent_uei, ultimate_parent_unique_ide ORDER BY update_date DESC NULLS LAST) AS row_num
        FROM raw.sam_recipient
        WHERE COALESCE(ultimate_parent_uei, ultimate_parent_unique_ide) IS NOT NULL AND ultimate_parent_legal_enti IS NULL
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
            NULL AS alternate_names,
            ROW_NUMBER() OVER (PARTITION BY parent_recipient_hash ORDER BY action_date DESC, is_fpds, transaction_unique_id) AS row_num
        FROM temporary_transaction_recipients_view
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
            NULL AS alternate_names,
            ROW_NUMBER() OVER (PARTITION BY recipient_hash ORDER BY action_date DESC, is_fpds, transaction_unique_id) AS row_num
        FROM temporary_transaction_recipients_view
        WHERE COALESCE(uei, awardee_or_recipient_uniqu) IS NULL
    ),
    union_all AS (
        SELECT * FROM latest_duns_sam WHERE row_num = 1
        UNION ALL
        SELECT * FROM latest_tx WHERE row_num = 1
        UNION ALL
        SELECT * FROM latest_duns_sam_parent WHERE row_num = 1
        UNION ALL
        SELECT * FROM latest_tx_parent WHERE row_num = 1
        UNION ALL
        SELECT * FROM latest_duns_sam_no_name WHERE row_num = 1
        UNION ALL
        SELECT * FROM latest_tx_no_name WHERE row_num = 1
        UNION ALL
        SELECT * FROM latest_duns_sam_parent_no_name WHERE row_num = 1
        UNION ALL
        SELECT * FROM latest_tx_parent_no_name WHERE row_num = 1
        UNION ALL
        SELECT * FROM latest_tx_no_fabs_fpds WHERE row_num = 1
    ),
    union_all_priority AS (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY recipient_hash ORDER BY priority ASC) AS row_num_union
        FROM union_all
    )
    INSERT INTO temp.temporary_restock_recipient_lookup (
        recipient_hash,
        duns_recipient_hash,
        legal_business_name,
        duns,
        uei,
        address_line_1,
        address_line_2,
        business_types_codes,
        city,
        congressional_district,
        country_code,
        parent_duns,
        parent_legal_business_name,
        parent_uei,
        state,
        zip4,
        zip5,
        alternate_names,
        source,
        update_date,
        row_num_union
    )
    SELECT
        recipient_hash,
        duns_recipient_hash,
        legal_business_name,
        duns,
        uei,
        address_line_1,
        address_line_2,
        business_types_codes,
        city,
        congressional_district,
        country_code,
        parent_duns,
        parent_legal_business_name,
        parent_uei,
        state,
        zip4,
        zip5,
        alternate_names,
        source,
        update_date,
        row_num_union
    FROM union_all_priority
    """,
    # -----
    # Update the temporary_restock_recipient_lookup table to include any alternate names
    # -----
    r"""
    MERGE INTO temp.temporary_restock_recipient_lookup temp_rl
    USING (
        WITH alt_names AS (
            SELECT
                recipient_hash,
                COLLECT_SET(awardee_or_recipient_legal) AS all_names
            FROM temporary_transaction_recipients_view
            WHERE COALESCE(awardee_or_recipient_legal, '') != ''
            GROUP BY recipient_hash
        ),
        alt_parent_names AS (
            SELECT
                parent_recipient_hash AS recipient_hash,
                COLLECT_SET(ultimate_parent_legal_enti) AS all_names
            FROM temporary_transaction_recipients_view
            WHERE COALESCE(ultimate_parent_legal_enti, '') != ''
            GROUP BY parent_recipient_hash
        )
        SELECT
            COALESCE(an.recipient_hash, apn.recipient_hash) AS recipient_hash,
            COALESCE(
                ARRAY_SORT(ARRAY_UNION(COALESCE(an.all_names, ARRAY()), COALESCE(apn.all_names, ARRAY()))),
                ARRAY()
             )AS all_names
        FROM alt_names AS an
        FULL OUTER JOIN alt_parent_names AS apn ON an.recipient_hash = apn.recipient_hash
    ) AS alt_names
    ON temp_rl.recipient_hash = alt_names.recipient_hash AND row_num_union = 1
    WHEN MATCHED
    AND temp_rl.alternate_names IS DISTINCT FROM ARRAY_REMOVE(alt_names.all_names, COALESCE(temp_rl.legal_business_name, ''))
    THEN UPDATE SET temp_rl.alternate_names = COALESCE(ARRAY_REMOVE(alt_names.all_names, COALESCE(temp_rl.legal_business_name, '')), ARRAY())
    """,
    # -----
    # Delete any instances of a NULL recipient_hash
    # -----
    r"""
    DELETE FROM temp.temporary_restock_recipient_lookup
    WHERE recipient_hash IS NULL OR row_num_union != 1
    """,
    # -----
    # Merge the temporary_restock_recipient_lookup table into recipient_lookup
    # -----
    r"""
    MERGE INTO {DESTINATION_DATABASE}.{DESTINATION_TABLE} AS rl
    USING temp.temporary_restock_recipient_lookup AS temp_rl
    ON rl.recipient_hash = temp_rl.recipient_hash
    WHEN MATCHED
    AND (
        temp_rl.address_line_1                 IS DISTINCT FROM rl.address_line_1
        OR temp_rl.address_line_2              IS DISTINCT FROM rl.address_line_2
        OR temp_rl.business_types_codes        IS DISTINCT FROM rl.business_types_codes
        OR temp_rl.city                        IS DISTINCT FROM rl.city
        OR temp_rl.congressional_district      IS DISTINCT FROM rl.congressional_district
        OR temp_rl.country_code                IS DISTINCT FROM rl.country_code
        OR temp_rl.duns                        IS DISTINCT FROM rl.duns
        OR temp_rl.uei                         IS DISTINCT FROM rl.uei
        OR temp_rl.legal_business_name         IS DISTINCT FROM rl.legal_business_name
        OR temp_rl.parent_duns                 IS DISTINCT FROM rl.parent_duns
        OR temp_rl.parent_legal_business_name  IS DISTINCT FROM rl.parent_legal_business_name
        OR temp_rl.parent_uei                  IS DISTINCT FROM rl.parent_uei
        OR temp_rl.recipient_hash              IS DISTINCT FROM rl.recipient_hash
        OR temp_rl.source                      IS DISTINCT FROM rl.source
        OR temp_rl.state                       IS DISTINCT FROM rl.state
        OR temp_rl.zip4                        IS DISTINCT FROM rl.zip4
        OR temp_rl.zip5                        IS DISTINCT FROM rl.zip5
        OR ARRAY_SORT(ARRAY_REMOVE(temp_rl.alternate_names, COALESCE(rl.legal_business_name, '')))
                                         IS DISTINCT FROM rl.alternate_names
    )
    THEN UPDATE SET
        rl.legal_business_name = temp_rl.legal_business_name,
        rl.duns = temp_rl.duns,
        rl.uei = temp_rl.uei,
        rl.address_line_1 = temp_rl.address_line_1,
        rl.address_line_2 = temp_rl.address_line_2,
        rl.business_types_codes = temp_rl.business_types_codes,
        rl.city = temp_rl.city,
        rl.congressional_district = temp_rl.congressional_district,
        rl.country_code = temp_rl.country_code,
        rl.parent_duns = temp_rl.parent_duns,
        rl.parent_legal_business_name = temp_rl.parent_legal_business_name,
        rl.parent_uei = temp_rl.parent_uei,
        rl.state = temp_rl.state,
        rl.zip4 = temp_rl.zip4,
        rl.zip5 = temp_rl.zip5,
        rl.alternate_names = temp_rl.alternate_names,
        rl.source = temp_rl.source,
        rl.update_date = NOW()
    WHEN NOT MATCHED THEN INSERT (
        id,
        recipient_hash,
        legal_business_name,
        duns,
        uei,
        address_line_1,
        address_line_2,
        business_types_codes,
        city,
        congressional_district,
        country_code,
        parent_duns,
        parent_legal_business_name,
        parent_uei,
        state,
        zip4,
        zip5,
        alternate_names,
        source,
        update_date
    )
    VALUES (
        0,
        recipient_hash,
        legal_business_name,
        duns,
        uei,
        address_line_1,
        address_line_2,
        business_types_codes,
        city,
        congressional_district,
        country_code,
        parent_duns,
        parent_legal_business_name,
        parent_uei,
        state,
        zip4,
        zip5,
        alternate_names,
        source,
        NOW()
    )
    """,
    # -----
    # Delete any cases of old recipients from recipient_lookup where the recipient now has a UEI
    # -----
    r"""
    MERGE INTO {DESTINATION_DATABASE}.{DESTINATION_TABLE} AS rl
    USING (
        SELECT duns_recipient_hash
        FROM temp.temporary_restock_recipient_lookup
        WHERE
            uei IS NOT NULL
            AND duns IS NOT NULL
    ) AS temp_rl
    ON rl.recipient_hash = temp_rl.duns_recipient_hash AND rl.uei IS NULL
    WHEN MATCHED
    THEN DELETE
    """,
    # -----
    # Populate recipient_lookup with incremented Primary Key fields
    # -----
    r"""
    CREATE OR REPLACE TEMPORARY VIEW temp_recipient_lookup_view AS (
        SELECT
            {AUTO_INCREMENT_MAX_ID} + ROW_NUMBER() OVER (ORDER BY recipient_hash) AS new_id,
            rl.*
        FROM {DESTINATION_DATABASE}.{DESTINATION_TABLE} AS rl
        WHERE rl.id = 0
    )
    """,
    r"""
    MERGE INTO {DESTINATION_DATABASE}.{DESTINATION_TABLE} AS rl
    USING temp_recipient_lookup_view temp_rl
    ON rl.recipient_hash = temp_rl.recipient_hash
    WHEN MATCHED
    THEN UPDATE SET
        rl.id = temp_rl.new_id
    """,
    # -----
    # Cleanup the temporary table
    # -----
    r"""
    DELETE FROM temp.temporary_restock_recipient_lookup
    """,
    r"""
    DROP TABLE temp.temporary_restock_recipient_lookup
    """,
    r"""
    DROP VIEW temp_recipient_lookup_view
    """,
    r"""
    DROP VIEW temporary_transaction_recipients_view
    """,
]
