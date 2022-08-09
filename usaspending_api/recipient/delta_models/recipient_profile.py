RECIPIENT_PROFILE_COLUMNS = {
    "recipient_level": {"delta": "STRING NOT NULL", "postgres": "TEXT NOT NULL"},
    "recipient_hash": {"delta": "STRING", "postgres": "UUID"},
    "recipient_unique_id": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_name": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_affiliations": {"delta": "ARRAY<STRING> NOT NULL", "postgres": "TEXT[]"},
    "last_12_months": {"delta": "numeric(23,2) NOT NULL", "postgres": "numeric(23,2"},
    "id": {"delta": "LONG NOT NULL", "postgres": "bigint"},
    "last_12_contracts": {"delta": "numeric(23,2) NOT NULL", "postgres": "numeric(23,2"},
    "last_12_direct_payments": {"delta": "numeric(23,2) NOT NULL", "postgres": "numeric(23,2"},
    "last_12_grants": {"delta": "numeric(23,2) NOT NULL", "postgres": "numeric(23,2"},
    "last_12_loans": {"delta": "numeric(23,2) NOT NULL", "postgres": "numeric(23,2"},
    "last_12_months_count": {"delta": "INTEGER NOT NULL", "postgres": "numeric(23,2"},
    "last_12_other": {"delta": "numeric(23,2) NOT NULL", "postgres": "numeric(23,2"},
    "award_types": {"delta": "ARRAY<STRING> NOT NULL", "postgres": "TEXT[]"},
    "uei": {"delta": "STRING", "postgres": "text"},
    "parent_uei": {"delta": "STRING", "postgres": "text"},
}
RECIPIENT_PROFILE_DELTA_COLUMNS = {k: v["delta"] for k, v in RECIPIENT_PROFILE_COLUMNS.items()}
RECIPIENT_PROFILE_POSTGRES_COLUMNS = {k: v["postgres"] for k, v in RECIPIENT_PROFILE_COLUMNS.items()}
recipient_profile_create_sql_string = f"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in RECIPIENT_PROFILE_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """
recipient_profile_load_sql_string = [
    #   --------------------------------------------------------------------------------
    #     -- Step 1, create the temporary matview of recipients from transactions
    #   --------------------------------------------------------------------------------
    r"""
    CREATE OR REPLACE TEMPORARY VIEW temporary_recipients_from_transactions_view AS (
    SELECT
        REGEXP_REPLACE(MD5(UPPER(
        CASE
            WHEN COALESCE(fpds.awardee_or_recipient_uei, fabs.uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(fpds.awardee_or_recipient_uei, fabs.uei))
            WHEN COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu))
            ELSE CONCAT('name-', COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal, '')) END
        )), '^(\.{{8}})(\.{{4}})(\.{{4}})(\.{{4}})(\.{{12}})$', '\$1-\$2-\$3-\$4-\$5') AS recipient_hash,
        COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) AS recipient_unique_id,
        COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide) AS parent_recipient_unique_id,
        COALESCE(fpds.awardee_or_recipient_uei, fabs.uei) AS uei,
        COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei) AS parent_uei,
        CASE
            WHEN tn.type IN ('A', 'B', 'C', 'D')      THEN 'contract'
            WHEN tn.type IN ('02', '03', '04', '05')  THEN 'grant'
            WHEN tn.type IN ('06', '10')              THEN 'direct payment'
            WHEN tn.type IN ('07', '08')              THEN 'loans'
            WHEN tn.type IN ('09', '11')              THEN 'other'     -- collapsing insurance into other
            WHEN tn.type LIKE 'IDV%'                  THEN 'contract'  -- collapsing idv into contract
            ELSE NULL
        END AS award_category,
        CASE
            WHEN COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei) IS NOT NULL THEN 'C'
            ELSE 'R' END AS recipient_level,
        tn.action_date,
        CAST(COALESCE(CASE
            WHEN tn.type IN('07','08') THEN tn.original_loan_subsidy_cost
            ELSE tn.federal_action_obligation
        END, 0) AS NUMERIC(23, 2)) AS generated_pragmatic_obligation
    FROM
        raw.transaction_normalized as tn
    LEFT OUTER JOIN raw.transaction_fpds as fpds ON tn.id = fpds.transaction_id
    LEFT OUTER JOIN raw.transaction_fabs as fabs ON tn.id = fabs.transaction_id
    WHERE
        tn.action_date >= '2007-10-01'
        AND tn.type IS NOT NULL
    );""",
    #
    # --------------------------------------------------------------------------------
    # -- Step 2, Create the new table and populate with 100% of combinations
    # --------------------------------------------------------------------------------
    f"""CREATE OR REPLACE TABLE temporary_restock_recipient_profile (
        id BIGINT,
        recipient_level character(1) NOT NULL,
        recipient_hash STRING,
        recipient_unique_id STRING,
        uei STRING,
        parent_uei STRING,
        recipient_name STRING,
        unused BOOLEAN,
        recipient_affiliations ARRAY<STRING>,
        award_types ARRAY<STRING>,
        last_12_months NUMERIC(23,2),
        last_12_contracts NUMERIC(23,2),
        last_12_grants NUMERIC(23,2),
        last_12_direct_payments NUMERIC(23,2),
        last_12_loans NUMERIC(23,2),
        last_12_other NUMERIC(23,2),
        last_12_months_count INT
    ) USING DELTA;
    """,
    """INSERT INTO temporary_restock_recipient_profile (
    SELECT
        monotonically_increasing_id() AS id,
        'P' as recipient_level,
        recipient_hash AS recipient_hash,
        duns AS recipient_unique_id,
        uei AS uei,
        parent_uei AS parent_uei,
        legal_business_name AS recipient_name,
        true AS unused,
        ARRAY() AS recipient_affiliations,
        ARRAY() AS award_types,
        0.00 AS last_12_months,
        0.00 AS last_12_contracts,
        0.00 AS last_12_grants,
        0.00 AS last_12_direct_payments,
        0.00 AS last_12_loans,
        0.00 AS last_12_other,
        0 AS last_12_months_count
    FROM
    raw.recipient_lookup
    UNION ALL
        SELECT
            monotonically_increasing_id() AS id,
            'C' as recipient_level,
            recipient_hash AS recipient_hash,
            duns AS recipient_unique_id,
            uei AS uei,
            parent_uei AS parent_uei,
            legal_business_name AS recipient_name,
            true AS unused,
            ARRAY() AS recipient_affiliations,
            ARRAY() AS award_types,
            0.00 AS last_12_months,
            0.00 AS last_12_contracts,
            0.00 AS last_12_grants,
            0.00 AS last_12_direct_payments,
            0.00 AS last_12_loans,
            0.00 AS last_12_other,
            0 AS last_12_months_count
        FROM
        raw.recipient_lookup
    UNION ALL
        SELECT
            monotonically_increasing_id() AS id,
            'R' as recipient_level,
            recipient_hash AS recipient_hash,
            duns AS recipient_unique_id,
            uei AS uei,
            parent_uei AS parent_uei,
            legal_business_name AS recipient_name,
            true AS unused,
            ARRAY() AS recipient_affiliations,
            ARRAY() AS award_types,
            0.00 AS last_12_months,
            0.00 AS last_12_contracts,
            0.00 AS last_12_grants,
            0.00 AS last_12_direct_payments,
            0.00 AS last_12_loans,
            0.00 AS last_12_other,
            0 AS last_12_months_count
        FROM
        raw.recipient_lookup);""",
    # --------------------------------------------------------------------------------
    # -- Step 3, Obligation for past 12 months
    # --------------------------------------------------------------------------------
    """WITH grouped_by_category AS (
        WITH grouped_by_category_inner AS (
            SELECT
                recipient_hash,
                recipient_level,
                CASE
                    WHEN award_category NOT IN ('contract', 'grant', 'direct payment', 'loans')
                    THEN 'other' ELSE award_category
                END AS award_category,
                CAST(CASE WHEN award_category = 'contract' THEN SUM(generated_pragmatic_obligation) ELSE 0 END AS NUMERIC(23,2)) AS inner_contracts,
                CAST(CASE WHEN award_category = 'grant' THEN SUM(generated_pragmatic_obligation) ELSE 0 END AS NUMERIC(23,2)) AS inner_grants,
                CAST(CASE WHEN award_category = 'direct payment' THEN SUM(generated_pragmatic_obligation) ELSE 0 END AS NUMERIC(23,2)) AS inner_direct_payments,
                CAST(CASE WHEN award_category = 'loans' THEN SUM(generated_pragmatic_obligation) ELSE 0 END AS NUMERIC(23,2)) AS inner_loans,
                CAST(CASE WHEN award_category NOT IN ('contract', 'grant', 'direct payment', 'loans') THEN SUM(generated_pragmatic_obligation) ELSE 0 END AS NUMERIC(23,2)) AS inner_other,
                SUM(generated_pragmatic_obligation) AS inner_amount,
                COUNT(*) AS inner_count
            FROM
                temporary_recipients_from_transactions_view AS trft
            WHERE
                trft.action_date >= now() - INTERVAL '1 year'
            GROUP BY recipient_hash, recipient_level, award_category
        )
        SELECT
            recipient_hash,
            recipient_level,
            SORT_ARRAY(COLLECT_SET(award_category)) AS award_types,
            SUM(inner_contracts) AS last_12_contracts,
            SUM(inner_grants) AS last_12_grants,
            SUM(inner_direct_payments) AS last_12_direct_payments,
            SUM(inner_loans) AS last_12_loans,
            SUM(inner_other) AS last_12_other,
            SUM(inner_amount) AS amount,
            SUM(inner_count) AS count
        FROM
            grouped_by_category_inner AS gbci
        GROUP BY recipient_hash, recipient_level
    )
    MERGE INTO temporary_restock_recipient_profile AS rpv
    USING grouped_by_category AS gbc
    ON gbc.recipient_hash = rpv.recipient_hash AND
        gbc.recipient_level = rpv.recipient_level
    WHEN MATCHED THEN UPDATE SET
        award_types = gbc.award_types || rpv.award_types,
        last_12_months = rpv.last_12_months + gbc.amount,
        last_12_contracts = rpv.last_12_contracts + gbc.last_12_contracts,
        last_12_grants = rpv.last_12_grants + gbc.last_12_grants,
        last_12_direct_payments = rpv.last_12_direct_payments + gbc.last_12_direct_payments,
        last_12_loans = rpv.last_12_loans + gbc.last_12_loans,
        last_12_other = rpv.last_12_other + gbc.last_12_other,
        last_12_months_count = rpv.last_12_months_count + gbc.count,
        unused = false;""",
    # --------------------------------------------------------------------------------
    # -- Step 4, Populate the Parent Obligation for past 12 months
    # --------------------------------------------------------------------------------
    """WITH grouped_by_parent AS (
        WITH grouped_by_parent_inner AS (
            SELECT
                parent_uei,
                CASE
                    WHEN award_category NOT IN ('contract', 'grant', 'direct payment', 'loans')
                    THEN 'other' ELSE award_category
                END AS award_category,
                CAST(CASE WHEN award_category = 'contract' THEN SUM(generated_pragmatic_obligation) ELSE 0 END AS NUMERIC(23,2)) AS inner_contracts,
                CAST(CASE WHEN award_category = 'grant' THEN SUM(generated_pragmatic_obligation) ELSE 0 END AS NUMERIC(23,2)) AS inner_grants,
                CAST(CASE WHEN award_category = 'direct payment' THEN SUM(generated_pragmatic_obligation) ELSE 0 END AS NUMERIC(23,2)) AS inner_direct_payments,
                CAST(CASE WHEN award_category = 'loans' THEN SUM(generated_pragmatic_obligation) ELSE 0 END AS NUMERIC(23,2)) AS inner_loans,
                CAST(CASE WHEN award_category NOT IN ('contract', 'grant', 'direct payment', 'loans') THEN SUM(generated_pragmatic_obligation) ELSE 0 END AS NUMERIC(23,2)) AS inner_other,
                SUM(generated_pragmatic_obligation) AS inner_amount,
                COUNT(*) AS inner_count
            FROM
                temporary_recipients_from_transactions_view AS trft
            WHERE
                trft.action_date >= now() - INTERVAL '1 year' AND
                parent_uei IS NOT NULL
            GROUP BY parent_uei, award_category
        )
        SELECT
            parent_uei AS uei,
            COLLECT_SET(award_category) AS award_types,
            SUM(inner_contracts) AS last_12_contracts,
            SUM(inner_grants) AS last_12_grants,
            SUM(inner_direct_payments) AS last_12_direct_payments,
            SUM(inner_loans) AS last_12_loans,
            SUM(inner_other) AS last_12_other,
            SUM(inner_amount) AS amount,
            SUM(inner_count) AS count
        FROM
            grouped_by_parent_inner AS gbpi
        GROUP BY parent_uei
    )
    MERGE INTO temporary_restock_recipient_profile AS rpv USING  grouped_by_parent AS gbp
    ON rpv.uei = gbp.uei AND
        rpv.recipient_level = 'P'
    WHEN MATCHED THEN UPDATE SET
        award_types = gbp.award_types || rpv.award_types,
        last_12_months = rpv.last_12_months + gbp.amount,
        last_12_contracts = rpv.last_12_contracts + gbp.last_12_contracts,
        last_12_grants = rpv.last_12_grants + gbp.last_12_grants,
        last_12_direct_payments = rpv.last_12_direct_payments + gbp.last_12_direct_payments,
        last_12_loans = rpv.last_12_loans + gbp.last_12_loans,
        last_12_other = rpv.last_12_other + gbp.last_12_other,
        last_12_months_count = rpv.last_12_months_count + gbp.count,
        unused = false;""",
    # --------------------------------------------------------------------------------
    # -- Step 5, Populating child recipient list in parents
    # --------------------------------------------------------------------------------
    """WITH parent_recipients AS (
        SELECT
            parent_uei,
            COLLECT_SET(DISTINCT uei) AS uei_list
        FROM
            temporary_recipients_from_transactions_view
        WHERE
            parent_uei IS NOT NULL
        GROUP BY
            parent_uei
    )
    MERGE INTO temporary_restock_recipient_profile AS rpv
    USING parent_recipients AS pr
    ON rpv.uei = pr.parent_uei and rpv.recipient_level = 'P'
    WHEN MATCHED THEN UPDATE SET
        recipient_affiliations = pr.uei_list,
        unused = false;""",
    # --------------------------------------------------------------------------------
    # -- Step 6, Populate parent recipient list in children
    # --------------------------------------------------------------------------------
    """WITH all_recipients AS (
        SELECT
            uei,
            COLLECT_SET(DISTINCT parent_uei) AS parent_uei_list
        FROM
            temporary_recipients_from_transactions_view
        WHERE
            uei IS NOT NULL AND
            parent_uei IS NOT NULL
        GROUP BY uei
    )
    MERGE INTO temporary_restock_recipient_profile AS rpv
    USING all_recipients AS ar
    ON rpv.uei = ar.uei AND
        rpv.recipient_level = 'C'
    WHEN MATCHED THEN UPDATE SET
        recipient_affiliations = ar.parent_uei_list,
        unused = false;""",
    # --------------------------------------------------------------------------------
    # -- Step 7, Mark recipient profile rows older than 12 months  as valid
    # --------------------------------------------------------------------------------
    """WITH grouped_by_old_recipients AS (
        SELECT
            recipient_hash,
            recipient_level
        FROM
            temporary_recipients_from_transactions_view AS trft
        GROUP BY recipient_hash, recipient_level
    )

    MERGE INTO temporary_restock_recipient_profile AS rpv
    USING grouped_by_old_recipients AS gbc
    ON gbc.recipient_hash = rpv.recipient_hash AND
        gbc.recipient_level = rpv.recipient_level AND
        rpv.unused = true
    WHEN MATCHED THEN UPDATE SET
        unused = false;""",
    # --------------------------------------------------------------------------------
    # -- Step 8, Mark Parent recipient profile rows older than 12 months  as valid
    # --------------------------------------------------------------------------------
    """WITH grouped_by_parent_old AS (
        SELECT
            parent_uei
        FROM
            temporary_recipients_from_transactions_view AS trft
        WHERE
            parent_uei IS NOT NULL
        GROUP BY parent_uei
    )
    MERGE INTO temporary_restock_recipient_profile AS rpv
    USING grouped_by_parent_old AS gbp
    ON rpv.uei = gbp.parent_uei AND
        rpv.recipient_level = 'P' AND
        rpv.unused = true
    WHEN MATCHED THEN UPDATE SET
        unused = false;""",
    # --------------------------------------------------------------------------------
    # -- Step 9, Finalize new table
    # --------------------------------------------------------------------------------
    """DELETE FROM temporary_restock_recipient_profile WHERE unused = true;""",
    # --------------------------------------------------------------------------------
    # -- Step 10, Drop unnecessary relations and standup new table as final
    # --------------------------------------------------------------------------------
    f"""
        WITH CTE AS (
            SELECT recipient_hash, recipient_level from {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}} rp WHERE NOT EXISTS (
                SELECT recipient_hash, recipient_level FROM temporary_restock_recipient_profile temp_p
                WHERE rp.recipient_hash = temp_p.recipient_hash
                AND rp.recipient_level = temp_p.recipient_level
            )
        )
        MERGE INTO {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}} rp
        USING CTE ON CTE.recipient_hash = rp.recipient_hash AND CTE.recipient_level = rp.recipient_level
        WHEN MATCHED THEN DELETE;
    """,
    f"""
        MERGE INTO {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}} rp
        USING temporary_restock_recipient_profile temp_p
        ON rp.recipient_hash = temp_p.recipient_hash
            AND rp.recipient_level = temp_p.recipient_level
            AND (
                rp.recipient_unique_id IS DISTINCT FROM temp_p.recipient_unique_id
                OR rp.recipient_name IS DISTINCT FROM temp_p.recipient_name
                OR SORT_ARRAY(rp.recipient_affiliations) IS DISTINCT FROM SORT_ARRAY(temp_p.recipient_affiliations)
                OR SORT_ARRAY(rp.award_types) IS DISTINCT FROM SORT_ARRAY(temp_p.award_types)
                OR rp.last_12_months IS DISTINCT FROM temp_p.last_12_months
                OR rp.last_12_contracts IS DISTINCT FROM temp_p.last_12_contracts
                OR rp.last_12_loans IS DISTINCT FROM temp_p.last_12_loans
                OR rp.last_12_grants IS DISTINCT FROM temp_p.last_12_grants
                OR rp.last_12_direct_payments IS DISTINCT FROM temp_p.last_12_direct_payments
                OR rp.last_12_other IS DISTINCT FROM temp_p.last_12_other
                OR rp.last_12_months_count IS DISTINCT FROM temp_p.last_12_months_count
                OR rp.uei IS DISTINCT FROM temp_p.uei
                OR rp.parent_uei IS DISTINCT FROM temp_p.parent_uei
            )
        WHEN MATCHED THEN UPDATE SET
            recipient_unique_id = temp_p.recipient_unique_id,
            uei = temp_p.uei,
            recipient_name = temp_p.recipient_name,
            recipient_affiliations = temp_p.recipient_affiliations,
            award_types = temp_p.award_types,
            last_12_months = temp_p.last_12_months,
            last_12_contracts = temp_p.last_12_contracts,
            last_12_loans = temp_p.last_12_loans,
            last_12_grants = temp_p.last_12_grants,
            last_12_direct_payments = temp_p.last_12_direct_payments,
            last_12_other = temp_p.last_12_other,
            last_12_months_count = temp_p.last_12_months_count,
            parent_uei = temp_p.parent_uei
        WHEN NOT MATCHED THEN
        INSERT (
            id,
            recipient_level,
            recipient_hash,
            recipient_unique_id,
            uei,
            parent_uei,
            recipient_name,
            recipient_affiliations,
            award_types,
            last_12_months,
            last_12_contracts,
            last_12_loans,
            last_12_grants,
            last_12_direct_payments,
            last_12_other,
            last_12_months_count
        ) VALUES (
            temp_p.id + 1,
            temp_p.recipient_level,
            temp_p.recipient_hash,
            temp_p.recipient_unique_id,
            temp_p.uei,
            temp_p.parent_uei,
            temp_p.recipient_name,
            temp_p.recipient_affiliations,
            temp_p.award_types,
            temp_p.last_12_months,
            temp_p.last_12_contracts,
            temp_p.last_12_loans,
            temp_p.last_12_grants,
            temp_p.last_12_direct_payments,
            temp_p.last_12_other,
            temp_p.last_12_months_count
        );
    """,
    """DROP TABLE temporary_restock_recipient_profile;""",
    """DROP VIEW temporary_recipients_from_transactions_view;""",
]
