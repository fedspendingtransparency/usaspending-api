RECIPIENT_PROFILE_COLUMNS_WITHOUT_ID = {
    "recipient_level": {"delta": "STRING NOT NULL", "postgres": "TEXT NOT NULL"},
    "recipient_hash": {"delta": "STRING", "postgres": "UUID"},
    "recipient_unique_id": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_name": {"delta": "STRING", "postgres": "TEXT"},
    "recipient_affiliations": {"delta": "ARRAY<STRING> NOT NULL", "postgres": "TEXT[]"},
    "last_12_months": {"delta": "numeric(23,2) NOT NULL", "postgres": "numeric(23,2"},
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
RECIPIENT_PROFILE_COLUMNS = {
    "id": {"delta": "LONG", "postgres": "BIGINT NOT NULL"},
    **RECIPIENT_PROFILE_COLUMNS_WITHOUT_ID,
}
RECIPIENT_PROFILE_DELTA_COLUMNS = {k: v["delta"] for k, v in RECIPIENT_PROFILE_COLUMNS.items()}
RPT_RECIPIENT_PROFILE_DELTA_COLUMNS = {k: v["postgres"] for k, v in RECIPIENT_PROFILE_COLUMNS_WITHOUT_ID.items()}
RECIPIENT_PROFILE_POSTGRES_COLUMNS = {k: v["postgres"] for k, v in RECIPIENT_PROFILE_COLUMNS_WITHOUT_ID.items()}

recipient_profile_create_sql_string = f"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in RECIPIENT_PROFILE_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """
recipient_profile_load_sql_strings = [
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
        int.transaction_normalized as tn
    LEFT OUTER JOIN int.transaction_fpds as fpds ON tn.id = fpds.transaction_id
    LEFT OUTER JOIN int.transaction_fabs as fabs ON tn.id = fabs.transaction_id
    WHERE
        tn.action_date >= '2007-10-01'
        AND tn.type IS NOT NULL
    );""",
    # --------------------------------------------------------------------------------
    # -- Step 2, Populate table with 100% of combinations
    # --------------------------------------------------------------------------------
    f"""
        CREATE OR REPLACE TEMPORARY VIEW step_2 AS (
            SELECT
                'P' as recipient_level,
                recipient_hash AS recipient_hash,
                duns AS recipient_unique_id,
                legal_business_name AS recipient_name,
                ARRAY() AS recipient_affiliations,
                0.00 AS last_12_months,
                0 AS id,
                0.00 AS last_12_contracts,
                0.00 AS last_12_grants,
                0.00 AS last_12_direct_payments,
                0.00 AS last_12_loans,
                0.00 AS last_12_other,
                0 AS last_12_months_count,
                ARRAY() AS award_types,
                uei AS uei,
                parent_uei AS parent_uei
            FROM
                rpt.recipient_lookup
            UNION ALL
                SELECT
                    'C' as recipient_level,
                    recipient_hash AS recipient_hash,
                    duns AS recipient_unique_id,
                    legal_business_name AS recipient_name,
                    ARRAY() AS recipient_affiliations,
                    0.00 AS last_12_months,
                    0 AS id,
                    0.00 AS last_12_contracts,
                    0.00 AS last_12_grants,
                    0.00 AS last_12_direct_payments,
                    0.00 AS last_12_loans,
                    0.00 AS last_12_other,
                    0 AS last_12_months_count,
                    ARRAY() AS award_types,
                    uei AS uei,
                    parent_uei AS parent_uei
                FROM
                    rpt.recipient_lookup
            UNION ALL
                SELECT
                    'R' as recipient_level,
                    recipient_hash AS recipient_hash,
                    duns AS recipient_unique_id,
                    legal_business_name AS recipient_name,
                    ARRAY() AS recipient_affiliations,
                    0.00 AS last_12_months,
                    0 AS id,
                    0.00 AS last_12_contracts,
                    0.00 AS last_12_grants,
                    0.00 AS last_12_direct_payments,
                    0.00 AS last_12_loans,
                    0.00 AS last_12_other,
                    0 AS last_12_months_count,
                    ARRAY() AS award_types,
                    uei AS uei,
                    parent_uei AS parent_uei
                FROM
                    rpt.recipient_lookup
        );
    """,
    # --------------------------------------------------------------------------------
    # -- Step 3, Obligation for past 12 months
    # --------------------------------------------------------------------------------
    f"""
    CREATE OR REPLACE TEMPORARY VIEW step_3 AS (
        WITH grouped_by_category AS (
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
        SELECT
            rpv.recipient_level,
            rpv.recipient_hash,
            rpv.recipient_unique_id,
            rpv.recipient_name,
            rpv.recipient_affiliations,
            rpv.uei,
            rpv.parent_uei,
            rpv.award_types || COALESCE(gbc.award_types, ARRAY()) AS award_types,
            rpv.last_12_months + COALESCE(gbc.amount, 0) AS last_12_months,
            rpv.last_12_contracts + COALESCE(gbc.last_12_contracts, 0) AS last_12_contracts,
            rpv.last_12_grants + COALESCE(gbc.last_12_grants, 0) AS last_12_grants,
            rpv.last_12_direct_payments + COALESCE(gbc.last_12_direct_payments, 0) AS last_12_direct_payments,
            rpv.last_12_loans + COALESCE(gbc.last_12_loans, 0) AS last_12_loans,
            rpv.last_12_other + COALESCE(gbc.last_12_other, 0) AS last_12_other,
            rpv.last_12_months_count + COALESCE(gbc.count, 0) AS last_12_months_count,
            CASE
                WHEN gbc.recipient_hash IS NOT NULL THEN 1 -- WHEN "MATCHED"
                ELSE rpv.id
            END AS id
        FROM step_2 AS rpv
        LEFT OUTER JOIN grouped_by_category AS gbc
        ON gbc.recipient_hash = rpv.recipient_hash AND
            gbc.recipient_level = rpv.recipient_level
    );""",
    # --------------------------------------------------------------------------------
    # -- Step 4, Populate the Parent Obligation for past 12 months
    # --------------------------------------------------------------------------------
    f"""
    CREATE OR REPLACE TEMPORARY VIEW step_4 AS (
        WITH grouped_by_parent AS (
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
        SELECT
            rpv.recipient_level,
            rpv.recipient_hash,
            rpv.recipient_unique_id,
            rpv.recipient_name,
            rpv.recipient_affiliations,
            rpv.uei,
            rpv.parent_uei,
            rpv.award_types || COALESCE(gbp.award_types, ARRAY()) AS award_types,
            rpv.last_12_months + COALESCE(gbp.amount, 0) AS last_12_months,
            rpv.last_12_contracts + COALESCE(gbp.last_12_contracts, 0) AS last_12_contracts,
            rpv.last_12_grants + COALESCE(gbp.last_12_grants, 0) AS last_12_grants,
            rpv.last_12_direct_payments + COALESCE(gbp.last_12_direct_payments, 0) AS last_12_direct_payments,
            rpv.last_12_loans + COALESCE(gbp.last_12_loans, 0) AS last_12_loans,
            rpv.last_12_other + COALESCE(gbp.last_12_other, 0) AS last_12_other,
            rpv.last_12_months_count + COALESCE(gbp.count, 0) AS last_12_months_count,
            CASE
                WHEN gbp.uei IS NOT NULL THEN 1 -- WHEN "MATCHED"
                ELSE rpv.id
            END AS id
        FROM step_3 AS rpv
        LEFT OUTER JOIN grouped_by_parent AS gbp
        ON rpv.uei = gbp.uei AND
            rpv.recipient_level = 'P'
    );""",
    # --------------------------------------------------------------------------------
    # -- Step 5, Populating child recipient list in parents
    # --------------------------------------------------------------------------------
    f"""
    CREATE OR REPLACE TEMPORARY VIEW step_5 AS (
        WITH parent_recipients AS (
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
        SELECT
            rpv.recipient_level,
            rpv.recipient_hash,
            rpv.recipient_unique_id,
            rpv.recipient_name,
            rpv.uei,
            rpv.parent_uei,
            rpv.award_types,
            rpv.last_12_months,
            rpv.last_12_contracts,
            rpv.last_12_grants,
            rpv.last_12_direct_payments,
            rpv.last_12_loans,
            rpv.last_12_other,
            rpv.last_12_months_count,
            COALESCE(pr.uei_list, rpv.recipient_affiliations) AS recipient_affiliations,
            CASE
                WHEN pr.parent_uei IS NOT NULL THEN 1 -- WHEN "MATCHED"
                ELSE rpv.id
            END AS id
        FROM step_4 AS rpv
        LEFT OUTER JOIN parent_recipients AS pr
        ON rpv.uei = pr.parent_uei and rpv.recipient_level = 'P'
    );""",
    # --------------------------------------------------------------------------------
    # -- Step 6, Populate parent recipient list in children
    # --------------------------------------------------------------------------------
    f"""
    CREATE OR REPLACE TEMPORARY VIEW step_6 AS (
        WITH all_recipients AS (
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
        SELECT
            rpv.recipient_level,
            rpv.recipient_hash,
            rpv.recipient_unique_id,
            rpv.recipient_name,
            rpv.uei,
            rpv.parent_uei,
            rpv.award_types,
            rpv.last_12_months,
            rpv.last_12_contracts,
            rpv.last_12_grants,
            rpv.last_12_direct_payments,
            rpv.last_12_loans,
            rpv.last_12_other,
            rpv.last_12_months_count,
            COALESCE(ar.parent_uei_list, rpv.recipient_affiliations) AS recipient_affiliations,
            CASE
                WHEN ar.uei IS NOT NULL THEN 1 -- WHEN "MATCHED"
                ELSE rpv.id
            END AS id
        FROM step_5 AS rpv
        LEFT OUTER JOIN all_recipients AS ar
        ON rpv.uei = ar.uei AND
            rpv.recipient_level = 'C'
    );""",
    # --------------------------------------------------------------------------------
    # -- Step 7, Mark recipient profile rows older than 12 months as valid
    # --------------------------------------------------------------------------------
    f"""
    CREATE OR REPLACE TEMPORARY VIEW step_7 AS (
        WITH grouped_by_old_recipients AS (
            SELECT
                recipient_hash,
                recipient_level
            FROM
                temporary_recipients_from_transactions_view AS trft
            GROUP BY recipient_hash, recipient_level
        )
        SELECT
            rpv.recipient_level,
            rpv.recipient_hash,
            rpv.recipient_unique_id,
            rpv.recipient_name,
            rpv.recipient_affiliations,
            rpv.uei,
            rpv.parent_uei,
            rpv.award_types,
            rpv.last_12_months,
            rpv.last_12_contracts,
            rpv.last_12_grants,
            rpv.last_12_direct_payments,
            rpv.last_12_loans,
            rpv.last_12_other,
            rpv.last_12_months_count,
            CASE
                WHEN gbc.recipient_hash IS NOT NULL THEN 1 -- WHEN "MATCHED"
                ELSE rpv.id
            END AS id
        FROM step_6 AS rpv
        LEFT OUTER JOIN grouped_by_old_recipients AS gbc
        ON gbc.recipient_hash = rpv.recipient_hash AND
            gbc.recipient_level = rpv.recipient_level AND
            rpv.id = 0
    );""",
    # --------------------------------------------------------------------------------
    # -- Step 8, Mark Parent recipient profile rows older than 12 months as valid
    # --------------------------------------------------------------------------------
    f"""
    CREATE OR REPLACE TEMPORARY VIEW step_8 AS (
        WITH grouped_by_parent_old AS (
            SELECT
                parent_uei
            FROM
                temporary_recipients_from_transactions_view AS trft
            WHERE
                parent_uei IS NOT NULL
            GROUP BY parent_uei
        )
        SELECT
            rpv.recipient_level,
            rpv.recipient_hash,
            rpv.recipient_unique_id,
            rpv.recipient_name,
            rpv.recipient_affiliations,
            rpv.uei,
            rpv.parent_uei,
            rpv.award_types,
            rpv.last_12_months,
            rpv.last_12_contracts,
            rpv.last_12_grants,
            rpv.last_12_direct_payments,
            rpv.last_12_loans,
            rpv.last_12_other,
            rpv.last_12_months_count,
            CASE
                WHEN gbp.parent_uei IS NOT NULL THEN 1 -- WHEN "MATCHED"
                ELSE rpv.id
            END AS id
        FROM step_7 AS rpv
        LEFT OUTER JOIN grouped_by_parent_old AS gbp
        ON rpv.uei = gbp.parent_uei AND
            rpv.recipient_level = 'P' AND
            rpv.id = 0
    );""",
    # --------------------------------------------------------------------------------
    # -- Step 9, Delete unused data from table
    # --------------------------------------------------------------------------------
    rf"""
        INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}}
        (
            {",".join(list(RECIPIENT_PROFILE_COLUMNS))}
        )
        SELECT
            {",".join(list(RECIPIENT_PROFILE_COLUMNS))}
        FROM
            step_8 AS rpv
        WHERE rpv.id != 0
    """,
    """DROP VIEW temporary_recipients_from_transactions_view;""",
    """DROP VIEW step_2;""",
    """DROP VIEW step_3;""",
    """DROP VIEW step_4;""",
    """DROP VIEW step_5;""",
    """DROP VIEW step_6;""",
    """DROP VIEW step_7;""",
    """DROP VIEW step_8;""",
]
