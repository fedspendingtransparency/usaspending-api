FINANCIAL_ACCOUNTS_BY_AWARDS_COLUMNS = {
    "data_source": "STRING",
    "financial_accounts_by_awards_id": "INTEGER NOT NULL",
    "piid": "STRING",
    "parent_award_id": "STRING",
    "fain": "STRING",
    "uri": "STRING",
    "ussgl480100_undelivered_orders_obligations_unpaid_fyb": "NUMERIC(23,2)",
    "ussgl480100_undelivered_orders_obligations_unpaid_cpe": "NUMERIC(23,2)",
    "ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe": "NUMERIC(23,2)",
    "ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe": "NUMERIC(23,2)",
    "ussgl490100_delivered_orders_obligations_unpaid_fyb": "NUMERIC(23,2)",
    "ussgl490100_delivered_orders_obligations_unpaid_cpe": "NUMERIC(23,2)",
    "ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe": "NUMERIC(23,2)",
    "ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe": "NUMERIC(23,2)",
    "ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb": "NUMERIC(23,2)",
    "ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe": "NUMERIC(23,2)",
    "ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe": "NUMERIC(23,2)",
    "ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe": "NUMERIC(23,2)",
    "ussgl490200_delivered_orders_obligations_paid_cpe": "NUMERIC(23,2)",
    "ussgl490800_authority_outlayed_not_yet_disbursed_fyb": "NUMERIC(23,2)",
    "ussgl490800_authority_outlayed_not_yet_disbursed_cpe": "NUMERIC(23,2)",
    "ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe": "NUMERIC(23,2)",
    "obligations_undelivered_orders_unpaid_total_cpe": "NUMERIC(23,2)",
    "obligations_delivered_orders_unpaid_total_fyb": "NUMERIC(23,2)",
    "obligations_delivered_orders_unpaid_total_cpe": "NUMERIC(23,2)",
    "gross_outlays_undelivered_orders_prepaid_total_fyb": "NUMERIC(23,2)",
    "gross_outlays_undelivered_orders_prepaid_total_cpe": "NUMERIC(23,2)",
    "gross_outlays_delivered_orders_paid_total_fyb": "NUMERIC(23,2)",
    "gross_outlay_amount_by_award_fyb": "NUMERIC(23,2)",
    "gross_outlay_amount_by_award_cpe": "NUMERIC(23,2)",
    "obligations_incurred_total_by_award_cpe": "NUMERIC(23,2)",
    "ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe": "NUMERIC(23,2)",
    "ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe": "NUMERIC(23,2)",
    "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe": "NUMERIC(23,2)",
    "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe": "NUMERIC(23,2)",
    "deobligations_recoveries_refunds_of_prior_year_by_award_cpe": "NUMERIC(23,2)",
    "obligations_undelivered_orders_unpaid_total_fyb": "NUMERIC(23,2)",
    "gross_outlays_delivered_orders_paid_total_cpe": "NUMERIC(23,2)",
    "drv_award_id_field_type": "STRING",
    "drv_obligations_incurred_total_by_award": "NUMERIC(23,2)",
    "transaction_obligated_amount": "NUMERIC(23,2)",
    "reporting_period_start": "DATE",
    "reporting_period_end": "DATE",
    "last_modified_date": "DATE",
    "certified_date": "DATE",
    "create_date": "TIMESTAMP",
    "update_date": "TIMESTAMP",
    "award_id": "LONG",
    "object_class_id": "INTEGER",
    "program_activity_id": "INTEGER",
    "submission_id": "INTEGER NOT NULL",
    "treasury_account_id": "INTEGER",
    "distinct_award_key": "STRING NOT NULL",
    "disaster_emergency_fund_code": "STRING",
}

financial_accounts_by_awards_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in FINANCIAL_ACCOUNTS_BY_AWARDS_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
"""

c_to_d_linkage_view_sql_strings = [
    # -----
    # Before starting to link records, we should first unlink FABA records from Awards that no longer
    # exist. This creates a view on FABA with a subset of fields needed for the linkage process;
    # Also upper cases some fields because the UPPER() operation can't be used in some predicates
    # in Spark SQL
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW faba_upper AS (
        SELECT
            faba.financial_accounts_by_awards_id,
            aw.id AS award_id,
            UPPER(faba.piid) AS piid,
            UPPER(faba.parent_award_id) AS parent_award_id,
            UPPER(faba.fain) AS fain,
            UPPER(faba.uri) AS uri
        FROM
            raw.financial_accounts_by_awards AS faba
        LEFT JOIN
            int.awards AS aw
        ON
            faba.award_id = aw.id
    );
    """,
    # -----
    # Selects a subset of fields from the Awards table and uppercases them because the UPPER() operation
    # can't be used in some predicates in Spark SQL
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW awards_upper AS (
        SELECT
            id,
            UPPER(piid) AS piid,
            UPPER(parent_award_piid) AS parent_award_piid,
            UPPER(fain) AS fain,
            UPPER(uri) AS uri
        FROM
            int.awards
    );
    """,
    # -----
    # The first of several views that link FABA records to Award records;
    # When PIID and Parent PIID are populated, update File C contract records to link to a corresponding
    # Award if there is a single exact match based on PIID and Parent PIID
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW c_to_d_piid_and_parent_piid AS (
        SELECT
            financial_accounts_by_awards_id,
            aw.id AS award_id,
            1 AS priority
        FROM
            faba_upper AS faba
        INNER JOIN
            awards_upper AS aw
        ON
            aw.piid = faba.piid
            AND aw.parent_award_piid = faba.parent_award_id
        WHERE
            faba.piid IS NOT NULL
            AND faba.award_id IS NULL
            AND faba.parent_award_id IS NOT NULL
            AND (
                SELECT COUNT(*)
                FROM awards_upper AS aw_sub
                WHERE
                    aw_sub.piid = faba.piid
                    AND aw_sub.parent_award_piid = faba.parent_award_id
            ) = 1
    );
    """,
    # -----
    # When PIID is populated and Parent PIID is NULL, update File C contract records to link to a
    # corresponding award if there is a single exact match based on PIID
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW c_to_d_piid AS (
        SELECT
            financial_accounts_by_awards_id,
            aw.id AS award_id,
            2 AS priority
        FROM
            faba_upper AS faba
        INNER JOIN
            awards_upper AS aw
        ON
            aw.piid = faba.piid
        WHERE
            faba.piid IS NOT NULL
            AND faba.award_id IS NULL
            AND faba.parent_award_id IS NULL
            AND (
                SELECT COUNT(*)
                FROM awards_upper AS aw_sub
                WHERE
                    aw_sub.piid = faba.piid
            ) = 1
    );
    """,
    # -----
    # When only FAIN is populated in File C, update File C assistance records to link to a corresponding
    # award if there is a single exact match based on FAIN.
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW c_to_d_fain AS (
        SELECT
            financial_accounts_by_awards_id,
            aw.id AS award_id,
            3 AS priority
        FROM
            faba_upper AS faba
        INNER JOIN
            awards_upper AS aw
        ON
            aw.fain = faba.fain
        WHERE
            faba.fain IS NOT NULL
            AND faba.uri IS NULL
            AND faba.award_id IS NULL
            AND (
                SELECT COUNT(*)
                FROM awards_upper AS aw_sub
                WHERE aw_sub.fain = faba.fain
            ) = 1
    );
    """,
    # -----
    # When only URI is populated in File C, update File C assistance records to link to a corresponding
    # award if there is a single exact match based on URI.
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW c_to_d_uri AS (
        SELECT
            financial_accounts_by_awards_id,
            aw.id AS award_id,
            4 AS priority
        FROM
            faba_upper AS faba
        INNER JOIN
            awards_upper AS aw
        ON
            aw.uri = faba.uri
        WHERE
            faba.uri IS NOT NULL
            AND faba.fain IS NULL
            AND faba.award_id IS NULL
            AND (
                SELECT COUNT(*)
                FROM awards_upper AS aw_sub
                WHERE aw_sub.uri = faba.uri
            ) = 1
    );
    """,
    # -----
    # When both FAIN and URI are populated in File C, update File C assistance records to link to a
    # corresponding award if there is an single exact match based on FAIN
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW c_to_d_fain_and_uri_fain AS (
        SELECT
            financial_accounts_by_awards_id,
            aw.id AS award_id,
            5 AS priority
        FROM
            faba_upper AS faba
        INNER JOIN
            awards_upper AS aw
        ON
            aw.fain = faba.fain
        WHERE
            faba.fain IS NOT NULL
            AND faba.uri IS NOT NULL
            AND faba.award_id IS NULL
            AND (
                SELECT COUNT(*)
                FROM awards_upper AS aw_sub
                WHERE
                    aw_sub.fain = faba.fain
            ) = 1
    );
    """,
    # -----
    # When both FAIN and URI are populated in File C, update File C assistance records to link to a
    # corresponding award if there is an single exact match based on URI
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW c_to_d_fain_and_uri_uri AS (
        SELECT
            financial_accounts_by_awards_id,
            aw.id AS award_id,
            6 AS priority
        FROM
            faba_upper AS faba
        INNER JOIN
            awards_upper AS aw
        ON
            aw.uri = faba.uri
        WHERE
            faba.fain IS NOT NULL
            AND faba.uri IS NOT NULL
            AND faba.award_id IS NULL
            AND (
                SELECT COUNT(*)
                FROM awards_upper AS aw_sub
                WHERE
                    aw_sub.uri = faba.uri
            ) = 1
    );
    """,
    # -----
    # Determine which FABA records need to be unlinked to an Award because its corresponding Award
    # was deleted;
    # This is necessary in addition to the first SQL statement checking for deletes. The first view
    # was of ALL FABA records and included NULLs on deleted awards, so those FABA records would
    # be included in the previous six `c_to_d_*` linkage views. This second view checking for deletes
    # is adding a new `c_to_d_*` linkage view to be included in the final UNION and MERGE into the
    # `int` version of `financial_accounts_by_awards`
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW c_to_d_deletes AS (
        SELECT
            faba.financial_accounts_by_awards_id,
            aw.id,
            7 AS priority
        FROM
            raw.financial_accounts_by_awards AS faba
        LEFT JOIN
            awards_upper AS aw
        ON
            faba.award_id = aw.id
        WHERE
            aw.id IS NULL
            AND faba.award_id IS NOT NULL
    );
    """,
    # -----
    # Union all the `c_to_d_*` linkage views into a single view
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW union_all AS (
        SELECT * FROM c_to_d_piid_and_parent_piid
        UNION ALL
        SELECT * FROM c_to_d_piid
        UNION ALL
        SELECT * FROM c_to_d_fain
        UNION ALL
        SELECT * FROM c_to_d_uri
        UNION ALL
        SELECT * FROM c_to_d_fain_and_uri_fain
        UNION ALL
        SELECT * FROM c_to_d_fain_and_uri_uri
        UNION ALL
        SELECT * FROM c_to_d_deletes
    );
    """,
    # -----
    # Filter the results of the above view down to a single set of distinct FABA IDs based on view
    # priority. In theory, no filtering should happen here because each view should include a distinct
    # set of FABA IDs. This is a safeguard
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW union_all_priority AS (
        SELECT
        financial_accounts_by_awards_id,
        award_id
        FROM (
        SELECT
            financial_accounts_by_awards_id,
            award_id,
            ROW_NUMBER() OVER (
            PARTITION BY financial_accounts_by_awards_id
            ORDER BY
                priority ASC
            ) as row_num
        FROM union_all
        )
        WHERE row_num = 1
    );
    """,
    # -----
    # Create view of distinct awards that are being linked to
    # -----
    """
    CREATE OR REPLACE TEMPORARY VIEW updated_awards AS (
        SELECT DISTINCT award_id FROM union_all_priority
    );
    """,
]

# List of SQL strings used to drop the views created for File C to D Linkage
c_to_d_linkage_drop_view_sql_strings = [
    "DROP VIEW faba_upper;",
    "DROP VIEW awards_upper;",
    "DROP VIEW c_to_d_piid_and_parent_piid;",
    "DROP VIEW c_to_d_piid;",
    "DROP VIEW c_to_d_fain;",
    "DROP VIEW c_to_d_uri;",
    "DROP VIEW c_to_d_fain_and_uri_fain;",
    "DROP VIEW c_to_d_fain_and_uri_uri;",
    "DROP VIEW c_to_d_deletes;",
    "DROP VIEW union_all;",
    "DROP VIEW union_all_priority;",
    "DROP VIEW updated_awards;",
]
