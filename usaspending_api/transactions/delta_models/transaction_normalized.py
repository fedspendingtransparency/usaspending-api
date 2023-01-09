TRANSACTION_NORMALIZED_COLUMNS = {
    "action_date": "DATE",
    "action_type": "STRING",
    "action_type_description": "STRING",
    "award_id": "LONG NOT NULL",
    "awarding_agency_id": "INTEGER",
    "business_categories": "ARRAY<STRING>",
    "certified_date": "DATE",
    "create_date": "TIMESTAMP",
    "description": "STRING",
    "face_value_loan_guarantee": "NUMERIC(23, 2)",
    "federal_action_obligation": "NUMERIC(23, 2)",
    "fiscal_year": "INTEGER",
    "funding_agency_id": "INTEGER",
    "funding_amount": "NUMERIC(23, 2)",
    "id": "LONG NOT NULL",
    "indirect_federal_sharing": "NUMERIC(23, 2)",
    "is_fpds": "BOOLEAN NOT NULL",
    "last_modified_date": "DATE",
    "modification_number": "STRING",
    "non_federal_funding_amount": "NUMERIC(23, 2)",
    "original_loan_subsidy_cost": "NUMERIC(23, 2)",
    "period_of_performance_current_end_date": "DATE",
    "period_of_performance_start_date": "DATE",
    "transaction_unique_id": "STRING NOT NULL",
    "type": "STRING",
    "type_description": "STRING",
    "unique_award_key": "STRING",
    "update_date": "TIMESTAMP",
    "usaspending_unique_transaction_id": "STRING",
}

transaction_normalized_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in TRANSACTION_NORMALIZED_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """
