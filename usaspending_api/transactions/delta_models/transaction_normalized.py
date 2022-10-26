TRANSACTION_NORMALIZED_COLUMNS = {
    "id": "LONG NOT NULL",
    "award_id": "LONG NOT NULL",
    "is_fpds": "BOOLEAN NOT NULL",
    "transaction_unique_id": "STRING NOT NULL",
    "usaspending_unique_transaction_id": "STRING",
    "type": "STRING",
    "type_description": "STRING",
    "period_of_performance_start_date": "DATE",
    "period_of_performance_current_end_date": "DATE",
    "action_date": "DATE NOT NULL",
    "action_type": "STRING",
    "action_type_description": "STRING",
    "federal_action_obligation": "NUMERIC(23,2)",
    "modification_number": "STRING",
    "description": "STRING",
    "drv_award_transaction_usaspend": "NUMERIC(23,2)",
    "drv_current_total_award_value_amount_adjustment": "NUMERIC(23,2)",
    "drv_potential_total_award_value_amount_adjustment": "NUMERIC(23,2)",
    "last_modified_date": "DATE",
    "certified_date": "DATE",
    "create_date": "TIMESTAMP",
    "update_date": "TIMESTAMP",
    "fiscal_year": "INTEGER",
    "awarding_agency_id": "INTEGER",
    "funding_agency_id": "INTEGER",
    "original_loan_subsidy_cost": "NUMERIC(23,2)",
    "face_value_loan_guarantee": "NUMERIC(23,2)",
    "indirect_federal_sharing": "NUMERIC(23,2)",
    "funding_amount": "NUMERIC(23,2)",
    "non_federal_funding_amount": "NUMERIC(23,2)",
    "unique_award_key": "STRING",
    "business_categories": "ARRAY<STRING> NOT NULL",
}

transaction_normalized_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in TRANSACTION_NORMALIZED_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """
