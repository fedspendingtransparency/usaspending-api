RECIPIENT_LOOKUP_COLUMNS = {
    "id": "LONG NOT NULL",
    "recipient_hash": "STRING",
    "legal_business_name": "STRING",
    "duns": "STRING",
    "address_line_1": "STRING",
    "address_line_2": "STRING",
    "business_types_codes": "ARRAY<STRING>",
    "city": "STRING",
    "congressional_district": "STRING",
    "country_code": "STRING",
    "parent_duns": "STRING",
    "parent_legal_business_name": "STRING",
    "state": "STRING",
    "zip4": "STRING",
    "zip5": "STRING",
    "alternate_names": "ARRAY<STRING>",
    "source": "STRING NOT NULL",
    "update_date": "TIMESTAMP NOT NULL",
    "uei": "STRING",
    "parent_uei": "STRING",
}

recipient_lookup_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in RECIPIENT_LOOKUP_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """
