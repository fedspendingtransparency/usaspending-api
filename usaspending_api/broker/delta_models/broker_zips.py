ZIPS_COLUMNS = {
    "created_at": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP"},
    "updated_at": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP"},
    "zips_id": {"delta": "INTEGER NOT NULL", "postgres": "INTEGER NOT NULL"},
    "zip5": {"delta": "STRING", "postgres": "TEXT"},
    "zip_last4": {"delta": "STRING", "postgres": "TEXT"},
    "state_abbreviation": {"delta": "STRING", "postgres": "TEXT"},
    "county_number": {"delta": "STRING", "postgres": "TEXT"},
    "congressional_district_no": {"delta": "STRING", "postgres": "TEXT"},
}
ZIPS_DELTA_COLUMNS = {k: v["delta"] for k, v in ZIPS_COLUMNS.items()}
ZIPS_POSTGRES_COLUMNS = {k: v["postgres"] for k, v in ZIPS_COLUMNS.items()}

zips_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in ZIPS_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
"""
