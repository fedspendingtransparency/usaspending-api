DOWNLOAD_JOB_COLUMNS = {
    "download_job_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "file_name": {"delta": "STRING", "postgres": "TEXT"},
    "file_size": {"delta": "LONG", "postgres": "BIGINT"},
    "number_of_rows": {"delta": "INTEGER", "postgres": "INTEGER"},
    "number_of_columns": {"delta": "INTEGER", "postgres": "INTEGER"},
    "error_message": {"delta": "STRING", "postgres": "TEXT"},
    "create_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP"},
    "update_date": {"delta": "TIMESTAMP", "postgres": "TIMESTAMP"},
    "job_status_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "monthly_download": {"delta": "BOOLEAN", "postgres": "BOOLEAN"},
    "json_request": {"delta": "STRING", "postgres": "TEXT"},
}

DOWNLOAD_JOB_DELTA_COLUMNS = {k: v["delta"] for k, v in DOWNLOAD_JOB_COLUMNS.items()}

download_job_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in DOWNLOAD_JOB_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """
