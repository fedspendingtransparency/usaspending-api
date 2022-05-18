recipient_profile_sql_string = r"""
CREATE OR REPLACE TABLE {DESTINATION_TABLE} (
  recipient_level STRING NOT NULL,
  recipient_hash STRING,
  recipient_unique_id STRING,
  recipient_name STRING,
  recipient_affiliations ARRAY<STRING> NOT NULL,
  last_12_months numeric(23,2) NOT NULL,
  id LONG NOT NULL,
  last_12_contracts numeric(23,2) NOT NULL,
  last_12_direct_payments numeric(23,2) NOT NULL,
  last_12_grants numeric(23,2) NOT NULL,
  last_12_loans numeric(23,2) NOT NULL,
  last_12_months_count INTEGER NOT NULL,
  last_12_other numeric(23,2) NOT NULL,
  award_types ARRAY<STRING> NOT NULL,
  uei STRING,
  parent_uei STRING
)
USING DELTA
LOCATION 's3a://{SPARK_S3_BUCKET}/{DELTA_LAKE_S3_PATH}/{DESTINATION_DATABASE}/{DESTINATION_TABLE}'
"""
