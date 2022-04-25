sam_recipient_sql_string = r"""
CREATE OR REPLACE TABLE {DESTINATION_TABLE} (
  awardee_or_recipient_uniqu STRING,
  legal_business_name STRING,
  ultimate_parent_unique_ide STRING,
  ultimate_parent_legal_enti STRING,
  broker_duns_id STRING NOT NULL,
  update_date DATE NOT NULL,
  address_line_1 STRING,
  address_line_2 STRING,
  city STRING,
  congressional_district STRING,
  country_code STRING,
  state STRING,
  zip STRING,
  zip4 STRING,
  business_types_codes ARRAY<STRING>,
  dba_name STRING,
  entity_structure STRING,
  uei STRING,
  ultimate_parent_uei STRING
)
USING DELTA
LOCATION 's3a://{AWS_S3_BUCKET}/{AWS_S3_OUTPUT_PATH}/{DESTINATION_TABLE}'
"""
