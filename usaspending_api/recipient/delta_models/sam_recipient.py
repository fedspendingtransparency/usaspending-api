SAM_RECIPIENT_COLUMNS = {
    "awardee_or_recipient_uniqu": {"delta": "STRING", "postgres": "TEXT"},
    "legal_business_name": {"delta": "STRING", "postgres": "TEXT"},
    "ultimate_parent_unique_ide": {"delta": "INTEGER", "postgres": "INTEGER"},
    "broker_duns_id": {"delta": "INTEGER", "postgres": "INTEGER"},
    "update_date": {"delta": "DATE", "postgres": "DATE"},
    "address_line_1": {"delta": "STRING", "postgres": "TEXT"},
    "address_line_2": {"delta": "STRING", "postgres": "TEXT"},
    "city": {"delta": "STRING", "postgres": "TEXT"},
    "congressional_district": {"delta": "STRING", "postgres": "TEXT"},
    "country_code": {"delta": "STRING", "postgres": "TEXT"},
    "state": {"delta": "STRING", "postgres": "TEXT"},
    "zip": {"delta": "STRING", "postgres": "TEXT"},
    "zip4": {"delta": "STRING", "postgres": "TEXT"},
    "business_types_codes": {"delta": "ARRAY<STRING>", "postgres": "TEXT[]"},
    "dba_name": {"delta": "STRING", "postgres": "TEXT"},
    "entity_structure": {"delta": "STRING", "postgres": "TEXT"},
    "uei": {"delta": "STRING", "postgres": "TEXT"},
    "ultimate_parent_uei": {"delta": "STRING", "postgres": "TEXT"},
}

SAM_RECIPIENT_DELTA_COLUMNS = {k: v["delta"] for k, v in SAM_RECIPIENT_COLUMNS.items()}
SAM_RECIPIENT_POSTGRES_COLUMNS = {k: v["postgres"] for k, v in SAM_RECIPIENT_COLUMNS.items()}

sam_recipient_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in SAM_RECIPIENT_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """
sam_recipient_load_sql_string = [
    rf"""
CREATE OR REPLACE TEMPORARY VIEW source_data AS (
      SELECT
        ROW_NUMBER() OVER ( PARTITION BY broker_sam_recipient.award_or_recipient_uniqu
                            ORDER BY
                            awardee_or_recipient_uniqu ASC NULLS LAST,
                            activation_date DESC NULLS LAST
                            ) AS row_num,
        broker_sam_recipient.awardee_or_recipient_uniqu,
        broker_sam_recipient.legal_business_name,
        broker_sam_recipient.dba_name,
        broker_sam_recipient.ultimate_parent_unique_ide,
        broker_sam_recipient.ultimate_parent_legal_enti,
        broker_sam_recipient.address_line_1,
        broker_sam_recipient.address_line_2,
        broker_sam_recipient.city,
        broker_sam_recipient.state,
        broker_sam_recipient.zip,
        broker_sam_recipient.zip4,
        broker_sam_recipient.country_code,
        broker_sam_recipient.congressional_district,
        CAST(COALESCE(broker_sam_recipient.business_types_codes, 0), AS TEXT) AS business_types_codes,
        broker_sam_recipient.entity_structure,
        broker_sam_recipient.sam_recipient_id,
        COALESCE(broker_sam_recipient.activation_date, broker_sam_recipient.deactivation_date) as record_date,
        broker_sam_recipient.uei,
        broker_sam_recipient.ultimate_parent_uei
      FROM
        global_temp.sam_recipient as broker_sam_recipient
);""",
    rf"""
  INSERT OVERWRITE {{DESTINATION_DATABASE}}.{{DESTINATION_TABLE}}
    (
        {",".join([col for col in SAM_RECIPIENT_DELTA_COLUMNS])}
    )
  SELECT
        {",".join([col for col in SAM_RECIPIENT_DELTA_COLUMNS])}
  FROM
    source_data
  WHERE source_data.row_num =1
);

""",
]
