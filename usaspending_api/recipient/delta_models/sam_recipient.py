SAM_RECIPIENT_COLUMNS = {
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
SAM_RECIPIENT_DELTA_COLUMNS = {k: v["delta"] for k, v in SAM_RECIPIENT_COLUMNS.items()}
SAM_RECIPIENT_POSTGRES_COLUMNS = {k: v["postgres"] for k, v in SAM_RECIPIENT_COLUMNS.items()}

sam_recipient_create_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in SAM_RECIPIENT_DELTA_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """
sam_recipient_load_sql_string = rf"""


CREATE TABLE int.sam_recipient AS (
  SELECT
    broker_sam_recipient.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
    broker_sam_recipient.legal_business_name AS legal_business_name,
    broker_sam_recipient.ultimate_parent_unique_ide AS ultimate_parent_unique_ide,
    broker_sam_recipient.ultimate_parent_legal_enti AS ultimate_parent_legal_enti,
    broker_sam_recipient.sam_recipient_id AS broker_duns_id,
    broker_sam_recipient.record_date AS update_date,
    broker_sam_recipient.address_line_1 AS address_line_1,
    broker_sam_recipient.address_line_2 AS address_line_2,
    broker_sam_recipient.city AS city,
    broker_sam_recipient.congressional_district AS congressional_district,
    broker_sam_recipient.country_code AS country_code,
    broker_sam_recipient.state AS state,
    broker_sam_recipient.zip AS zip,
    broker_sam_recipient.zip4 AS zip4,
    broker_sam_recipient.business_types_codes AS business_types_codes,
    broker_sam_recipient.dba_name as dba_name,
    broker_sam_recipient.entity_structure as entity_structure,
    broker_sam_recipient.uei AS uei,
    broker_sam_recipient.ultimate_parent_uei AS ultimate_parent_uei
  FROM
    global_temp.broker_sam_recipient as broker_sam_recipient
      SELECT
        DISTINCT ON (broker_sam_recipient.awardee_or_recipient_uniqu)
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
        COALESCE(broker_sam_recipient.business_types_codes, text[]) AS business_types_codes,
        broker_sam_recipient.entity_structure,
        broker_sam_recipient.sam_recipient_id,
        COALESCE(broker_sam_recipient.activation_date, broker_sam_recipient.deactivation_date) as record_date,
        broker_sam_recipient.uei,
        broker_sam_recipient.ultimate_parent_uei
      FROM
        global_temp.sam_recipient as broker_sam_recipient
      ORDER BY
        broker_sam_recipient.awardee_or_recipient_uniqu,
        broker_sam_recipient.activation_date DESC NULLS LAST)') AS broker_duns
          (
            awardee_or_recipient_uniqu text,
            legal_business_name text,
            dba_name text,
            ultimate_parent_unique_ide text,
            ultimate_parent_legal_enti text,
            address_line_1 text,
            address_line_2 text,
            city text,
            state text,
            zip text,
            zip4 text,
            country_code text,
            congressional_district text,
            business_types_codes text[],
            entity_structure text,
            sam_recipient_id text,
            record_date date,
            uei text,
            ultimate_parent_uei text
          )
);

"""
