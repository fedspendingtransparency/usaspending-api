SAM_RECIPIENT_COLUMNS = {
    "awardee_or_recipient_uniqu": "STRING",
    "legal_business_name": "STRING",
    "ultimate_parent_unique_ide": "STRING",
    "ultimate_parent_legal_enti": "STRING",
    "broker_duns_id": "INTEGER NOT NULL",
    "update_date": "DATE NOT NULL",
    "address_line_1": "STRING",
    "address_line_2": "STRING",
    "city": "STRING",
    "congressional_district": "STRING",
    "country_code": "STRING",
    "state": "STRING",
    "zip": "STRING",
    "zip4": "STRING",
    "business_types_codes": "ARRAY<STRING>",
    "dba_name": "STRING",
    "entity_structure": "STRING",
    "uei": "STRING",
    "ultimate_parent_uei": "STRING",
}

sam_recipient_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in SAM_RECIPIENT_COLUMNS.items()])}
    ) AS
    SELECT
        broker_duns.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
        broker_duns.legal_business_name AS legal_business_name,
        broker_duns.ultimate_parent_unique_ide AS ultimate_parent_unique_ide,
        broker_duns.ultimate_parent_legal_enti AS ultimate_parent_legal_enti,
        broker_duns.sam_recipient_id AS broker_duns_id,
        broker_duns.record_date AS update_date,
        broker_duns.address_line_1 AS address_line_1,
        broker_duns.address_line_2 AS address_line_2,
        broker_duns.city AS city,
        broker_duns.congressional_district AS congressional_district,
        broker_duns.country_code AS country_code,
        broker_duns.state AS state,
        broker_duns.zip AS zip,
        broker_duns.zip4 AS zip4,
        broker_duns.business_types_codes AS business_types_codes,
        broker_duns.dba_name as dba_name,
        broker_duns.entity_structure as entity_structure,
        broker_duns.uei AS uei,
        broker_duns.ultimate_parent_uei AS ultimate_parent_uei
  FROM
  SELECT
        DISTINCT ON (sam_recipient.awardee_or_recipient_uniqu)
        sam_recipient.awardee_or_recipient_uniqu,
        sam_recipient.legal_business_name,
        sam_recipient.dba_name,
        sam_recipient.ultimate_parent_unique_ide,
        sam_recipient.ultimate_parent_legal_enti,
        sam_recipient.address_line_1,
        sam_recipient.address_line_2,
        sam_recipient.city,
        sam_recipient.state,
        sam_recipient.zip,
        sam_recipient.zip4,
        sam_recipient.country_code,
        sam_recipient.congressional_district,
        COALESCE(sam_recipient.business_types_codes, ''{}''::text[]) AS business_types_codes,
        sam_recipient.entity_structure,
        sam_recipient.sam_recipient_id,
        COALESCE(sam_recipient.activation_date, sam_recipient.deactivation_date) as record_date,
        sam_recipient.uei,
        sam_recipient.ultimate_parent_uei
      FROM
        sam_recipient
      ORDER BY
        sam_recipient.awardee_or_recipient_uniqu,
        sam_recipient.activation_date DESC NULLS LAST)') AS broker_duns
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

    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """
