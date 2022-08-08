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

sam_recipient_create_sql_string = fr"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{key} {val}' for key, val in SAM_RECIPIENT_COLUMNS.items()])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """
sam_recipient_load_sql_string = fr"""\
-- Create temp table to reload into the duns table without dropping the destination table
DROP TABLE IF EXISTS int.sam_recipient;

CREATE TABLE int.sam_recipient AS (
  SELECT
    sam_recipient.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
    sam_recipient.legal_business_name AS legal_business_name,
    sam_recipient.ultimate_parent_unique_ide AS ultimate_parent_unique_ide,
    sam_recipient.ultimate_parent_legal_enti AS ultimate_parent_legal_enti,
    sam_recipient.sam_recipient_id AS broker_duns_id,
    sam_recipient.record_date AS update_date,
    sam_recipient.address_line_1 AS address_line_1,
    sam_recipient.address_line_2 AS address_line_2,
    sam_recipient.city AS city,
    sam_recipient.congressional_district AS congressional_district,
    sam_recipient.country_code AS country_code,
    sam_recipient.state AS state,
    sam_recipient.zip AS zip,
    sam_recipient.zip4 AS zip4,
    sam_recipient.business_types_codes AS business_types_codes,
    sam_recipient.dba_name as dba_name,
    sam_recipient.entity_structure as entity_structure,
    sam_recipient.uei AS uei,
    sam_recipient.ultimate_parent_uei AS ultimate_parent_uei
  FROM
    sam_recipient
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
        COALESCE(sam_recipient.business_types_codes, text[]) AS business_types_codes,
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

BEGIN;
DELETE FROM raw.duns;
INSERT INTO raw.duns SELECT * FROM raw.temporary_restock_duns;
DROP TABLE raw.temporary_restock_duns;
COMMIT;
VACUUM ANALYZE raw.duns;



"""
