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
sam_recipient_load_sql_string = fr"""
-- Create temp table to reload into the duns table without dropping the destination table
DROP TABLE IF EXISTS int.sam_recipient;

CREATE TABLE int.sam_recipient AS (
  SELECT
    broker_SR.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
    broker_SR.legal_business_name AS legal_business_name,
    broker_SR.ultimate_parent_unique_ide AS ultimate_parent_unique_ide,
    broker_SR.ultimate_parent_legal_enti AS ultimate_parent_legal_enti,
    broker_SR.sam_recipient_id AS broker_duns_id,
    broker_SR.record_date AS update_date,
    broker_SR.address_line_1 AS address_line_1,
    broker_SR.address_line_2 AS address_line_2,
    broker_SR.city AS city,
    broker_SR.congressional_district AS congressional_district,
    broker_SR.country_code AS country_code,
    broker_SR.state AS state,
    broker_SR.zip AS zip,
    broker_SR.zip4 AS zip4,
    broker_SR.business_types_codes AS business_types_codes,
    broker_SR.dba_name as dba_name,
    broker_SR.entity_structure as entity_structure,
    broker_SR.uei AS uei,
    broker_SR.ultimate_parent_uei AS ultimate_parent_uei
  FROM
    global_temp.broker_SR as broker_SR
      SELECT
        DISTINCT ON (broker_SR.awardee_or_recipient_uniqu)
        broker_SR.awardee_or_recipient_uniqu,
        broker_SR.legal_business_name,
        broker_SR.dba_name,
        broker_SR.ultimate_parent_unique_ide,
        broker_SR.ultimate_parent_legal_enti,
        broker_SR.address_line_1,
        broker_SR.address_line_2,
        broker_SR.city,
        broker_SR.state,
        broker_SR.zip,
        broker_SR.zip4,
        broker_SR.country_code,
        broker_SR.congressional_district,
        COALESCE(broker_SR.business_types_codes, text[]) AS business_types_codes,
        broker_SR.entity_structure,
        broker_SR.sam_recipient_id,
        COALESCE(broker_SR.activation_date, broker_SR.deactivation_date) as record_date,
        broker_SR.uei,
        broker_SR.ultimate_parent_uei
      FROM
        global_temp.sam_recipient as broker_SR
      ORDER BY
        broker_SR.awardee_or_recipient_uniqu,
        broker_SR.activation_date DESC NULLS LAST)') AS broker_duns
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
