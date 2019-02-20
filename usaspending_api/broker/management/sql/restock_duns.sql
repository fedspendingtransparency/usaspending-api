-- Create temp table to reload into the duns table without dropping the destination table
DROP TABLE IF EXISTS public.temporary_restock_duns;

CREATE TABLE public.temporary_restock_duns AS (
  SELECT
    broker_duns.awardee_or_recipient_uniqu AS awardee_or_recipient_uniqu,
    broker_duns.legal_business_name AS legal_business_name,
    broker_duns.ultimate_parent_unique_ide AS ultimate_parent_unique_ide,
    broker_duns.ultimate_parent_legal_enti AS ultimate_parent_legal_enti,
    broker_duns.duns_id AS broker_duns_id,
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
    broker_duns.entity_structure as entity_structure
  FROM
    dblink ('broker_server', '(
      SELECT
        DISTINCT ON (duns.awardee_or_recipient_uniqu)
        duns.awardee_or_recipient_uniqu,
        duns.legal_business_name,
        duns.dba_name,
        duns.ultimate_parent_unique_ide,
        duns.ultimate_parent_legal_enti,
        duns.address_line_1,
        duns.address_line_2,
        duns.city,
        duns.state,
        duns.zip,
        duns.zip4,
        duns.country_code,
        duns.congressional_district,
        COALESCE(duns.business_types_codes, ''{}''::text[]) AS business_types_codes,
        duns.entity_structure,
        duns.duns_id,
        COALESCE(duns.activation_date, duns.deactivation_date) as record_date
      FROM
        duns
      ORDER BY
        duns.awardee_or_recipient_uniqu,
        duns.activation_date DESC NULLS LAST)') AS broker_duns
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
            duns_id text,
            record_date date
          )
);

BEGIN;
TRUNCATE TABLE public.duns RESTART IDENTITY;
INSERT INTO public.duns SELECT * FROM public.temporary_restock_duns;
DROP TABLE public.temporary_restock_duns;
COMMIT;
VACUUM ANALYZE public.duns;