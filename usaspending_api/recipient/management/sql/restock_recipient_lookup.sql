
DROP TABLE IF EXISTS public.temporary_restock_recipient_lookup;

--------------------------------------------------------------------------------
-- Step 1, Create temporary table
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 1: Creating temporary table and unique index'; END $$;
CREATE TABLE public.temporary_restock_recipient_lookup (
    recipient_hash uuid,
    legal_business_name text,
    duns text,
    parent_duns text,
    parent_legal_business_name text,
    address_line_1 text,
    address_line_2 text,
    city text,
    state text,
    zip text,
    zip4 text,
    country_code text,
    congressional_district text,
    business_types_codes text[]
);

CREATE UNIQUE INDEX idx_temporary_duns_unique_duns ON public.temporary_restock_recipient_lookup (duns);

-- --------------------------------------------------------------------------------
-- -- Step 2, Create rows with Parent DUNS + Parent Recipient Names from SAM
-- --------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 2: Creating records from SAM parent data'; END $$;
WITH grouped_parent_recipients AS (
    SELECT
    DISTINCT ON (ultimate_parent_unique_ide, ultimate_parent_legal_enti)
      ultimate_parent_unique_ide AS duns,
      ultimate_parent_legal_enti AS legal_business_name
    FROM duns
    WHERE ultimate_parent_unique_ide IS NOT NULL
    ORDER BY ultimate_parent_unique_ide, ultimate_parent_legal_enti, broker_duns_id DESC
)
INSERT INTO public.temporary_restock_recipient_lookup (
    recipient_hash, legal_business_name, duns, parent_duns, parent_legal_business_name)
SELECT
    MD5(UPPER(CONCAT(gpr.duns, gpr.legal_business_name)))::uuid AS recipient_hash,
    UPPER(gpr.legal_business_name) AS legal_business_name,
    gpr.duns AS duns,
    gpr.duns AS parent_duns,
    UPPER(gpr.legal_business_name) AS parent_legal_business_name
FROM grouped_parent_recipients AS gpr
ON CONFLICT (duns) DO NOTHING;


-- --------------------------------------------------------------------------------
-- -- Step 3, Upsert rows with DUNS + Recipient Names from SAM
-- --------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 3: Adding/updating records from SAM'; END $$;

INSERT INTO public.temporary_restock_recipient_lookup (
  recipient_hash, legal_business_name, duns,
  parent_duns, parent_legal_business_name, address_line_1,
  address_line_2, city, state, zip, zip4, country_code,
  congressional_district, business_types_codes
  )
  SELECT
    DISTINCT ON (awardee_or_recipient_uniqu, legal_business_name)
    MD5(UPPER(CONCAT(awardee_or_recipient_uniqu, legal_business_name)))::uuid AS recipient_hash,
    UPPER(legal_business_name) AS legal_business_name,
    awardee_or_recipient_uniqu AS duns,
    ultimate_parent_unique_ide AS parent_duns,
    UPPER(ultimate_parent_legal_enti) AS parent_legal_business_name,
    address_line_1,
    address_line_2,
    city,
    state,
    zip,
    zip4,
    country_code,
    congressional_district,
    business_types_codes
  FROM duns
  ORDER BY awardee_or_recipient_uniqu, legal_business_name, update_date DESC
ON CONFLICT (duns) DO UPDATE SET
    parent_legal_business_name = excluded.parent_legal_business_name,
    address_line_1 = excluded.address_line_1,
    address_line_2 = excluded.address_line_2,
    city = excluded.city,
    state = excluded.state,
    zip = excluded.zip,
    zip4 = excluded.zip4,
    country_code = excluded.country_code,
    congressional_district = excluded.congressional_district,
    business_types_codes = excluded.business_types_codes;

-- --------------------------------------------------------------------------------
-- -- Step 4, Get other recipients from FPDS (20 minutes)
-- --------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 4: Adding missing DUNS records from FPDS'; END $$;
WITH transaction_recipients AS (
    WITH transaction_recipients_inner AS (
      SELECT
        tf.awardee_or_recipient_uniqu,
        tf.ultimate_parent_legal_enti,
        tf.ultimate_parent_unique_ide,
        tf.awardee_or_recipient_legal,
        tf.legal_entity_city_name,
        tf.legal_entity_state_code,
        tf.legal_entity_zip4,
        tf.legal_entity_congressional,
        tf.legal_entity_address_line1,
        tf.legal_entity_address_line2,
        tf.legal_entity_country_code
      FROM transaction_fpds AS tf
      WHERE tf.awardee_or_recipient_uniqu IS NOT NULL
      ORDER BY tf.action_date DESC
    )
    SELECT
      DISTINCT awardee_or_recipient_uniqu,
      MD5(UPPER(CONCAT(awardee_or_recipient_uniqu, awardee_or_recipient_legal)))::uuid AS recipient_hash,
      awardee_or_recipient_uniqu AS duns,
      ultimate_parent_legal_enti AS parent_duns,
      ultimate_parent_unique_ide AS parent_legal_business_name,
      awardee_or_recipient_legal AS legal_business_name,
      legal_entity_city_name AS city,
      legal_entity_state_code AS state,
      legal_entity_zip4 AS zip4,
      legal_entity_congressional AS congressional_district,
      legal_entity_address_line1 AS address_line_1,
      legal_entity_address_line2 AS address_line_2,
      legal_entity_country_code AS country_code
    FROM transaction_recipients_inner
)

INSERT INTO public.temporary_restock_recipient_lookup (
    recipient_hash, legal_business_name, duns, parent_duns,
    parent_legal_business_name, address_line_1, address_line_2, city,
     state, zip4, country_code,
    congressional_district)
SELECT
    recipient_hash, legal_business_name, duns, parent_duns,
    parent_legal_business_name, address_line_1, address_line_2, city,
    state, zip4, country_code,
    congressional_district
FROM transaction_recipients
ON CONFLICT (duns) DO NOTHING;


-- -- --------------------------------------------------------------------------------
-- -- -- Step 5, Get other recipients from FABS
-- -- --------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 5: Adding missing DUNS records from FABS'; END $$;
WITH transaction_recipients AS (
    WITH transaction_recipients_inner AS (
      SELECT
        tf.awardee_or_recipient_uniqu,
        tf.ultimate_parent_legal_enti,
        tf.ultimate_parent_unique_ide,
        tf.awardee_or_recipient_legal,
        tf.legal_entity_city_name,
        tf.legal_entity_state_code,
        tf.legal_entity_zip5,
        tf.legal_entity_congressional,
        tf.legal_entity_address_line1,
        tf.legal_entity_address_line2,
        tf.legal_entity_country_code
      FROM transaction_fabs AS tf
      WHERE tf.awardee_or_recipient_uniqu IS NOT NULL
      ORDER BY tf.action_date DESC
    )
    SELECT
      DISTINCT awardee_or_recipient_uniqu,
      MD5(UPPER(CONCAT(awardee_or_recipient_uniqu, awardee_or_recipient_legal)))::uuid AS recipient_hash,
      awardee_or_recipient_uniqu AS duns,
      ultimate_parent_legal_enti AS parent_duns,
      ultimate_parent_unique_ide AS parent_legal_business_name,
      awardee_or_recipient_legal AS legal_business_name,
      legal_entity_city_name AS city,
      legal_entity_state_code AS state,
      legal_entity_zip5 AS zip,
      legal_entity_congressional AS congressional_district,
      legal_entity_address_line1 AS address_line_1,
      legal_entity_address_line2 AS address_line_2,
      legal_entity_country_code AS country_code
    FROM transaction_recipients_inner
)

INSERT INTO public.temporary_restock_recipient_lookup (
    recipient_hash, legal_business_name, duns, parent_duns,
    parent_legal_business_name, address_line_1, address_line_2, city,
     state, zip, country_code,
    congressional_district)
SELECT
    recipient_hash, legal_business_name, duns, parent_duns,
    parent_legal_business_name, address_line_1, address_line_2, city,
    state, zip, country_code,
    congressional_district
FROM transaction_recipients
ON CONFLICT (duns) DO NOTHING;



-- --------------------------------------------------------------------------------
-- -- Step 6, Get all recipients without DUNS from transactions (fabs/fpds) 12 minutes
-- --------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 6: Get all recipients without DUNS from transactions (fabs/fpds)'; END $$;

DROP INDEX IF EXISTS idx_temporary_duns_unique_duns;

WITH transactions AS (
    WITH transactions_inner AS (
      (SELECT
        tf.awardee_or_recipient_uniqu,
        UPPER(tf.ultimate_parent_legal_enti) AS ultimate_parent_legal_enti,
        tf.ultimate_parent_unique_ide,
        UPPER(tf.awardee_or_recipient_legal) AS awardee_or_recipient_legal,
        tf.legal_entity_city_name,
        tf.legal_entity_state_code,
        tf.legal_entity_zip5 AS zip,
        tf.legal_entity_congressional,
        tf.legal_entity_address_line1,
        tf.legal_entity_address_line2,
        tf.legal_entity_country_code
      FROM transaction_fabs AS tf
      WHERE tf.awardee_or_recipient_uniqu IS NULL
      ORDER BY tf.action_date DESC)
    UNION
    (SELECT
        tf.awardee_or_recipient_uniqu,
        UPPER(tf.ultimate_parent_legal_enti) AS ultimate_parent_legal_enti,
        tf.ultimate_parent_unique_ide,
        UPPER(tf.awardee_or_recipient_legal) AS awardee_or_recipient_legal,
        tf.legal_entity_city_name,
        tf.legal_entity_state_code,
        tf.legal_entity_zip4 AS zip,
        tf.legal_entity_congressional,
        tf.legal_entity_address_line1,
        tf.legal_entity_address_line2,
        tf.legal_entity_country_code
      FROM transaction_fpds AS tf
      WHERE tf.awardee_or_recipient_uniqu IS NULL
      ORDER BY tf.action_date DESC)
  )
  SELECT
    DISTINCT awardee_or_recipient_legal,
    awardee_or_recipient_uniqu AS duns,
    ultimate_parent_legal_enti AS parent_duns,
    ultimate_parent_unique_ide AS parent_legal_business_name,
    awardee_or_recipient_legal AS legal_business_name,
    legal_entity_city_name AS city,
    legal_entity_state_code AS state,
    zip,
    legal_entity_congressional AS congressional_district,
    legal_entity_address_line1 AS address_line_1,
    legal_entity_address_line2 AS address_line_2,
    legal_entity_country_code AS country_code
  FROM transactions_inner
)

INSERT INTO public.temporary_restock_recipient_lookup (
    recipient_hash, legal_business_name, duns, parent_duns,
    parent_legal_business_name, address_line_1, address_line_2, city,
     state, zip, country_code,
    congressional_district)
SELECT
    MD5(UPPER(CONCAT(duns, legal_business_name)))::uuid AS recipient_hash,
    legal_business_name, duns, parent_duns,
    parent_legal_business_name, address_line_1, address_line_2, city,
    state, zip, country_code,
    congressional_district
FROM transactions;


DO $$ BEGIN RAISE NOTICE 'Step 5: restocking destination table'; END $$;
BEGIN;
TRUNCATE TABLE public.recipient_lookup RESTART IDENTITY;
INSERT INTO public.recipient_lookup (recipient_hash, legal_business_name, duns) SELECT * FROM public.temporary_restock_recipient_lookup;
DROP TABLE public.temporary_restock_recipient_lookup;
COMMIT;

VACUUM ANALYZE VERBOSE public.recipient_lookup;
