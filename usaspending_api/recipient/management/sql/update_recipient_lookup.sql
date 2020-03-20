DROP TABLE IF EXISTS temporary_restock_recipient_lookup;
DROP MATERIALIZED VIEW IF EXISTS temporary_transaction_recipients_view;

--------------------------------------------------------------------------------
-- Step 1, Create temporary table and materialized view
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 1: Creating temporary table and materialized view'; END $$;

CREATE TABLE public.temporary_restock_recipient_lookup AS SELECT * FROM recipient_lookup limit 0;
CREATE UNIQUE INDEX recipient_lookup_new_recipient_hash ON public.temporary_restock_recipient_lookup(recipient_hash);

CREATE MATERIALIZED VIEW public.temporary_transaction_recipients_view AS (
  SELECT
    tn.transaction_unique_id,
    tn.is_fpds,
    MD5(UPPER(
      CASE
        WHEN COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu))
        ELSE CONCAT('name-', COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal, '')) END
    ))::uuid AS recipient_hash,
    COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) AS awardee_or_recipient_uniqu,
    UPPER(COALESCE(fpds.ultimate_parent_legal_enti, fabs.ultimate_parent_legal_enti)) AS ultimate_parent_legal_enti,
    COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide) AS ultimate_parent_unique_ide,
    UPPER(COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal)) AS awardee_or_recipient_legal,
    COALESCE(fpds.legal_entity_city_name, fabs.legal_entity_city_name) AS city,
    COALESCE(fpds.legal_entity_state_code, fabs.legal_entity_state_code) AS state,
    COALESCE(fpds.legal_entity_zip5, fabs.legal_entity_zip5) AS zip5,
    COALESCE(fpds.legal_entity_zip_last4, fabs.legal_entity_zip_last4) AS zip4,
    COALESCE(fpds.legal_entity_congressional, fabs.legal_entity_congressional) AS congressional_district,
    COALESCE(fpds.legal_entity_address_line1, fabs.legal_entity_address_line1) AS address_line_1,
    COALESCE(fpds.legal_entity_address_line2, fabs.legal_entity_address_line1) AS address_line_2,
    COALESCE(fpds.legal_entity_country_code, fabs.legal_entity_country_code) AS country_code,
    tn.action_date,
    CASE
      WHEN tn.is_fpds = TRUE THEN 'fpds'::TEXT
      ELSE 'fabs'::TEXT
      END AS source
  FROM
    transaction_normalized AS tn
  LEFT OUTER JOIN transaction_fpds AS fpds ON
    (tn.id = fpds.transaction_id)
  LEFT OUTER JOIN transaction_fabs AS fabs ON
    (tn.id = fabs.transaction_id)
  ORDER BY tn.action_date DESC
);

CREATE INDEX idx_temporary_restock_recipient_view ON public.temporary_transaction_recipients_view (awardee_or_recipient_uniqu, awardee_or_recipient_legal);
CREATE INDEX idx_temporary_restock_parent_recipient_view ON public.temporary_transaction_recipients_view (ultimate_parent_unique_ide, ultimate_parent_legal_enti);
ANALYZE public.temporary_transaction_recipients_view;

--------------------------------------------------------------------------------
-- Step 2a, Create rows from SAM
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 2a: Adding Recipient records from SAM'; END $$;

INSERT INTO public.temporary_restock_recipient_lookup (recipient_hash, legal_business_name, duns, source)
SELECT
  DISTINCT ON (awardee_or_recipient_uniqu, legal_business_name)
  MD5(UPPER(
    CASE
      WHEN awardee_or_recipient_uniqu IS NOT NULL THEN CONCAT('duns-', awardee_or_recipient_uniqu)
      ELSE CONCAT('name-', legal_business_name) END
    ))::uuid AS recipient_hash,
    UPPER(legal_business_name) AS legal_business_name,
    awardee_or_recipient_uniqu AS duns,
    'sam' as source
FROM duns
WHERE awardee_or_recipient_uniqu IS NOT NULL AND legal_business_name IS NOT NULL
  ORDER BY awardee_or_recipient_uniqu, legal_business_name, update_date DESC
ON CONFLICT (recipient_hash) DO NOTHING;


--------------------------------------------------------------------------------
-- Step 2b, Create rows with data from FPDS/FABS
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 2b: Adding Recipient records from FPDS and FABS'; END $$;
INSERT INTO public.temporary_restock_recipient_lookup (recipient_hash, legal_business_name, duns, source)
SELECT
  DISTINCT ON (recipient_hash)
    recipient_hash,
    UPPER(t.awardee_or_recipient_legal) AS legal_business_name,
    t.awardee_or_recipient_uniqu AS duns,
    t.source
FROM temporary_transaction_recipients_view t
  ORDER BY t.recipient_hash, action_date DESC, is_fpds, transaction_unique_id
ON CONFLICT (recipient_hash) DO NOTHING;



--------------------------------------------------------------------------------
-- Step 3a, Create rows with Parent DUNS + Parent Recipient Names from SAM
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 3a: Adding Recipient records from SAM parent data'; END $$;
INSERT INTO public.temporary_restock_recipient_lookup (recipient_hash, legal_business_name, duns, source, parent_duns, parent_legal_business_name, update_date)
SELECT
  DISTINCT ON (ultimate_parent_unique_ide, ultimate_parent_legal_enti)
  MD5(UPPER(
  CASE
    WHEN ultimate_parent_unique_ide IS NOT NULL THEN CONCAT('duns-', ultimate_parent_unique_ide)
    ELSE CONCAT('name-', ultimate_parent_legal_enti) END
  ))::uuid AS recipient_hash,
  UPPER(ultimate_parent_legal_enti) AS ultimate_parent_legal_enti,
  ultimate_parent_unique_ide AS duns,
  'parent-sam' as source,
  ultimate_parent_unique_ide AS parent_duns,
  ultimate_parent_legal_enti AS parent_legal_business_name,
  now() AS update_date
FROM duns
WHERE ultimate_parent_unique_ide IS NOT NULL OR ultimate_parent_legal_enti IS NOT NULL
  ORDER BY ultimate_parent_unique_ide, ultimate_parent_legal_enti, update_date DESC
ON CONFLICT (recipient_hash) DO NOTHING;

--------------------------------------------------------------------------------
-- Step 3b, Create rows with Parent Recipient details from FPDS/FABS
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 3b: Adding Recipient records from FPDS and FABS parents'; END $$;
INSERT INTO public.temporary_restock_recipient_lookup (recipient_hash, legal_business_name, duns, source, parent_duns, parent_legal_business_name, update_date)
  SELECT
    DISTINCT ON (tf.ultimate_parent_unique_ide, tf.ultimate_parent_legal_enti)
    MD5(UPPER(
      CASE
        WHEN tf.ultimate_parent_unique_ide IS NOT NULL THEN CONCAT('duns-', tf.ultimate_parent_unique_ide)
        ELSE CONCAT('name-', tf.ultimate_parent_legal_enti) END
    ))::uuid AS recipient_hash,
    tf.ultimate_parent_legal_enti,
    tf.ultimate_parent_unique_ide,
    CONCAT('parent-', tf.source),
    tf.ultimate_parent_unique_ide AS parent_duns,
    tf.ultimate_parent_legal_enti AS parent_legal_business_name,
    now() AS update_date
  FROM public.temporary_transaction_recipients_view AS tf
  WHERE tf.ultimate_parent_unique_ide IS NOT NULL AND tf.ultimate_parent_legal_enti IS NOT NULL
  ORDER BY tf.ultimate_parent_unique_ide, tf.ultimate_parent_legal_enti, tf.action_date DESC, tf.is_fpds, tf.transaction_unique_id
ON CONFLICT (recipient_hash) DO NOTHING;


--------------------------------------------------------------------------------
-- Step 4a, Update rows with details from SAM
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 4a: Update Recipient records from SAM'; END $$;
UPDATE public.temporary_restock_recipient_lookup rl
SET
    address_line_1              = d.address_line_1,
    address_line_2              = d.address_line_2,
    business_types_codes        = d.business_types_codes,
    city                        = d.city,
    congressional_district      = d.congressional_district,
    country_code                = d.country_code,
    legal_business_name         = UPPER(d.legal_business_name),
    parent_duns                 = d.ultimate_parent_unique_ide,
    parent_legal_business_name  = UPPER(d.ultimate_parent_legal_enti),
    source                      = 'sam',
    state                       = d.state,
    update_date                 = now(),
    zip4                        = d.zip4,
    zip5                        = d.zip
FROM duns d
WHERE
    rl.duns = d.awardee_or_recipient_uniqu
    AND (
         rl.address_line_1              IS DISTINCT FROM d.address_line_1
      OR rl.address_line_2              IS DISTINCT FROM d.address_line_2
      OR rl.business_types_codes        IS DISTINCT FROM d.business_types_codes
      OR rl.city                        IS DISTINCT FROM d.city
      OR rl.congressional_district      IS DISTINCT FROM d.congressional_district
      OR rl.country_code                IS DISTINCT FROM d.country_code
      OR rl.legal_business_name         IS DISTINCT FROM UPPER(d.legal_business_name)
      OR rl.parent_duns                 IS DISTINCT FROM d.ultimate_parent_unique_ide
      OR rl.parent_legal_business_name  IS DISTINCT FROM UPPER(d.ultimate_parent_legal_enti)
      or rl.state                       IS DISTINCT FROM d.state
      OR rl.zip4                        IS DISTINCT FROM d.zip4
      OR rl.zip5                        IS DISTINCT FROM d.zip
    );

--------------------------------------------------------------------------------
-- Step 4b, Update rows with details from transactions
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 4b: Update Recipient records from transactions'; END $$;
UPDATE public.temporary_restock_recipient_lookup rl
SET
    address_line_1              = t.address_line_1,
    address_line_2              = t.address_line_2,
    business_types_codes        = NULL,
    city                        = t.city,
    congressional_district      = t.congressional_district,
    country_code                = t.country_code,
    legal_business_name         = UPPER(t.awardee_or_recipient_legal),
    parent_duns                 = t.ultimate_parent_unique_ide,
    parent_legal_business_name  = UPPER(t.ultimate_parent_legal_enti),
    source                      = t.source,
    state                       = t.state,
    update_date                 = now(),
    zip4                        = t.zip4,
    zip5                        = t.zip5
FROM temporary_transaction_recipients_view t
WHERE
    rl.recipient_hash = t.recipient_hash AND rl.source != 'sam'
    AND (
         rl.address_line_1              IS DISTINCT FROM t.address_line_1
      OR rl.address_line_2              IS DISTINCT FROM t.address_line_2
      OR rl.business_types_codes        IS DISTINCT FROM NULL
      OR rl.city                        IS DISTINCT FROM t.city
      OR rl.congressional_district      IS DISTINCT FROM t.congressional_district
      OR rl.country_code                IS DISTINCT FROM t.country_code
      OR rl.legal_business_name         IS DISTINCT FROM UPPER(t.awardee_or_recipient_legal)
      OR rl.parent_duns                 IS DISTINCT FROM t.ultimate_parent_unique_ide
      OR rl.parent_legal_business_name  IS DISTINCT FROM UPPER(t.ultimate_parent_legal_enti)
      or rl.state                       IS DISTINCT FROM t.state
      OR rl.zip4                        IS DISTINCT FROM t.zip4
      OR rl.zip5                        IS DISTINCT FROM t.zip5
    );

--------------------------------------------------------------------------------
-- Step 5, Finalizing
--------------------------------------------------------------------------------
VACUUM ANALYZE public.temporary_restock_recipient_lookup;
DO $$ BEGIN RAISE NOTICE 'Step 5: Updating recipient_lookup'; END $$;
BEGIN;

WITH removed_recipients AS (
  SELECT
    rl.recipient_hash
    FROM public.recipient_lookup rl
    LEFT JOIN public.temporary_restock_recipient_lookup tem ON rl.recipient_hash = tem.recipient_hash
    WHERE tem.recipient_hash IS NULL
)
DELETE FROM public.recipient_lookup rl WHERE rl.recipient_hash IN (SELECT recipient_hash FROM removed_recipients)
-- RETURNING rl.recipient_hash
;

UPDATE public.recipient_lookup rl SET
    address_line_1          = tem.address_line_1,
    address_line_2          = tem.address_line_2,
    business_types_codes    = tem.business_types_codes,
    city                    = tem.city,
    congressional_district  = tem.congressional_district,
    country_code            = tem.country_code,
    duns                    = tem.duns,
    legal_business_name     = tem.legal_business_name,
    recipient_hash          = tem.recipient_hash,
    source                  = tem.source,
    state                   = tem.state,
    update_date             = tem.update_date,
    zip4                    = tem.zip4,
    zip5                    = tem.zip5
  FROM public.temporary_restock_recipient_lookup tem
  WHERE
    rl.recipient_hash = tem.recipient_hash
    AND (
       rl.address_line_1          IS DISTINCT FROM tem.address_line_1
    OR rl.address_line_2          IS DISTINCT FROM tem.address_line_2
    OR rl.business_types_codes    IS DISTINCT FROM tem.business_types_codes
    OR rl.city                    IS DISTINCT FROM tem.city
    OR rl.congressional_district  IS DISTINCT FROM tem.congressional_district
    OR rl.country_code            IS DISTINCT FROM tem.country_code
    OR rl.duns                    IS DISTINCT FROM tem.duns
    OR rl.legal_business_name     IS DISTINCT FROM tem.legal_business_name
    OR rl.recipient_hash          IS DISTINCT FROM tem.recipient_hash
    OR rl.source                  IS DISTINCT FROM tem.source
    OR rl.state                   IS DISTINCT FROM tem.state
    OR rl.zip4                    IS DISTINCT FROM tem.zip4
    OR rl.zip5                    IS DISTINCT FROM tem.zip5
  );

INSERT INTO public.recipient_lookup (
    recipient_hash, legal_business_name, duns, address_line_1, address_line_2,
    city, state, zip5, zip4, country_code,
    congressional_district, business_types_codes, source, update_date)
  SELECT
    recipient_hash, legal_business_name, duns, address_line_1, address_line_2,
    city, state, zip5, zip4, country_code,
    congressional_district, business_types_codes, source, update_date
  FROM public.temporary_restock_recipient_lookup tem
  ON CONFLICT(recipient_hash) DO NOTHING;

WITH alternate_names AS (
      SELECT recipient_hash, array_agg(DISTINCT legal_business_name) as all_names
      FROM temporary_restock_recipient_lookup
      WHERE legal_business_name IS NOT NULL
      GROUP BY recipient_hash
)
UPDATE public.recipient_lookup rl SET
  alternate_names = all_names
FROM alternate_names an
WHERE rl.recipient_hash = an.recipient_hash AND alternate_names IS DISTINCT FROM all_names;

DROP TABLE public.temporary_restock_recipient_lookup;
DROP MATERIALIZED VIEW IF EXISTS public.temporary_transaction_recipients_view;
COMMIT;

VACUUM ANALYZE public.recipient_lookup;