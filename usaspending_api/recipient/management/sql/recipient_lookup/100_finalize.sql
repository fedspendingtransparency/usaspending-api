DO $$ BEGIN RAISE NOTICE '100 Loading recipient_lookup and cleaning up'; END $$;

DO $$ BEGIN RAISE NOTICE 'Removing stale records from recipient_lookup'; END $$;
BEGIN;
WITH removed_recipients AS (
  SELECT
    rl.recipient_hash
    FROM rpt.recipient_lookup rl
    LEFT JOIN public.temporary_restock_recipient_lookup tem ON rl.recipient_hash = tem.recipient_hash
    WHERE tem.recipient_hash IS NULL
)
DELETE FROM rpt.recipient_lookup rl WHERE rl.recipient_hash IN (SELECT recipient_hash FROM removed_recipients)
RETURNING rl.recipient_hash;


DO $$ BEGIN RAISE NOTICE 'Updating records in recipient_lookup'; END $$;
UPDATE rpt.recipient_lookup rl SET
    address_line_1              = tem.address_line_1,
    address_line_2              = tem.address_line_2,
    business_types_codes        = tem.business_types_codes,
    city                        = tem.city,
    congressional_district      = tem.congressional_district,
    country_code                = tem.country_code,
    duns                        = tem.duns,
    uei                         = tem.uei,
    legal_business_name         = tem.legal_business_name,
    parent_duns                 = tem.parent_duns,
    parent_legal_business_name  = tem.parent_legal_business_name,
    parent_uei                  = tem.parent_uei,
    recipient_hash              = tem.recipient_hash,
    source                      = tem.source,
    state                       = tem.state,
    update_date                 = now(),
    zip4                        = tem.zip4,
    zip5                        = tem.zip5
  FROM public.temporary_restock_recipient_lookup tem
  WHERE
    rl.recipient_hash = tem.recipient_hash
    AND (
        rl.address_line_1                 IS DISTINCT FROM tem.address_line_1
        OR rl.address_line_2              IS DISTINCT FROM tem.address_line_2
        OR rl.business_types_codes        IS DISTINCT FROM tem.business_types_codes
        OR rl.city                        IS DISTINCT FROM tem.city
        OR rl.congressional_district      IS DISTINCT FROM tem.congressional_district
        OR rl.country_code                IS DISTINCT FROM tem.country_code
        OR rl.duns                        IS DISTINCT FROM tem.duns
        OR rl.uei                         IS DISTINCT FROM tem.uei
        OR rl.legal_business_name         IS DISTINCT FROM tem.legal_business_name
        OR rl.parent_duns                 IS DISTINCT FROM tem.parent_duns
        OR rl.parent_legal_business_name  IS DISTINCT FROM tem.parent_legal_business_name
        OR rl.parent_uei                  IS DISTINCT FROM tem.parent_uei
        OR rl.recipient_hash              IS DISTINCT FROM tem.recipient_hash
        OR rl.source                      IS DISTINCT FROM tem.source
        OR rl.state                       IS DISTINCT FROM tem.state
        OR rl.zip4                        IS DISTINCT FROM tem.zip4
        OR rl.zip5                        IS DISTINCT FROM tem.zip5
  );

DO $$ BEGIN RAISE NOTICE 'Inserting new records into recipient_lookup'; END $$;
INSERT INTO rpt.recipient_lookup (
  recipient_hash,
  legal_business_name,
  duns,
  uei,
  address_line_1,
  address_line_2,
  city,
  state,
  zip5,
  zip4,
  country_code,
  congressional_district,
  business_types_codes,
  source,
  parent_duns,
  parent_legal_business_name,
  parent_uei,
  alternate_names,
  update_date
)
SELECT
  recipient_hash,
  legal_business_name,
  duns,
  uei,
  address_line_1,
  address_line_2,
  city,
  state,
  zip5,
  zip4,
  country_code,
  congressional_district,
  business_types_codes,
  source,
  parent_duns,
  parent_legal_business_name,
  parent_uei,
  '{}',
  now()
FROM public.temporary_restock_recipient_lookup
ON CONFLICT(recipient_hash) DO NOTHING;

DO $$ BEGIN RAISE NOTICE 'Deleting duplicate DUNS records that are superceded by records with UEI populated.'; END $$;

DELETE FROM rpt.recipient_lookup WHERE recipient_hash IN (
    SELECT rl.recipient_hash
    FROM recipient_lookup rl
        INNER JOIN public.temporary_restock_recipient_lookup tem
            ON rl.recipient_hash = tem.duns_recipient_hash AND tem.uei IS NOT NULL AND tem.duns IS NOT NULL AND rl.uei IS NULL
) RETURNING recipient_hash;

DO $$ BEGIN RAISE NOTICE 'Populating alternate_names in recipient_lookup'; END $$;
WITH alternate_names AS (
  WITH alt_names AS (
    SELECT
      recipient_hash,
      array_agg(DISTINCT awardee_or_recipient_legal) as all_names
    FROM temporary_transaction_recipients_view
    WHERE COALESCE(awardee_or_recipient_legal, '') != ''
    GROUP BY recipient_hash
  ), alt_parent_names AS (
    SELECT
      parent_recipient_hash AS recipient_hash,
      array_agg(DISTINCT ultimate_parent_legal_enti) as all_names
    FROM temporary_transaction_recipients_view
    WHERE COALESCE(ultimate_parent_legal_enti, '') != ''
    GROUP BY parent_recipient_hash
  )
  SELECT
    COALESCE(an.recipient_hash, apn.recipient_hash) as recipient_hash,
    (
      SELECT array_agg(merged_recipient_names)
      FROM (
        SELECT DISTINCT unnest(an.all_names || apn.all_names) as merged_recipient_names
      ) as merged_arrays
    ) as all_names
  FROM alt_names an
  FULL OUTER JOIN alt_parent_names apn ON an.recipient_hash = apn.recipient_hash
)
UPDATE rpt.recipient_lookup rl SET
  alternate_names = array_remove(an.all_names, rl.legal_business_name)
FROM alternate_names an
WHERE
      rl.recipient_hash = an.recipient_hash
  AND rl.alternate_names IS DISTINCT FROM array_remove(an.all_names, rl.legal_business_name);

DO $$ BEGIN RAISE NOTICE 'Post ETL clean up'; END $$;
DROP TABLE public.temporary_restock_recipient_lookup;
DROP MATERIALIZED VIEW IF EXISTS public.temporary_transaction_recipients_view;

COMMIT;
