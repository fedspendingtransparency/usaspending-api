DO $$ BEGIN RAISE NOTICE '050 Adding Recipient records from SAM without a name'; END $$;

INSERT INTO public.temporary_restock_recipient_lookup (
  recipient_hash,
  duns_recipient_hash,
  legal_business_name,
  duns,
  uei,
  source,
  address_line_1,
  address_line_2,
  business_types_codes,
  city,
  congressional_district,
  country_code,
  parent_duns,
  parent_legal_business_name,
  state,
  zip4,
  zip5
)
SELECT
  DISTINCT ON (awardee_or_recipient_uniqu, legal_business_name)
  MD5(UPPER(
    CASE WHEN awardee_or_recipient_uniqu IS NOT NULL THEN CONCAT('duns-', awardee_or_recipient_uniqu)
    ELSE CONCAT('uei-', uei) END
  ))::uuid AS recipient_hash,
  MD5(UPPER(CONCAT('duns-', awardee_or_recipient_uniqu)
  ))::uuid AS duns_recipient_hash,
  UPPER(legal_business_name) AS legal_business_name,
  awardee_or_recipient_uniqu AS duns,
  uei,
  'sam' as source,
  address_line_1,
  address_line_2,
  business_types_codes,
  city,
  congressional_district,
  country_code,
  ultimate_parent_unique_ide,
  UPPER(ultimate_parent_legal_enti) AS parent_legal_business_name,
  state,
  zip4,
  zip AS zip5
FROM duns
WHERE awardee_or_recipient_uniqu IS NOT NULL AND legal_business_name IS NULL
ORDER BY awardee_or_recipient_uniqu, legal_business_name, update_date DESC
ON CONFLICT (recipient_hash) DO NOTHING;
