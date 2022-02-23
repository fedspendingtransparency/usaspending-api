DO $$ BEGIN RAISE NOTICE '060 Adding Recipient records from FPDS and FABS without a name'; END $$;

INSERT INTO public.temporary_restock_recipient_lookup (
  recipient_hash,
  legal_business_name,
  duns,
  uei,
  source,
  address_line_1,
  address_line_2,
  city,
  congressional_district,
  country_code,
  parent_duns,
  parent_legal_business_name,
  state,
  zip4,
  zip5,
  duns_recipient_hash
)
SELECT
  DISTINCT ON (recipient_hash)
  recipient_hash,
  awardee_or_recipient_legal,
  awardee_or_recipient_uniqu,
  uei,
  source,
  address_line_1,
  address_line_2,
  city,
  congressional_district,
  country_code,
  ultimate_parent_unique_ide,
  ultimate_parent_legal_enti,
  state,
  zip4,
  zip5,
  duns_recipient_hash
FROM temporary_transaction_recipients_view
WHERE awardee_or_recipient_uniqu IS NOT NULL AND awardee_or_recipient_legal IS NULL
ORDER BY recipient_hash, action_date DESC, is_fpds, transaction_unique_id
ON CONFLICT (recipient_hash) DO NOTHING;
