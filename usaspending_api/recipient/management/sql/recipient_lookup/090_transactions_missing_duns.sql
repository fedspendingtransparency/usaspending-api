DO $$ BEGIN RAISE NOTICE '090 Adding Recipient records without DUNS from FPDS and FABS'; END $$;
INSERT INTO public.temporary_restock_recipient_lookup (
  recipient_hash,
  legal_business_name,
  duns,
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
  zip5
)
SELECT
  DISTINCT ON (recipient_hash)
  recipient_hash,
  UPPER(t.awardee_or_recipient_legal) AS legal_business_name,
  t.awardee_or_recipient_uniqu AS duns,
  t.source,
  address_line_1,
  address_line_2,
  city,
  congressional_district,
  country_code,
  ultimate_parent_unique_ide,
  UPPER(ultimate_parent_legal_enti) AS parent_legal_business_name,
  state,
  zip4,
  zip5
FROM temporary_transaction_recipients_view t
WHERE t.awardee_or_recipient_uniqu IS NULL
ORDER BY t.recipient_hash, action_date DESC, is_fpds, transaction_unique_id
ON CONFLICT (recipient_hash) DO NOTHING;
