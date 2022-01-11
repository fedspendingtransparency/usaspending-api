DO $$ BEGIN RAISE NOTICE '030 Adding Recipient records from SAM parent data'; END $$;

INSERT INTO public.temporary_restock_recipient_lookup (
  recipient_hash,
  legal_business_name,
  duns,
  uei,
  source,
  parent_duns,
  parent_legal_business_name
)
SELECT
  DISTINCT ON (ultimate_parent_unique_ide, ultimate_parent_legal_enti)
  MD5(UPPER(
    CASE WHEN awardee_or_recipient_uniqu IS NOT NULL THEN CONCAT('duns-', awardee_or_recipient_uniqu)
    ELSE CONCAT('uei-', uei) END
  ))::uuid AS recipient_hash,
  UPPER(ultimate_parent_legal_enti) AS legal_business_name,
  ultimate_parent_unique_ide AS duns,
  ultimate_parent_uei as uei,
  'sam-parent' as source,
  ultimate_parent_unique_ide,
  UPPER(ultimate_parent_legal_enti) AS parent_legal_business_name
FROM duns
WHERE ultimate_parent_unique_ide IS NOT NULL AND ultimate_parent_legal_enti IS NOT NULL
ORDER BY ultimate_parent_unique_ide, ultimate_parent_legal_enti, update_date DESC
ON CONFLICT (recipient_hash) DO NOTHING;
