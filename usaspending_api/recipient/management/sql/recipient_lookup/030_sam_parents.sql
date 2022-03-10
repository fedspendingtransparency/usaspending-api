DO $$ BEGIN RAISE NOTICE '030 Adding Recipient records from SAM parent data'; END $$;

INSERT INTO public.temporary_restock_recipient_lookup (
  recipient_hash,
  duns_recipient_hash,
  legal_business_name,
  duns,
  uei,
  source,
  parent_duns,
  parent_legal_business_name,
  parent_uei
)
SELECT
  DISTINCT ON (ultimate_parent_unique_ide, ultimate_parent_uei)
  MD5(UPPER(
    CASE WHEN ultimate_parent_uei IS NOT NULL THEN CONCAT('uei-', ultimate_parent_uei)
    ELSE CONCAT('duns-', ultimate_parent_unique_ide) END
  ))::uuid AS recipient_hash,
  MD5(UPPER(CONCAT('duns-', ultimate_parent_unique_ide)
  ))::uuid AS duns_recipient_hash,
  UPPER(ultimate_parent_legal_enti) AS legal_business_name,
  ultimate_parent_unique_ide AS duns,
  ultimate_parent_uei as uei,
  'sam-parent' as source,
  ultimate_parent_unique_ide,
  UPPER(ultimate_parent_legal_enti) AS parent_legal_business_name,
  ultimate_parent_uei AS parent_uei
FROM duns
WHERE COALESCE(ultimate_parent_uei, ultimate_parent_unique_ide) IS NOT NULL AND ultimate_parent_legal_enti IS NOT NULL
ORDER BY ultimate_parent_unique_ide, ultimate_parent_uei, update_date DESC
ON CONFLICT (recipient_hash) DO NOTHING;
