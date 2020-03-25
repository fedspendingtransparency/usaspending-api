DO $$ BEGIN RAISE NOTICE 'Adding Recipient records from SAM parent data with no name'; END $$;
INSERT INTO public.temporary_restock_recipient_lookup (recipient_hash, legal_business_name, duns, source, parent_duns, parent_legal_business_name, update_date)
SELECT
  DISTINCT ON (ultimate_parent_unique_ide)
  MD5(UPPER(
  CASE
    WHEN ultimate_parent_unique_ide IS NOT NULL THEN CONCAT('duns-', ultimate_parent_unique_ide)
    ELSE CONCAT('name-', ultimate_parent_legal_enti) END
  ))::uuid AS recipient_hash,
  UPPER(ultimate_parent_legal_enti) AS ultimate_parent_legal_enti,
  ultimate_parent_unique_ide AS duns,
  'sam-parent' as source,
  ultimate_parent_unique_ide AS parent_duns,
  ultimate_parent_legal_enti AS parent_legal_business_name,
  now() AS update_date
FROM duns
WHERE ultimate_parent_unique_ide IS NOT NULL
  ORDER BY ultimate_parent_unique_ide, ultimate_parent_legal_enti, update_date DESC
ON CONFLICT (recipient_hash) DO NOTHING;
