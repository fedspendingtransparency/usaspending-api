DO $$ BEGIN RAISE NOTICE '040 Adding Recipient records from FPDS and FABS parents'; END $$;
INSERT INTO public.temporary_restock_recipient_lookup (
  recipient_hash,
  legal_business_name,
  duns,
  source,
  parent_duns,
  parent_legal_business_name
)
SELECT
  DISTINCT ON (tf.ultimate_parent_unique_ide, tf.ultimate_parent_legal_enti)
  MD5(UPPER(
    CASE
      WHEN tf.ultimate_parent_unique_ide IS NOT NULL THEN CONCAT('duns-', tf.ultimate_parent_unique_ide)
      ELSE CONCAT('name-', tf.ultimate_parent_legal_enti) END
  ))::uuid AS recipient_hash,
  tf.ultimate_parent_legal_enti,
  tf.ultimate_parent_unique_ide,
  CONCAT(tf.source, 'parent-'),
  tf.ultimate_parent_unique_ide AS parent_duns,
  tf.ultimate_parent_legal_enti AS parent_legal_business_name
FROM public.temporary_transaction_recipients_view AS tf
WHERE tf.ultimate_parent_unique_ide IS NOT NULL AND tf.ultimate_parent_legal_enti IS NOT NULL
ORDER BY tf.ultimate_parent_unique_ide, tf.ultimate_parent_legal_enti, tf.action_date DESC, tf.is_fpds, tf.transaction_unique_id
ON CONFLICT (recipient_hash) DO NOTHING;
