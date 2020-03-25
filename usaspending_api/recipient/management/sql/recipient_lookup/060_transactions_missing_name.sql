DO $$ BEGIN RAISE NOTICE 'Adding Recipient records from FPDS and FABS without a name'; END $$;
INSERT INTO public.temporary_restock_recipient_lookup (recipient_hash, legal_business_name, duns, source)
SELECT
  DISTINCT ON (recipient_hash)
    recipient_hash,
    UPPER(t.awardee_or_recipient_legal) AS legal_business_name,
    t.awardee_or_recipient_uniqu AS duns,
    CONCAT('partial-', t.source)
FROM temporary_transaction_recipients_view t
WHERE t.awardee_or_recipient_uniqu IS NOT NULL AND t.awardee_or_recipient_legal IS NULL
ORDER BY t.recipient_hash, action_date DESC, is_fpds, transaction_unique_id
ON CONFLICT (recipient_hash) DO NOTHING;
