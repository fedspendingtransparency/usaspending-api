--------------------------------------------------------------------------------
-- Step 1, Restock summary_award_recipient
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 1: Restock summary_award_recipient'; END $$;
SELECT now();

DELETE FROM public.summary_award_recipient sar
WHERE NOT EXISTS (SELECT FROM awards a WHERE sar.award_id = a.id);

UPDATE public.summary_award_recipient AS sar SET
  action_date = a.date_signed,
  recipient_hash = MD5(UPPER(
    CASE
      WHEN COALESCE(fpds.awardee_or_recipient_uei, fabs.uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(fpds.awardee_or_recipient_uei, fabs.uei))
      WHEN COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu))
      ELSE CONCAT('name-', COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal)) END
    )
  )::uuid,
  parent_uei = COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei)
FROM public.awards a
LEFT OUTER JOIN int.transaction_fpds fpds ON (a.earliest_transaction_id = fpds.transaction_id)
LEFT OUTER JOIN int.transaction_fabs fabs ON (a.earliest_transaction_id = fabs.transaction_id)
WHERE
  sar.award_id = a.id
  AND (
    sar.action_date IS DISTINCT FROM a.date_signed
    OR sar.recipient_hash IS DISTINCT FROM MD5(
      UPPER(
        CASE
          WHEN COALESCE(fpds.awardee_or_recipient_uei, fabs.uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(fpds.awardee_or_recipient_uei, fabs.uei))
          WHEN COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu))
          ELSE CONCAT('name-', COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal)) END
        )
      )::uuid
    OR sar.parent_uei IS DISTINCT FROM COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei)
);


INSERT INTO public.summary_award_recipient
  (award_id, action_date, recipient_hash, parent_uei)
SELECT
    a.id AS award_id,
    a.date_signed AS action_date,
    MD5(UPPER(
      CASE
        WHEN COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu))
        WHEN COALESCE(fpds.awardee_or_recipient_uei, fabs.uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(fpds.awardee_or_recipient_uei, fabs.uei))
        ELSE CONCAT('name-', COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal)) END
      )
    )::uuid AS recipient_hash,
    COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei) AS parent_uei
FROM public.awards a
LEFT OUTER JOIN int.transaction_fpds fpds ON (a.earliest_transaction_id = fpds.transaction_id)
LEFT OUTER JOIN int.transaction_fabs fabs ON (a.earliest_transaction_id = fabs.transaction_id)
WHERE a.date_signed >= '2007-10-01'
ON CONFLICT(award_id) DO NOTHING;
