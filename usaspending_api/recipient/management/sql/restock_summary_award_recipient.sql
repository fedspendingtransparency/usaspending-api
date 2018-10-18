--------------------------------------------------------------------------------
-- Step 1, create the temporary matview of awards and transactions with their earliest action_date
--------------------------------------------------------------------------------
DROP TABLE IF EXISTS summary_award_recipient_temp CASCADE;
DROP TABLE IF EXISTS summary_award_recipient_old CASCADE;
DROP TABLE IF EXISTS earliest_transaction CASCADE;

DO $$ BEGIN RAISE NOTICE 'Step 1: Create temp earliest action date'; END $$;

CREATE TABLE earliest_transaction AS (
  WITH lowest_action_date AS (
      SELECT
        award_id,
        MIN(action_date) AS min_date
      FROM transaction_normalized
      WHERE action_date >= '2007-10-01'
      GROUP BY award_id
  )
  SELECT
    txn.award_id,
    txn.id,
    txn.action_date,
    txn.is_fpds
  FROM lowest_action_date AS lad
  JOIN transaction_normalized AS txn ON lad.award_id=txn.award_id AND lad.min_date = txn.action_date
);

--------------------------------------------------------------------------------
-- Step 2, create summary_award_recipient using txn_earliest_temp
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 2: Create summary_award_recipient'; END $$;

CREATE TABLE summary_award_recipient_temp AS (
  SELECT
  earliest_transaction.award_id,
  earliest_transaction.action_date,
  COALESCE(recipient_lookup.recipient_hash, MD5(
    UPPER(COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)))::uuid
  )::uuid AS recipient_hash,
  COALESCE(transaction_fpds.ultimate_parent_unique_ide, transaction_fabs.ultimate_parent_unique_ide) AS parent_recipient_unique_id

  FROM earliest_transaction
  LEFT OUTER JOIN transaction_fabs ON (earliest_transaction.is_fpds = false AND earliest_transaction.id = transaction_fabs.transaction_id)
  LEFT OUTER JOIN transaction_fpds ON (earliest_transaction.is_fpds = true AND earliest_transaction.id = transaction_fpds.transaction_id)
  LEFT OUTER JOIN
  (SELECT
    recipient_hash,
    legal_business_name AS recipient_name,
    duns
  FROM recipient_lookup AS rlv
  ) recipient_lookup ON (
    recipient_lookup.duns = COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) AND
    COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL
  )
);

--------------------------------------------------------------------------------
-- Step 3, create indexes on our new summary_award_recipient
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 3: Creating indexes on summary_award_recipient'; END $$;

CREATE INDEX idx_25fc29e1$aca_award_id ON summary_award_recipient_temp USING BTREE(award_id) WITH (fillfactor = 97);
CREATE INDEX idx_25fc29e1$aca_action_date_temp ON summary_award_recipient_temp USING BTREE(action_date DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_25fc29e1$aca_recipient_hash_temp ON summary_award_recipient_temp USING BTREE(recipient_hash) WITH (fillfactor = 97);
CREATE INDEX idx_25fc29e1$aca_parent_recipient_unique_id_temp ON summary_award_recipient_temp USING BTREE(parent_recipient_unique_id) WITH (fillfactor = 97);

--------------------------------------------------------------------------------
-- Step 4, Cleanup, renaming, drop temp table
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 4: Cleaning up'; END $$;

ALTER TABLE IF EXISTS summary_award_recipient RENAME TO summary_award_recipient_old;
ALTER INDEX IF EXISTS idx_25fc29e1$aca_action_date RENAME TO idx_25fc29e1$aca_action_date_old;
ALTER INDEX IF EXISTS idx_25fc29e1$aca_recipient_hash RENAME TO idx_25fc29e1$aca_recipient_hash_old;
ALTER INDEX IF EXISTS idx_25fc29e1$aca_parent_recipient_unique_id RENAME TO idx_25fc29e1$aca_parent_recipient_unique_id_old;

ALTER TABLE summary_award_recipient_temp RENAME TO summary_award_recipient;
ALTER INDEX idx_25fc29e1$aca_action_date_temp RENAME TO idx_25fc29e1$aca_action_date;
ALTER INDEX idx_25fc29e1$aca_recipient_hash_temp RENAME TO idx_25fc29e1$aca_recipient_hash;
ALTER INDEX idx_25fc29e1$aca_parent_recipient_unique_id_temp RENAME TO idx_25fc29e1$aca_parent_recipient_unique_id;

DROP TABLE IF EXISTS earliest_transaction;

ANALYZE VERBOSE summary_award_recipient;
GRANT SELECT ON summary_award_recipient TO readonly;