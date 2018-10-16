--------------------------------------------------------------------------------
-- Step 1, create the temporary matview of awards and transactions with their earliest action_date
--------------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS summary_award_recipient_view_temp CASCADE;
DROP MATERIALIZED VIEW IF EXISTS summary_award_recipient_view_old CASCADE;
DROP MATERIALIZED VIEW IF EXISTS txn_earliest_temp CASCADE;

DO $$ BEGIN RAISE NOTICE 'Step 1: Create temp earliest action date view'; END $$;

CREATE MATERIALIZED VIEW txn_earliest_temp AS (
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
-- Step 2, create possibly helpful index on our new massive table
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 2: Create indexes on txn_earliest_temp'; END $$;

--CREATE INDEX idx_25fc29e1$aca_award_id ON txn_earliest_temp USING BTREE(award_id) WITH (fillfactor = 97);
--CREATE INDEX idx_25fc29e1$aca_transaction_id ON txn_earliest_temp USING BTREE(id) WITH (fillfactor = 97);
--CREATE INDEX idx_25fc29e1$aca_action_date ON txn_earliest_temp USING BTREE(action_date) WITH (fillfactor = 97);
--CREATE INDEX idx_25fc29e1$aca_is_fpds ON txn_earliest_temp USING BTREE(is_fpds) WITH (fillfactor = 97);

--------------------------------------------------------------------------------
-- Step 3, create summary_award_recipient_view using txn_earliest_temp
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 3: Create summary_award_recipient_view'; END $$;

CREATE MATERIALIZED VIEW summary_award_recipient_view_temp AS (
  SELECT
  txn_earliest_temp.award_id,
  txn_earliest_temp.action_date,
  COALESCE(recipient_lookup.recipient_hash, MD5(
    UPPER(COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)))::uuid
  )::uuid AS recipient_hash,
  COALESCE(transaction_fpds.ultimate_parent_unique_ide, transaction_fabs.ultimate_parent_unique_ide) AS parent_recipient_unique_id

  FROM txn_earliest_temp
  LEFT OUTER JOIN transaction_fabs ON (txn_earliest_temp.is_fpds = false AND txn_earliest_temp.id = transaction_fabs.transaction_id)
  LEFT OUTER JOIN transaction_fpds ON (txn_earliest_temp.is_fpds = true AND txn_earliest_temp.id = transaction_fpds.transaction_id)
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
-- Step 4, create indexes on our new summary_award_recipient_view
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 4: Creating indexes on summary_award_recipient_view'; END $$;

CREATE INDEX idx_25fc29e1$aca_award_id ON summary_award_recipient_view_temp USING BTREE(award_id) WITH (fillfactor = 97);
CREATE INDEX idx_25fc29e1$aca_action_date_temp ON summary_award_recipient_view_temp USING BTREE(action_date DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_25fc29e1$aca_recipient_hash_temp ON summary_award_recipient_view_temp USING BTREE(recipient_hash) WITH (fillfactor = 97);
CREATE INDEX idx_25fc29e1$aca_parent_recipient_unique_id_temp ON summary_award_recipient_view_temp USING BTREE(parent_recipient_unique_id) WITH (fillfactor = 97);

--------------------------------------------------------------------------------
-- Step 5, Cleanup, renaming, drop temp table
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 5: Cleaning up'; END $$;

ALTER MATERIALIZED VIEW IF EXISTS summary_award_recipient_view RENAME TO summary_award_recipient_view_old;
ALTER INDEX IF EXISTS idx_25fc29e1$aca_action_date RENAME TO idx_25fc29e1$aca_action_date_old;
ALTER INDEX IF EXISTS idx_25fc29e1$aca_fiscal_year RENAME TO idx_25fc29e1$aca_fiscal_year_old;
ALTER INDEX IF EXISTS idx_25fc29e1$aca_recipient_hash RENAME TO idx_25fc29e1$aca_recipient_hash_old;
ALTER INDEX IF EXISTS idx_25fc29e1$aca_parent_recipient_unique_id RENAME TO idx_25fc29e1$aca_parent_recipient_unique_id_old;

ALTER MATERIALIZED VIEW summary_award_recipient_view_temp RENAME TO summary_award_recipient_view;
ALTER INDEX idx_25fc29e1$aca_action_date_temp RENAME TO idx_25fc29e1$aca_action_date;
ALTER INDEX idx_25fc29e1$aca_fiscal_year_temp RENAME TO idx_25fc29e1$aca_fiscal_year;
ALTER INDEX idx_25fc29e1$aca_recipient_hash_temp RENAME TO idx_25fc29e1$aca_recipient_hash;
ALTER INDEX idx_25fc29e1$aca_parent_recipient_unique_id_temp RENAME TO idx_25fc29e1$aca_parent_recipient_unique_id;

DROP MATERIALIZED VIEW IF EXISTS txn_earliest_temp;

ANALYZE VERBOSE summary_award_recipient_view;
GRANT SELECT ON summary_award_recipient_view TO readonly;