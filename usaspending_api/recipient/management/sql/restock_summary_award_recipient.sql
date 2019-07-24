DROP TABLE IF EXISTS public.earliest_transaction_temp CASCADE;

--------------------------------------------------------------------------------
-- Step 1, create the temporary matview of awards and transactions with their earliest action_date
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 1: Create temp earliest action date'; END $$;

CREATE TABLE public.earliest_transaction_temp AS (
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
    txn.action_date
  FROM lowest_action_date AS lad
  JOIN transaction_normalized AS txn ON lad.award_id=txn.award_id AND lad.min_date = txn.action_date
);

--------------------------------------------------------------------------------
-- Step 2, update summary_award_recipient using txn_earliest_temp
--------------------------------------------------------------------------------
DO $$ BEGIN RAISE NOTICE 'Step 2: Create summary_award_recipient'; END $$;

BEGIN;
TRUNCATE TABLE public.summary_award_recipient RESTART IDENTITY;
INSERT INTO public.summary_award_recipient
  (award_id, action_date, recipient_hash, parent_recipient_unique_id)
  SELECT
      DISTINCT ON (award_id)
      earliest_transaction_temp.award_id,
      earliest_transaction_temp.action_date,
      txn_tableview.recipient_hash,
      txn_tableview.parent_recipient_unique_id
  FROM public.earliest_transaction_temp
  JOIN universal_transaction_tableview txn_tableview ON (earliest_transaction_temp.id = txn_tableview.transaction_id);
DROP TABLE public.earliest_transaction_temp;
COMMIT;
