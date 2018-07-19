--------------------------------------------------------------------------------
-- Step 1, create the temporary matview of recipients from transactions
--------------------------------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS temporary_recipients_from_transactions_view;

CREATE MATERIALIZED VIEW temporary_recipients_from_transactions_view AS (
  WITH all_transactions AS (
    SELECT
      parent_recipient_unique_id,
      recipient_unique_id,
      action_date,
      CASE
        WHEN parent_recipient_unique_id IS NOT NULL THEN 'C'
      ELSE 'R' END AS recipient_level,
      recipient_hash,
      generated_pragmatic_obligation
    FROM
      universal_transaction_matview
    WHERE action_date >= '2007-10-01'
  )
  SELECT
    recipient_hash,
    recipient_unique_id,
    parent_recipient_unique_id,
    recipient_level,
    action_date,
    generated_pragmatic_obligation
  FROM
    all_transactions
);

CREATE INDEX idx_recipients_from_transactions_view_1 ON temporary_recipients_from_transactions_view USING BTREE(recipient_hash, recipient_level);

--------------------------------------------------------------------------------
-- Step 2, Create the new table and populate with 100% of combinations
--------------------------------------------------------------------------------
CREATE TABLE recipient_profile_new (
  recipient_level character(1) NOT NULL,
  recipient_hash UUID,
  recipient_unique_id TEXT,
  recipient_name TEXT,
  unused BOOLEAN DEFAULT true,
  recipient_affiliations TEXT[] DEFAULT '{}'::text[],
  last_12_months NUMERIC(23,2) DEFAULT 0.00
);

INSERT INTO recipient_profile_new (
  recipient_level,
  recipient_hash,
  recipient_unique_id,
  recipient_name
)
SELECT
  'P' as recipient_level,
  recipient_hash,
  duns AS recipient_unique_id,
  legal_business_name AS recipient_name
FROM
  recipient_lookup_view
WHERE
  recipient_lookup_view.duns IS NOT NULL
UNION ALL
SELECT
  'C' as recipient_level,
  recipient_hash,
  duns AS recipient_unique_id,
  legal_business_name AS recipient_name
FROM
  recipient_lookup_view
UNION ALL
SELECT
  'R' as recipient_level,
  recipient_hash,
  duns AS recipient_unique_id,
  legal_business_name AS recipient_name
FROM
  recipient_lookup_view
WHERE
  recipient_lookup_view.duns IS NULL;


CREATE UNIQUE INDEX idx_recipient_profile_uniq_new ON recipient_profile_new USING BTREE(recipient_hash, recipient_level);

--------------------------------------------------------------------------------
-- Step 3, Obligation for past 12 months
--------------------------------------------------------------------------------
UPDATE recipient_profile_new AS rpv
SET
  last_12_months = rpv.last_12_months + trft.generated_pragmatic_obligation,
  unused = false
FROM
  temporary_recipients_from_transactions_view AS trft
WHERE
  trft.recipient_hash = rpv.recipient_hash AND
  trft.recipient_level = rpv.recipient_level AND
  trft.action_date >= now() - INTERVAL '1 year';

--------------------------------------------------------------------------------
-- Step 4, Populate the Parent Obligation for past 12 months
--------------------------------------------------------------------------------
WITH grouped_by_parent AS (
  SELECT
   parent_recipient_unique_id AS duns,
   SUM(generated_pragmatic_obligation) AS amount
  FROM
    temporary_recipients_from_transactions_view AS trft
  WHERE
    trft.action_date >= now() - INTERVAL '1 year' AND
    parent_recipient_unique_id IS NOT NULL
  GROUP BY parent_recipient_unique_id
)

UPDATE recipient_profile_new AS rpv
SET
  last_12_months = rpv.last_12_months + gbp.amount,
  unused = false
FROM
  grouped_by_parent AS gbp
WHERE
  rpv.recipient_unique_id = gbp.duns AND
  rpv.recipient_level = 'P';

--------------------------------------------------------------------------------
-- Step 5, Populating children list in parents
--------------------------------------------------------------------------------
WITH parent_recipients AS (
  SELECT
    parent_recipient_unique_id,
    array_agg(DISTINCT recipient_unique_id) AS duns_list
  FROM temporary_recipients_from_transactions_view
  WHERE
    parent_recipient_unique_id IS NOT NULL
  GROUP BY
    parent_recipient_unique_id
)
UPDATE recipient_profile_new AS rpv
SET
  recipient_affiliations = pr.duns_list,
  unused = false

FROM parent_recipients AS pr
WHERE rpv.recipient_unique_id = pr.parent_recipient_unique_id and rpv.recipient_level = 'P';

--------------------------------------------------------------------------------
-- Step 6, Populate parent DUNS in children
--------------------------------------------------------------------------------
WITH all_recipients AS (
  SELECT
    DISTINCT recipient_unique_id,
    parent_recipient_unique_id,
    action_date
  FROM
    temporary_recipients_from_transactions_view
    WHERE
      recipient_unique_id IS NOT NULL AND
      parent_recipient_unique_id IS NOT NULL
  ORDER BY action_date DESC
)
UPDATE recipient_profile_new AS rpv
SET
  recipient_affiliations = ARRAY[ar.parent_recipient_unique_id],
  unused = false
FROM all_recipients AS ar
WHERE
  rpv.recipient_unique_id = ar.recipient_unique_id AND
  rpv.recipient_level = 'C';

--------------------------------------------------------------------------------
-- Step 7, Finalize new table
--------------------------------------------------------------------------------
ANALYZE VERBOSE recipient_profile_new;

DELETE FROM recipient_profile_new WHERE unused = true;
ALTER TABLE recipient_profile_new DROP COLUMN unused;

VACUUM ANALYZE VERBOSE recipient_profile_new;

--------------------------------------------------------------------------------
-- Step 8, Drop unnecessary relations and standup new table as final
--------------------------------------------------------------------------------
BEGIN;
DROP MATERIALIZED VIEW temporary_recipients_from_transactions_view;
DROP TABLE IF EXISTS recipient_profile;
ALTER TABLE recipient_profile_new RENAME TO recipient_profile;
ALTER INDEX idx_recipient_profile_uniq_new RENAME TO idx_recipient_profile_uniq;
COMMIT;

--------------------------------------------------------------------------------
-- Step 9, Create indexes useful for API
--------------------------------------------------------------------------------
CREATE INDEX idx_recipient_profile_name ON recipient_profile USING GIN(recipient_name gin_trgm_ops);
CREATE INDEX idx_recipient_profile_unique_id ON recipient_profile USING BTREE(recipient_unique_id);