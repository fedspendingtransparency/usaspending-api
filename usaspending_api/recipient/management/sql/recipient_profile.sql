

-- CREATE MATERIALIZED VIEW intermediate_normalized_transactions AS (
-- WITH all_transactions AS (
--   SELECT
--     le.parent_recipient_unique_id,
--     le.recipient_unique_id,
--     tn.action_date,

--     CASE
--       WHEN le.parent_recipient_unique_id IS NOT NULL THEN 'C'
--     ELSE 'R' END AS recipient_level,

--     MD5(
--       UPPER((
--         SELECT CONCAT(duns::text, name::text) FROM recipient_normalization_pair(
--           le.recipient_name, le.recipient_unique_id
--         ) AS (name text, duns text)
--       ))
--     )::uuid AS recipient_hash,

--     COALESCE(CASE
--       WHEN tn.type IN ('07', '08') THEN awards.total_subsidy_cost
--       ELSE tn.federal_action_obligation
--     END, 0)::NUMERIC(23, 2) AS generated_pragmatic_obligation
--   FROM
--     transaction_normalized AS tn
--   INNER JOIN awards ON tn.award_id = awards.id
--   LEFT OUTER JOIN
--     legal_entity AS le ON (tn.recipient_id = le.legal_entity_id)
--   WHERE tn.action_date >= '2007-10-01')

-- SELECT
-- recipient_hash, recipient_unique_id, parent_recipient_unique_id, recipient_level, action_date, generated_pragmatic_obligation
-- FROM all_transactions
-- );

DROP MATERIALIZED VIEW IF EXISTS intermediate_normalized_transactions;

CREATE MATERIALIZED VIEW intermediate_normalized_transactions AS (
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
recipient_hash, recipient_unique_id, parent_recipient_unique_id, recipient_level, action_date, generated_pragmatic_obligation
FROM all_transactions
);


--------------------------------------------------------------------------------
-- CREATE MATERIALIZED VIEW intermediate_normalized_transactions AS (
-- WITH all_transactions AS (
--   SELECT
--     le.parent_recipient_unique_id,
--     le.recipient_unique_id,
--     tn.action_date,

--     CASE
--       WHEN le.parent_recipient_unique_id IS NOT NULL THEN 'C'
--     ELSE 'R' END AS recipient_level,

--     MD5(
--       UPPER((
--         SELECT CONCAT(duns::text, name::text) FROM recipient_normalization_pair(
--           le.recipient_name, le.recipient_unique_id
--         ) AS (name text, duns text)
--       ))
--     )::uuid AS recipient_hash,

--     COALESCE(CASE
--       WHEN tn.type IN ('07', '08') THEN awards.total_subsidy_cost
--       ELSE tn.federal_action_obligation
--     END, 0)::NUMERIC(23, 2) AS generated_pragmatic_obligation
--   FROM
--     transaction_normalized AS tn
--   INNER JOIN awards ON tn.award_id = awards.id
--   LEFT OUTER JOIN
--     transaction_fabs ON (tn.id = transaction_fabs.transaction_id)
--   LEFT OUTER JOIN
--     transaction_fpds ON (tn.id = transaction_fpds.transaction_id)
--   LEFT OUTER JOIN
--     legal_entity AS le ON (tn.recipient_id = le.legal_entity_id)
--   WHERE tn.action_date >= '2007-10-01')


-- SELECT recipient_hash, recipient_unique_id, parent_recipient_unique_id, recipient_level, action_date, generated_pragmatic_obligation FROM all_transactions);  -- 76,800,475 2928s

CREATE INDEX idx_temp_intermediate ON intermediate_normalized_transactions (recipient_hash);    -- 130s


CREATE TABLE recipient_profile_new (
    recipient_level character(1) NOT NULL,
    recipient_hash UUID,
    recipient_unique_id TEXT,
    recipient_name TEXT,
    unused BOOLEAN DEFAULT true,  -- temporary for tracking
    recipient_affiliations TEXT[] DEFAULT '{}'::text[],
    all_fiscal_years NUMERIC(23,2) DEFAULT 0.00,
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
  FROM recipient_lookup_view
  WHERE recipient_lookup_view.duns IS NOT NULL
  UNION ALL
  SELECT
    'C' as recipient_level,
    recipient_hash,
    duns AS recipient_unique_id,
    legal_business_name AS recipient_name
  FROM recipient_lookup_view
  UNION ALL
  SELECT
    'R' as recipient_level,
    recipient_hash,
    duns AS recipient_unique_id,
    legal_business_name AS recipient_name
  FROM recipient_lookup_view
  WHERE recipient_lookup_view.duns IS NULL;


CREATE UNIQUE INDEX idx_recipient_profile_uniq_new ON recipient_profile_new USING BTREE(recipient_hash, recipient_level);

-- Obligation for past 12 months
UPDATE recipient_profile_new AS rpv SET last_12_months =
 rpv.last_12_months + tx.generated_pragmatic_obligation, unused = false

FROM intermediate_normalized_transactions AS tx
WHERE tx.recipient_hash = rpv.recipient_hash and tx.recipient_level = rpv.recipient_level and tx.action_date >= now() - INTERVAL '1 year';

-- obligations from FY2008+
UPDATE recipient_profile_new AS rpv SET all_fiscal_years =
 rpv.all_fiscal_years + tx.generated_pragmatic_obligation, unused = false

FROM intermediate_normalized_transactions AS tx
WHERE tx.recipient_hash = rpv.recipient_hash and tx.recipient_level = rpv.recipient_level;

-- Populating children list in parents
WITH parent_recipients AS (
  SELECT
    parent_recipient_unique_id,
    array_agg(DISTINCT recipient_unique_id) AS duns_list
  FROM intermediate_normalized_transactions
  WHERE parent_recipient_unique_id IS NOT NULL
  GROUP BY parent_recipient_unique_id
)
UPDATE recipient_profile_new AS rpv SET recipient_affiliations =
 pr.duns_list, unused = false

FROM parent_recipients AS pr
WHERE rpv.recipient_unique_id = pr.parent_recipient_unique_id and rpv.recipient_level = 'P';

-- populate parent DUNS in children
WITH all_recipients AS (
  SELECT
    DISTINCT le.recipient_unique_id,
    le.parent_recipient_unique_id,
    action_date
  FROM
    intermediate_normalized_transactions AS le
    WHERE le.recipient_unique_id IS NOT NULL AND le.parent_recipient_unique_id IS NOT NULL
  ORDER BY action_date DESC)

UPDATE recipient_profile_new AS rpv SET recipient_affiliations =
 ARRAY[tx.parent_recipient_unique_id], unused = false

FROM all_recipients AS tx
WHERE rpv.recipient_unique_id = tx.recipient_unique_id and rpv.recipient_level = 'C';

ANALYZE VERBOSE recipient_profile_new;

DELETE FROM recipient_profile_new WHERE unused = true;
ALTER TABLE recipient_profile_new DROP COLUMN unused;

VACUUM ANALYZE VERBOSE recipient_profile_new;

BEGIN;
DROP MATERIALIZED VIEW intermediate_normalized_transactions;
DROP TABLE IF EXISTS recipient_profile;
ALTER TABLE recipient_profile_new RENAME TO recipient_profile;
ALTER INDEX idx_recipient_profile_uniq_new RENAME TO idx_recipient_profile_uniq;
COMMIT;

CREATE INDEX idx_recipient_profile_name ON recipient_profile USING GIN(recipient_name gin_trgm_ops);
CREATE INDEX idx_recipient_profile_unique_id ON recipient_profile USING BTREE(recipient_unique_id);