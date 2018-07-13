CREATE TABLE recipient_profile_view (
    recipient_level character(1) NOT NULL,
    recipient_hash UUID NOT NULL,
    recipient_unique_id TEXT,
    recipient_name TEXT,
    recipient_affiliations TEXT[] DEFAULT '{}'::text[],
    generated_pragmatic_obligation NUMERIC(23,2) DEFAULT 0.00
);

INSERT INTO recipient_profile_view (
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
  WHERE recipient_lookup_view.duns IS NULL
;


CREATE UNIQUE INDEX idx_recipient_profile_view_uniq ON recipient_profile_view USING BTREE(recipient_hash, recipient_level);


WITH all_transactions AS (
  SELECT
    le.parent_recipient_unique_id,
    le.recipient_unique_id,

    CASE
      WHEN le.parent_recipient_unique_id IS NOT NULL THEN 'C'
    ELSE 'R' END AS recipient_level,

    MD5(
      UPPER((
        SELECT CONCAT(duns::text, name::text) FROM recipient_normalization_pair(
          le.recipient_name, le.recipient_unique_id
        ) AS (name text, duns text)
      ))
    )::uuid AS recipient_hash,

    COALESCE(CASE
      WHEN tn.type IN ('07', '08') THEN awards.total_subsidy_cost
      ELSE tn.federal_action_obligation
    END, 0)::NUMERIC(23, 2) AS generated_pragmatic_obligation
  FROM
    transaction_normalized AS tn
  INNER JOIN awards ON tn.award_id = awards.id
  LEFT OUTER JOIN
    transaction_fabs ON (tn.id = transaction_fabs.transaction_id)
  LEFT OUTER JOIN
    transaction_fpds ON (tn.id = transaction_fpds.transaction_id)
  LEFT OUTER JOIN
    legal_entity AS le ON (tn.recipient_id = le.legal_entity_id)
  WHERE tn.action_date >= now() - INTERVAL '1 year'
)

UPDATE recipient_profile_view AS rpv SET generated_pragmatic_obligation =
 rpv.generated_pragmatic_obligation + tx.generated_pragmatic_obligation

FROM all_transactions AS tx
WHERE tx.recipient_hash = rpv.recipient_hash and tx.recipient_level = rpv.recipient_level;



WITH all_child_transactions AS (
  SELECT
    le.parent_recipient_unique_id,
    le.recipient_unique_id
  FROM
    transaction_normalized AS tn
  LEFT OUTER JOIN
    legal_entity AS le ON (tn.recipient_id = le.legal_entity_id)
  WHERE le.parent_recipient_unique_id IS NOT NULL
)

UPDATE recipient_profile_view AS rpv SET recipient_affiliations =
 array_append(rpv.recipient_affiliations, tx.recipient_unique_id)

FROM all_child_transactions AS tx
WHERE rpv.recipient_unique_id = tx.parent_recipient_unique_id and rpv.recipient_level = 'P'
returning rpv.recipient_unique_id;



-- CREATE UNIQUE INDEX idx_unique_recipient_record ON recipient_profile_view USING BTREE(recipient_level, duns, action_date);
-- CREATE INDEX idx_recipient_duns_date_level ON recipient_profile_view USING BTREE(duns, action_date, recipient_level);

CREATE INDEX idx_recipient_affiliations ON recipient_profile_view USING GIN(recipient_affiliations);

VACUUM ANALYZE VERBOSE recipient_profile_view;
