DROP MATERIALIZED VIEW IF EXISTS summary_award_recipient_view_temp CASCADE;
DROP MATERIALIZED VIEW IF EXISTS summary_award_recipient_view_old CASCADE;

CREATE MATERIALIZED VIEW summary_award_recipient_view_temp AS
SELECT
  MD5(array_to_string(sort(array_agg(awards.id::int)), ' '))::uuid AS pk,
  awards.date_signed,
  transaction_normalized.action_date,
  transaction_normalized.fiscal_year,
  awards.type,
  awards.category,

  MD5(
    UPPER((
      SELECT CONCAT(duns::text, name::text) FROM public.recipient_normalization_pair(
        legal_entity.recipient_name, legal_entity.recipient_unique_id
      ) AS (name text, duns text)
    ))
  )::uuid AS recipient_hash,
  COUNT(*) counts
FROM
  awards
JOIN
  legal_entity ON awards.recipient_id = legal_entity.legal_entity_id
JOIN
  transaction_normalized ON (awards.latest_transaction_id = transaction_normalized.id)
WHERE
  transaction_normalized.action_date >= '2007-10-01'
GROUP BY
  awards.date_signed,
  transaction_normalized.action_date,
  transaction_normalized.fiscal_year,
  awards.type,
  transaction_fpds.pulled_from,
  awards.category,
  legal_entity.recipient_name,
  legal_entity.recipient_unique_id;

CREATE UNIQUE INDEX idx_7e0fb950f16_unique_pk_temp ON summary_award_recipient_view_temp USING BTREE(pk) WITH (fillfactor = 97);
CREATE INDEX idx_7e0fb950f16_date_signed_temp ON summary_award_recipient_view_temp USING BTREE(date_signed DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7e0fb950f16_type_temp ON summary_award_recipient_view_temp USING BTREE(type) WITH (fillfactor = 97);
CREATE INDEX idx_7e0fb950f16_pulled_from_temp ON summary_award_recipient_view_temp USING BTREE(pulled_from) WITH (fillfactor = 97) WHERE pulled_from IS NOT NULL;
CREATE INDEX idx_7e0fb950f16_recipient_hash_temp ON summary_award_recipient_view_temp USING BTREE(recipient_hash) WITH (fillfactor = 97) WHERE recipient_hash;

ALTER MATERIALIZED VIEW IF EXISTS summary_award_recipient_view RENAME TO summary_award_recipient_view_old;
ALTER INDEX IF EXISTS idx_7e0fb950f16_unique_pk RENAME TO idx_7e0fb950f16_unique_pk_old;
ALTER INDEX IF EXISTS idx_7e0fb950f16_date_signed RENAME TO idx_7e0fb950f16_date_signed_old;
ALTER INDEX IF EXISTS idx_7e0fb950f16_type RENAME TO idx_7e0fb950f16_type_old;
ALTER INDEX IF EXISTS idx_7e0fb950f16_pulled_from RENAME TO idx_7e0fb950f16_pulled_from_old;
ALTER INDEX IF EXISTS idx_7e0fb950f16_recipient_hash RENAME TO idx_7e0fb950f16_recipient_hash_old;

ALTER MATERIALIZED VIEW summary_award_recipient_view_temp RENAME TO summary_award_recipient_view;
ALTER INDEX idx_7e0fb950f16_unique_pk_temp RENAME TO idx_7e0fb950f16_unique_pk;
ALTER INDEX idx_7e0fb950f16_date_signed_temp RENAME TO idx_7e0fb950f16_date_signed;
ALTER INDEX idx_7e0fb950f16_type_temp RENAME TO idx_7e0fb950f16_type;
ALTER INDEX idx_7e0fb950f16_pulled_from_temp RENAME TO idx_7e0fb950f16_pulled_from;
ALTER INDEX idx_7e0fb950f16_recipient_hash_temp RENAME TO idx_7e0fb950f16_recipient_hash;

ANALYZE VERBOSE summary_award_recipient_view;
GRANT SELECT ON summary_award_recipient_view TO readonly;
