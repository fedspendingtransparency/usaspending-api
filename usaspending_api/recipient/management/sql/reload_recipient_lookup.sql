BEGIN;
TRUNCATE TABLE recipient_lookup;
ALTER SEQUENCE recipient_lookup_id_seq RESTART WITH 1;

-- GET ALL SAM RECIPIENTS
WITH sam_recipients AS (
  SELECT
    MD5(UPPER(CONCAT(duns.awardee_or_recipient_uniqu, duns.legal_business_name)))::uuid AS recipient_hash,
    UPPER(duns.legal_business_name) AS legal_business_name,
    duns.awardee_or_recipient_uniqu AS duns
  FROM duns
  GROUP BY duns.legal_business_name, duns.awardee_or_recipient_uniqu
)

INSERT INTO recipient_lookup (recipient_hash, legal_business_name, duns)
  SELECT recipient_hash, legal_business_name, duns FROM sam_recipients;

CREATE UNIQUE INDEX idx_temporary_duns_unique_duns ON recipient_lookup(duns);
-- GET OTHER LEGAL ENTITIES
WITH transaction_recipients as (
  SELECT
    DISTINCT ON (recipient_unique_id)
    MD5(UPPER(CONCAT(recipient_unique_id, recipient_name)))::uuid AS recipient_hash,
    UPPER(recipient_name) AS legal_business_name,
    recipient_unique_id AS duns
  FROM legal_entity
  WHERE recipient_unique_id IS NOT NULL
  ORDER BY recipient_unique_id, update_date DESC
)

INSERT INTO recipient_lookup (recipient_hash, legal_business_name, duns)
SELECT recipient_hash, legal_business_name, duns FROM transaction_recipients
ON CONFLICT (duns) DO NOTHING;

DROP INDEX idx_temporary_duns_unique_duns;


-- Adding Non-DUNS Recipients which are not included in SAM for obvious reasons
WITH dunsless_transaction_recipients as (
SELECT
  DISTINCT ON (recipient_name)
  MD5(UPPER(recipient_name))::uuid AS recipient_hash,
  UPPER(recipient_name) AS legal_business_name,
  NULL::text AS duns
FROM legal_entity
WHERE recipient_unique_id IS NULL
)

INSERT INTO recipient_lookup (recipient_hash, legal_business_name, duns)
  SELECT recipient_hash, legal_business_name, duns FROM dunsless_transaction_recipients;

COMMIT;
VACUUM ANALYZE VERBOSE recipient_lookup;
