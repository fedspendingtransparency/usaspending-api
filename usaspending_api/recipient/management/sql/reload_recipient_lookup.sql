TRUNCATE TABLE recipient_lookup;
ALTER SEQUENCE recipient_lookup_id_seq RESTART WITH 1;
VACUUM ANALYZE VERBOSE recipient_lookup;

WITH recipient_cte AS (
  WITH sam_recipients AS (
    SELECT
      MD5(UPPER(CONCAT(duns.awardee_or_recipient_uniqu, duns.legal_business_name)))::uuid AS recipient_hash,
      duns.legal_business_name,
      duns.awardee_or_recipient_uniqu AS duns
    FROM duns
    GROUP BY duns.legal_business_name, duns.awardee_or_recipient_uniqu
  ),
  transaction_recipients AS (
    WITH unique_groups as (
      SELECT
        DISTINCT ON (recipient_unique_id)
        recipient_name,
        recipient_unique_id
      FROM legal_entity
      WHERE recipient_unique_id IS NOT NULL
      ORDER BY recipient_unique_id, update_date DESC
    )
    SELECT
      MD5(UPPER(CONCAT(recipient_unique_id, recipient_name)))::uuid AS recipient_hash,
      recipient_name,
      recipient_unique_id AS duns
    FROM unique_groups
  )
  SELECT
    MD5(UPPER(CONCAT(
      COALESCE(sam.duns, tx_recip.duns),
      COALESCE(sam.legal_business_name, tx_recip.recipient_name))))::uuid AS recipient_hash,
    COALESCE(sam.legal_business_name, tx_recip.recipient_name) AS legal_business_name,
    COALESCE(sam.duns, tx_recip.duns) as duns
  FROM sam_recipients AS sam
  LEFT JOIN transaction_recipients as tx_recip ON tx_recip.duns = sam.duns
  GROUP BY
    COALESCE(sam.duns, tx_recip.duns),
    COALESCE(sam.legal_business_name, tx_recip.recipient_name)
-- Adding Non-DUNS Recipients which are not included in SAM for obvious reasons
  UNION
  (
    SELECT
      DISTINCT ON (recipient_name)
      MD5(UPPER(recipient_name))::uuid AS recipient_hash,
      recipient_name,
      NULL::text AS duns
    FROM legal_entity
    WHERE recipient_unique_id IS NULL
  )
)
INSERT INTO recipient_lookup (recipient_hash, legal_business_name, duns)
  SELECT recipient_hash, legal_business_name, duns FROM recipient_cte
;

VACUUM ANALYZE VERBOSE recipient_lookup;