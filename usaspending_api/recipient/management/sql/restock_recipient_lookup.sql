
DROP TABLE IF EXISTS public.temporary_restock_recipient_lookup;

-- GET ALL SAM RECIPIENTS
CREATE TABLE public.temporary_restock_recipient_lookup (recipient_hash, legal_business_name, duns) AS
  SELECT
    MD5(UPPER(CONCAT(duns.awardee_or_recipient_uniqu, duns.legal_business_name)))::uuid AS recipient_hash,
    UPPER(duns.legal_business_name) AS legal_business_name,
    duns.awardee_or_recipient_uniqu AS duns
  FROM duns
  GROUP BY duns.legal_business_name, duns.awardee_or_recipient_uniqu;

CREATE UNIQUE INDEX idx_temporary_duns_unique_duns ON public.temporary_restock_recipient_lookup (duns);
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

INSERT INTO public.temporary_restock_recipient_lookup (recipient_hash, legal_business_name, duns)
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

INSERT INTO public.temporary_restock_recipient_lookup (recipient_hash, legal_business_name, duns)
  SELECT recipient_hash, legal_business_name, duns FROM dunsless_transaction_recipients;


BEGIN;
TRUNCATE TABLE public.recipient_lookup RESTART IDENTITY;
INSERT INTO public.recipient_lookup (recipient_hash, legal_business_name, duns) SELECT * FROM public.temporary_restock_recipient_lookup;
DROP TABLE public.temporary_restock_recipient_lookup;
COMMIT;
VACUUM ANALYZE VERBOSE public.recipient_lookup;
