DO $$ BEGIN RAISE NOTICE '000 Creating temporary table and materialized view'; END $$;

DROP TABLE IF EXISTS public.temporary_restock_recipient_lookup;

DROP MATERIALIZED VIEW IF EXISTS public.temporary_transaction_recipients_view;

CREATE TABLE public.temporary_restock_recipient_lookup AS SELECT * FROM recipient_lookup limit 0;
ALTER TABLE public.temporary_restock_recipient_lookup ADD COLUMN duns_recipient_hash uuid;
CREATE UNIQUE INDEX recipient_lookup_new_recipient_hash ON public.temporary_restock_recipient_lookup(recipient_hash);

CREATE MATERIALIZED VIEW public.temporary_transaction_recipients_view AS (
  SELECT
    tn.transaction_unique_id,
    tn.is_fpds,
    MD5(UPPER(
      CASE
        WHEN COALESCE(fpds.awardee_or_recipient_uei, fabs.uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(fpds.awardee_or_recipient_uei, fabs.uei))
        WHEN COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu))
        ELSE CONCAT('name-', COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal)) END
    ))::uuid AS recipient_hash,
    MD5(UPPER(
      CASE
        WHEN COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu))
        WHEN COALESCE(fpds.awardee_or_recipient_uei, fabs.uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(fpds.awardee_or_recipient_uei, fabs.uei))
        ELSE CONCAT('name-', COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal)) END
    ))::uuid AS duns_recipient_hash,
    MD5(UPPER(
      CASE
        WHEN COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei))
        WHEN COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide) IS NOT  NULL THEN CONCAT('duns-', COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide))
        ELSE CONCAT('name-', COALESCE(fpds.ultimate_parent_legal_enti, fabs.ultimate_parent_legal_enti)) END
    ))::uuid AS parent_recipient_hash,
    MD5(UPPER(
      CASE
        WHEN COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide) IS NOT NULL THEN CONCAT('duns-', COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide))
        WHEN COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei) IS NOT NULL THEN CONCAT('uei-', COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei))
        ELSE CONCAT('name-', COALESCE(fpds.ultimate_parent_legal_enti, fabs.ultimate_parent_legal_enti)) END
    ))::uuid AS duns_parent_recipient_hash,
    COALESCE(fpds.awardee_or_recipient_uniqu, fabs.awardee_or_recipient_uniqu) AS awardee_or_recipient_uniqu,
    COALESCE(fpds.awardee_or_recipient_uei, fabs.uei) AS uei,
    COALESCE(fpds.ultimate_parent_uei, fabs.ultimate_parent_uei) AS ultimate_parent_uei,
    UPPER(COALESCE(fpds.ultimate_parent_legal_enti, fabs.ultimate_parent_legal_enti)) AS ultimate_parent_legal_enti,
    COALESCE(fpds.ultimate_parent_unique_ide, fabs.ultimate_parent_unique_ide) AS ultimate_parent_unique_ide,
    UPPER(COALESCE(fpds.awardee_or_recipient_legal, fabs.awardee_or_recipient_legal)) AS awardee_or_recipient_legal,
    COALESCE(fpds.legal_entity_city_name, fabs.legal_entity_city_name) AS city,
    COALESCE(fpds.legal_entity_state_code, fabs.legal_entity_state_code) AS state,
    COALESCE(fpds.legal_entity_zip5, fabs.legal_entity_zip5) AS zip5,
    COALESCE(fpds.legal_entity_zip_last4, fabs.legal_entity_zip_last4) AS zip4,
    COALESCE(fpds.legal_entity_congressional, fabs.legal_entity_congressional) AS congressional_district,
    COALESCE(fpds.legal_entity_address_line1, fabs.legal_entity_address_line1) AS address_line_1,
    COALESCE(fpds.legal_entity_address_line2, fabs.legal_entity_address_line1) AS address_line_2,
    COALESCE(fpds.legal_entity_country_code, fabs.legal_entity_country_code) AS country_code,
    tn.action_date,
    CASE
      WHEN tn.is_fpds = TRUE THEN 'fpds'::TEXT
      ELSE 'fabs'::TEXT
      END AS source
  FROM
    vw_transaction_normalized AS tn
  LEFT OUTER JOIN vw_transaction_fpds AS fpds ON
    (tn.id = fpds.transaction_id)
  LEFT OUTER JOIN vw_transaction_fabs AS fabs ON
    (tn.id = fabs.transaction_id)
  WHERE tn.action_date >= '2007-10-01'
  ORDER BY tn.action_date DESC
);

CREATE INDEX idx_temporary_restock_recipient_view ON public.temporary_transaction_recipients_view (uei, awardee_or_recipient_uniqu, awardee_or_recipient_legal);

CREATE INDEX idx_temporary_restock_parent_recipient_view ON public.temporary_transaction_recipients_view (ultimate_parent_uei, ultimate_parent_unique_ide, ultimate_parent_legal_enti);

ANALYZE public.temporary_transaction_recipients_view;
