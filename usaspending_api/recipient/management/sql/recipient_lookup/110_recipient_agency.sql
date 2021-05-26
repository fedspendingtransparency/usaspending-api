DO $$ BEGIN RAISE NOTICE '110 Dropping old recipient_agency'; END $$;

DROP TABLE IF EXISTS recipient_agency CASCADE;

DO $$ BEGIN RAISE NOTICE '110 Creating recipient_agency table'; END $$;

CREATE TABLE recipient_agency AS (
	SELECT
		fiscal_year, toptier_code,COALESCE(recipient_lookup.recipient_hash, MD5(UPPER(
        CASE
          WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu))
          ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) END
      ))::uuid) AS recipient_hash,
      SUM(COALESCE(CASE WHEN transaction_normalized.type IN('07','08') THEN transaction_normalized.original_loan_subsidy_cost ELSE transaction_normalized.federal_action_obligation END, 0)) AS recipient_amount
	FROM transaction_normalized
		LEFT JOIN agency ON awarding_agency_id = agency.id
		LEFT JOIN toptier_agency ta ON ta.toptier_agency_id = agency.toptier_agency_id
		LEFT JOIN transaction_fabs ON (transaction_normalized.id = transaction_fabs.transaction_id AND transaction_normalized.is_fpds = false)
		LEFT JOIN transaction_fpds ON (transaction_normalized.id = transaction_fpds.transaction_id AND transaction_normalized.is_fpds = true)
		LEFT JOIN recipient_lookup ON recipient_lookup.duns = COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) AND COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL

	WHERE fiscal_year >= 2017

	GROUP BY toptier_code, COALESCE(recipient_lookup.recipient_hash, MD5(UPPER(
        CASE
          WHEN COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu) IS NOT NULL THEN CONCAT('duns-', COALESCE(transaction_fpds.awardee_or_recipient_uniqu, transaction_fabs.awardee_or_recipient_uniqu))
          ELSE CONCAT('name-', COALESCE(transaction_fpds.awardee_or_recipient_legal, transaction_fabs.awardee_or_recipient_legal)) END
      ))::uuid), fiscal_year
	HAVING SUM(COALESCE(CASE WHEN transaction_normalized.type IN('07','08') THEN transaction_normalized.original_loan_subsidy_cost ELSE transaction_normalized.federal_action_obligation END, 0)) > 0);

COMMIT;