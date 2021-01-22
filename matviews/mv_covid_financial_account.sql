DROP MATERIALIZED VIEW IF EXISTS mv_covid_financial_account_temp CASCADE;
DROP MATERIALIZED VIEW IF EXISTS mv_covid_financial_account_old CASCADE;

CREATE MATERIALIZED VIEW mv_covid_financial_account_temp AS
SELECT
  awards.id AS award_id,
  awards.type,
  financial_account_records.def_codes,

  financial_account_records.outlay,
  financial_account_records.obligation,
  COALESCE(awards.total_loan_value, 0) AS total_loan_value,

  COALESCE(recipient_lookup.recipient_hash, MD5(UPPER(
    CASE
      WHEN COALESCE(FPDS.awardee_or_recipient_uniqu, FABS.awardee_or_recipient_uniqu) IS NOT NULL
        THEN CONCAT('duns-', COALESCE(FPDS.awardee_or_recipient_uniqu, FABS.awardee_or_recipient_uniqu))
      ELSE CONCAT('name-', COALESCE(FPDS.awardee_or_recipient_legal, FABS.awardee_or_recipient_legal))
    END
  ))::uuid) AS recipient_hash,
  UPPER(COALESCE(recipient_lookup.legal_business_name, FPDS.awardee_or_recipient_legal, FABS.awardee_or_recipient_legal)) AS recipient_name
FROM (
  SELECT
    FABA.award_id,
    ARRAY_AGG(DISTINCT FABA.disaster_emergency_fund_code ORDER BY FABA.disaster_emergency_fund_code) AS def_codes,
    COALESCE(SUM(
      CASE WHEN SA.is_final_balances_for_fy = TRUE THEN FABA.gross_outlay_amount_by_award_cpe END
    ), 0) AS outlay,
    COALESCE(SUM(FABA.transaction_obligated_amount), 0) AS obligation
  FROM
    financial_accounts_by_awards AS FABA
  INNER JOIN
    disaster_emergency_fund_code AS DEFC ON (FABA.disaster_emergency_fund_code = DEFC.code and DEFC.group_name = 'covid_19')
  INNER JOIN
    submission_attributes AS SA ON (FABA.submission_id = SA.submission_id AND SA.reporting_period_start >= '2020-04-01')
  INNER JOIN
    dabs_submission_window_schedule
      ON (SA.submission_window_id = dabs_submission_window_schedule.id
      AND dabs_submission_window_schedule.submission_reveal_date <= NOW())
  WHERE
    FABA.award_id IS NOT NULL
  GROUP BY
    FABA.award_id
) AS financial_account_records
INNER JOIN
  awards ON (financial_account_records.award_id = awards.id)
LEFT OUTER JOIN
  transaction_fpds AS FPDS ON (awards.latest_transaction_id = FPDS.transaction_id)
LEFT OUTER JOIN
  transaction_fabs AS FABS on (awards.latest_transaction_id = FABS.transaction_id)
LEFT OUTER JOIN
  recipient_lookup ON (recipient_lookup.duns = COALESCE(FPDS.awardee_or_recipient_uniqu, FABS.awardee_or_recipient_uniqu)
    AND COALESCE(FPDS.awardee_or_recipient_uniqu, FABS.awardee_or_recipient_uniqu) IS NOT NULL)
WHERE
  financial_account_records.outlay != 0
  OR financial_account_records.obligation != 0
  OR awards.total_loan_value != 0 WITH DATA;

CREATE UNIQUE INDEX idx_5b3e47f0$a65_award_id_temp ON mv_covid_financial_account_temp USING BTREE(award_id) WITH (fillfactor = 97);


ALTER MATERIALIZED VIEW IF EXISTS mv_covid_financial_account RENAME TO mv_covid_financial_account_old;
ALTER INDEX IF EXISTS idx_5b3e47f0$a65_award_id RENAME TO idx_5b3e47f0$a65_award_id_old;


ALTER MATERIALIZED VIEW mv_covid_financial_account_temp RENAME TO mv_covid_financial_account;
ALTER INDEX idx_5b3e47f0$a65_award_id_temp RENAME TO idx_5b3e47f0$a65_award_id;


ANALYZE VERBOSE mv_covid_financial_account;
GRANT SELECT ON mv_covid_financial_account TO readonly;
