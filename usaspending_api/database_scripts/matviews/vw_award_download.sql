BEGIN;

DROP VIEW IF EXISTS reporting_award_download_view;

CREATE VIEW reporting_award_download_view AS (
  SELECT * FROM reporting_award_contracts_view
  UNION ALL
  SELECT * FROM reporting_award_directpayments_view
  UNION ALL
  SELECT * FROM reporting_award_grants_view
  UNION ALL
  SELECT * FROM reporting_award_idvs_view
  UNION ALL
  SELECT * FROM reporting_award_loans_view
  UNION ALL
  SELECT * FROM reporting_award_other_view
  UNION ALL
  SELECT * FROM reporting_award_all_pre2008_view
)

COMMIT;