BEGIN;

DROP VIEW IF EXISTS vw_award_download;

CREATE VIEW vw_award_download AS (
  SELECT * FROM mv_award_contracts
  UNION ALL
  SELECT * FROM mv_award_directpayments
  UNION ALL
  SELECT * FROM mv_award_grants
  UNION ALL
  SELECT * FROM mv_award_idvs
  UNION ALL
  SELECT * FROM mv_award_loans
  UNION ALL
  SELECT * FROM mv_award_other
  UNION ALL
  SELECT * FROM mv_award_all_pre2008
);

COMMIT;
