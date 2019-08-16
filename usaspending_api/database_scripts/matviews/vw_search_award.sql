BEGIN;

DROP VIEW IF EXISTS vw_search_award;

CREATE VIEW vw_search_award AS (
  SELECT * FROM mv_search_award_all_pre2008
  UNION ALL
  SELECT * FROM mv_search_award_contract
  UNION ALL
  SELECT * FROM mv_search_award_directpayment
  UNION ALL
  SELECT * FROM mv_search_award_grant
  UNION ALL
  SELECT * FROM mv_search_award_idv
  UNION ALL
  SELECT * FROM mv_search_award_loan
  UNION ALL
  SELECT * FROM mv_search_award_other
);

COMMIT;
