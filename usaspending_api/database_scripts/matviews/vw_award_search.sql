DROP VIEW IF EXISTS vw_award_search CASCADE;

CREATE VIEW rpt.vw_award_search AS (
  SELECT * FROM temp.mv_contract_award_search
  UNION ALL
  SELECT * FROM temp.mv_directpayment_award_search
  UNION ALL
  SELECT * FROM temp.mv_grant_award_search
  UNION ALL
  SELECT * FROM temp.mv_idv_award_search
  UNION ALL
  SELECT * FROM temp.mv_loan_award_search
  UNION ALL
  SELECT * FROM temp.mv_other_award_search
  UNION ALL
  SELECT * FROM temp.mv_pre2008_award_search
);

GRANT SELECT ON vw_award_search TO readonly;
