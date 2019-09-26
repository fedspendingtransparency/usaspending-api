CREATE OR REPLACE VIEW vw_award_search AS (
  SELECT * FROM mv_contract_award_search
  UNION ALL
  SELECT * FROM mv_directpayment_award_search
  UNION ALL
  SELECT * FROM mv_grant_award_search
  UNION ALL
  SELECT * FROM mv_idv_award_search
  UNION ALL
  SELECT * FROM mv_loan_award_search
  UNION ALL
  SELECT * FROM mv_other_award_search
  UNION ALL
  SELECT * FROM mv_pre2008_award_search
);
