DROP VIEW IF EXISTS vw_es_award_search CASCADE;

CREATE VIEW vw_es_award_search AS (
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
);

GRANT SELECT ON vw_es_award_search TO readonly;
