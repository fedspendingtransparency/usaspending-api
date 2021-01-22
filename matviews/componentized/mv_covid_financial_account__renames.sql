ALTER MATERIALIZED VIEW IF EXISTS mv_covid_financial_account RENAME TO mv_covid_financial_account_old;
ALTER INDEX IF EXISTS idx_5b3e47f0$a65_award_id RENAME TO idx_5b3e47f0$a65_award_id_old;


ALTER MATERIALIZED VIEW mv_covid_financial_account_temp RENAME TO mv_covid_financial_account;
ALTER INDEX idx_5b3e47f0$a65_award_id_temp RENAME TO idx_5b3e47f0$a65_award_id;

