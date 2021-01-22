ALTER MATERIALIZED VIEW IF EXISTS transaction_search RENAME TO transaction_search_old;
ALTER INDEX IF EXISTS idx_21c979c6$49d_transaction_id RENAME TO idx_21c979c6$49d_transaction_id_old;
ALTER INDEX IF EXISTS idx_21c979c6$49d_action_date RENAME TO idx_21c979c6$49d_action_date_old;
ALTER INDEX IF EXISTS idx_21c979c6$49d_last_modified_date RENAME TO idx_21c979c6$49d_last_modified_date_old;
ALTER INDEX IF EXISTS idx_21c979c6$49d_fiscal_year RENAME TO idx_21c979c6$49d_fiscal_year_old;
ALTER INDEX IF EXISTS idx_21c979c6$49d_type RENAME TO idx_21c979c6$49d_type_old;
ALTER INDEX IF EXISTS idx_21c979c6$49d_award_id RENAME TO idx_21c979c6$49d_award_id_old;
ALTER INDEX IF EXISTS idx_21c979c6$49d_pop_zip5 RENAME TO idx_21c979c6$49d_pop_zip5_old;
ALTER INDEX IF EXISTS idx_21c979c6$49d_recipient_unique_id RENAME TO idx_21c979c6$49d_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_21c979c6$49d_parent_recipient_unique_id RENAME TO idx_21c979c6$49d_parent_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_21c979c6$49d_simple_pop_geolocation RENAME TO idx_21c979c6$49d_simple_pop_geolocation_old;
ALTER INDEX IF EXISTS idx_21c979c6$49d_recipient_hash RENAME TO idx_21c979c6$49d_recipient_hash_old;
ALTER INDEX IF EXISTS idx_21c979c6$49d_action_date_pre2008 RENAME TO idx_21c979c6$49d_action_date_pre2008_old;
ALTER INDEX IF EXISTS idx_21c979c6$49d_etl_update_date RENAME TO idx_21c979c6$49d_etl_update_date_old;


ALTER MATERIALIZED VIEW transaction_search_temp RENAME TO transaction_search;
ALTER INDEX idx_21c979c6$49d_transaction_id_temp RENAME TO idx_21c979c6$49d_transaction_id;
ALTER INDEX idx_21c979c6$49d_action_date_temp RENAME TO idx_21c979c6$49d_action_date;
ALTER INDEX idx_21c979c6$49d_last_modified_date_temp RENAME TO idx_21c979c6$49d_last_modified_date;
ALTER INDEX idx_21c979c6$49d_fiscal_year_temp RENAME TO idx_21c979c6$49d_fiscal_year;
ALTER INDEX idx_21c979c6$49d_type_temp RENAME TO idx_21c979c6$49d_type;
ALTER INDEX idx_21c979c6$49d_award_id_temp RENAME TO idx_21c979c6$49d_award_id;
ALTER INDEX idx_21c979c6$49d_pop_zip5_temp RENAME TO idx_21c979c6$49d_pop_zip5;
ALTER INDEX idx_21c979c6$49d_recipient_unique_id_temp RENAME TO idx_21c979c6$49d_recipient_unique_id;
ALTER INDEX idx_21c979c6$49d_parent_recipient_unique_id_temp RENAME TO idx_21c979c6$49d_parent_recipient_unique_id;
ALTER INDEX idx_21c979c6$49d_simple_pop_geolocation_temp RENAME TO idx_21c979c6$49d_simple_pop_geolocation;
ALTER INDEX idx_21c979c6$49d_recipient_hash_temp RENAME TO idx_21c979c6$49d_recipient_hash;
ALTER INDEX idx_21c979c6$49d_action_date_pre2008_temp RENAME TO idx_21c979c6$49d_action_date_pre2008;
ALTER INDEX idx_21c979c6$49d_etl_update_date_temp RENAME TO idx_21c979c6$49d_etl_update_date;

