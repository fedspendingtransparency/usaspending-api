ALTER MATERIALIZED VIEW IF EXISTS mv_contract_award_search RENAME TO mv_contract_award_search_old;
ALTER INDEX IF EXISTS idx_e4637983$af5_id RENAME TO idx_e4637983$af5_id_old;
ALTER INDEX IF EXISTS idx_e4637983$af5_recipient_hash RENAME TO idx_e4637983$af5_recipient_hash_old;
ALTER INDEX IF EXISTS idx_e4637983$af5_recipient_unique_id RENAME TO idx_e4637983$af5_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_e4637983$af5_action_date RENAME TO idx_e4637983$af5_action_date_old;
ALTER INDEX IF EXISTS idx_e4637983$af5_funding_agency_id RENAME TO idx_e4637983$af5_funding_agency_id_old;
ALTER INDEX IF EXISTS idx_e4637983$af5_recipient_location_state_code RENAME TO idx_e4637983$af5_recipient_location_state_code_old;
ALTER INDEX IF EXISTS idx_e4637983$af5_recipient_location_county_code RENAME TO idx_e4637983$af5_recipient_location_county_code_old;
ALTER INDEX IF EXISTS idx_e4637983$af5_recipient_location_cong_code RENAME TO idx_e4637983$af5_recipient_location_cong_code_old;

DO $$ BEGIN
ALTER STATISTICS st_e4637983$af5_tas_and_dates RENAME TO st_e4637983$af5_tas_and_dates_old;
EXCEPTION WHEN undefined_object THEN
RAISE NOTICE 'Skipping statistics renames, no conflicts'; END; $$ language 'plpgsql';

ALTER MATERIALIZED VIEW mv_contract_award_search_temp RENAME TO mv_contract_award_search;
ALTER INDEX idx_e4637983$af5_id_temp RENAME TO idx_e4637983$af5_id;
ALTER INDEX idx_e4637983$af5_recipient_hash_temp RENAME TO idx_e4637983$af5_recipient_hash;
ALTER INDEX idx_e4637983$af5_recipient_unique_id_temp RENAME TO idx_e4637983$af5_recipient_unique_id;
ALTER INDEX idx_e4637983$af5_action_date_temp RENAME TO idx_e4637983$af5_action_date;
ALTER INDEX idx_e4637983$af5_funding_agency_id_temp RENAME TO idx_e4637983$af5_funding_agency_id;
ALTER INDEX idx_e4637983$af5_recipient_location_state_code_temp RENAME TO idx_e4637983$af5_recipient_location_state_code;
ALTER INDEX idx_e4637983$af5_recipient_location_county_code_temp RENAME TO idx_e4637983$af5_recipient_location_county_code;
ALTER INDEX idx_e4637983$af5_recipient_location_cong_code_temp RENAME TO idx_e4637983$af5_recipient_location_cong_code;

ALTER STATISTICS st_e4637983$af5_tas_and_dates_temp RENAME TO st_e4637983$af5_tas_and_dates;
