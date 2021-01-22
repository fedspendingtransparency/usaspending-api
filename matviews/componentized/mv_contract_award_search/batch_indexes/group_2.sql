CREATE INDEX idx_e4637983$af5_recipient_unique_id_temp ON mv_contract_award_search_temp USING BTREE(recipient_unique_id) WITH (fillfactor = 97) WHERE recipient_unique_id IS NOT NULL;
CREATE INDEX idx_e4637983$af5_recipient_location_state_code_temp ON mv_contract_award_search_temp USING BTREE(recipient_location_state_code) WITH (fillfactor = 97) WHERE recipient_location_state_code IS NOT NULL;
CREATE STATISTICS st_e4637983$af5_tas_and_dates_temp ON treasury_account_identifiers, type, action_date FROM mv_contract_award_search_temp;
