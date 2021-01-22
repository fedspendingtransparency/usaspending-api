CREATE INDEX idx_e4637983$9c6_recipient_unique_id_temp ON mv_other_award_search_temp USING BTREE(recipient_unique_id) WITH (fillfactor = 97) WHERE recipient_unique_id IS NOT NULL;
CREATE INDEX idx_e4637983$9c6_recipient_location_state_code_temp ON mv_other_award_search_temp USING BTREE(recipient_location_state_code) WITH (fillfactor = 97) WHERE recipient_location_state_code IS NOT NULL;
