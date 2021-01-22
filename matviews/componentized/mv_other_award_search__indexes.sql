CREATE UNIQUE INDEX idx_e4637983$9c6_id_temp ON mv_other_award_search_temp USING BTREE(award_id) WITH (fillfactor = 97);
CREATE INDEX idx_e4637983$9c6_recipient_hash_temp ON mv_other_award_search_temp USING BTREE(recipient_hash) WITH (fillfactor = 97);
CREATE INDEX idx_e4637983$9c6_recipient_unique_id_temp ON mv_other_award_search_temp USING BTREE(recipient_unique_id) WITH (fillfactor = 97) WHERE recipient_unique_id IS NOT NULL;
CREATE INDEX idx_e4637983$9c6_action_date_temp ON mv_other_award_search_temp USING BTREE(action_date DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_e4637983$9c6_funding_agency_id_temp ON mv_other_award_search_temp USING BTREE(funding_agency_id ASC NULLS LAST) WITH (fillfactor = 97) WHERE funding_agency_id IS NOT NULL;
CREATE INDEX idx_e4637983$9c6_recipient_location_state_code_temp ON mv_other_award_search_temp USING BTREE(recipient_location_state_code) WITH (fillfactor = 97) WHERE recipient_location_state_code IS NOT NULL;
CREATE INDEX idx_e4637983$9c6_recipient_location_county_code_temp ON mv_other_award_search_temp USING BTREE(recipient_location_county_code) WITH (fillfactor = 97) WHERE recipient_location_county_code IS NOT NULL;
CREATE INDEX idx_e4637983$9c6_recipient_location_cong_code_temp ON mv_other_award_search_temp USING BTREE(recipient_location_congressional_code) WITH (fillfactor = 97) WHERE recipient_location_congressional_code IS NOT NULL;
