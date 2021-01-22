CREATE INDEX idx_e4637983$af5_recipient_hash_temp ON mv_contract_award_search_temp USING BTREE(recipient_hash) WITH (fillfactor = 97);
CREATE INDEX idx_e4637983$af5_funding_agency_id_temp ON mv_contract_award_search_temp USING BTREE(funding_agency_id ASC NULLS LAST) WITH (fillfactor = 97) WHERE funding_agency_id IS NOT NULL;
CREATE INDEX idx_e4637983$af5_recipient_location_cong_code_temp ON mv_contract_award_search_temp USING BTREE(recipient_location_congressional_code) WITH (fillfactor = 97) WHERE recipient_location_congressional_code IS NOT NULL;
