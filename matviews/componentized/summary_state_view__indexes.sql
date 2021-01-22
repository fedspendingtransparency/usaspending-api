CREATE UNIQUE INDEX idx_51ceb7ee$d77_deterministic_unique_hash_temp ON summary_state_view_temp USING BTREE(duh) WITH (fillfactor = 97);
CREATE INDEX idx_51ceb7ee$d77_ordered_action_date_temp ON summary_state_view_temp USING BTREE(action_date DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_51ceb7ee$d77_type_temp ON summary_state_view_temp USING BTREE(type) WITH (fillfactor = 97) WHERE type IS NOT NULL;
CREATE INDEX idx_51ceb7ee$d77_pop_country_code_temp ON summary_state_view_temp USING BTREE(pop_country_code) WITH (fillfactor = 97);
CREATE INDEX idx_51ceb7ee$d77_pop_state_code_temp ON summary_state_view_temp USING BTREE(pop_state_code) WITH (fillfactor = 97) WHERE pop_state_code IS NOT NULL;
CREATE INDEX idx_51ceb7ee$d77_compound_geo_pop_temp ON summary_state_view_temp USING BTREE(pop_country_code, pop_state_code, action_date) WITH (fillfactor = 97);
