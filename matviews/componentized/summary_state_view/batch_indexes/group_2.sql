CREATE INDEX idx_51ceb7ee$d77_type_temp ON summary_state_view_temp USING BTREE(type) WITH (fillfactor = 97) WHERE type IS NOT NULL;
CREATE INDEX idx_51ceb7ee$d77_compound_geo_pop_temp ON summary_state_view_temp USING BTREE(pop_country_code, pop_state_code, action_date) WITH (fillfactor = 97);
