CREATE UNIQUE INDEX idx_51ceb7ee$d77_deterministic_unique_hash_temp ON summary_state_view_temp USING BTREE(duh) WITH (fillfactor = 97);
CREATE INDEX idx_51ceb7ee$d77_pop_country_code_temp ON summary_state_view_temp USING BTREE(pop_country_code) WITH (fillfactor = 97);
