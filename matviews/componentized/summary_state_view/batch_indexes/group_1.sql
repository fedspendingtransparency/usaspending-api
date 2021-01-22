CREATE INDEX idx_51ceb7ee$d77_ordered_action_date_temp ON summary_state_view_temp USING BTREE(action_date DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_51ceb7ee$d77_pop_state_code_temp ON summary_state_view_temp USING BTREE(pop_state_code) WITH (fillfactor = 97) WHERE pop_state_code IS NOT NULL;
