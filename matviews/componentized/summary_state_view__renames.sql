ALTER MATERIALIZED VIEW IF EXISTS summary_state_view RENAME TO summary_state_view_old;
ALTER INDEX IF EXISTS idx_51ceb7ee$d77_deterministic_unique_hash RENAME TO idx_51ceb7ee$d77_deterministic_unique_hash_old;
ALTER INDEX IF EXISTS idx_51ceb7ee$d77_ordered_action_date RENAME TO idx_51ceb7ee$d77_ordered_action_date_old;
ALTER INDEX IF EXISTS idx_51ceb7ee$d77_type RENAME TO idx_51ceb7ee$d77_type_old;
ALTER INDEX IF EXISTS idx_51ceb7ee$d77_pop_country_code RENAME TO idx_51ceb7ee$d77_pop_country_code_old;
ALTER INDEX IF EXISTS idx_51ceb7ee$d77_pop_state_code RENAME TO idx_51ceb7ee$d77_pop_state_code_old;
ALTER INDEX IF EXISTS idx_51ceb7ee$d77_compound_geo_pop RENAME TO idx_51ceb7ee$d77_compound_geo_pop_old;


ALTER MATERIALIZED VIEW summary_state_view_temp RENAME TO summary_state_view;
ALTER INDEX idx_51ceb7ee$d77_deterministic_unique_hash_temp RENAME TO idx_51ceb7ee$d77_deterministic_unique_hash;
ALTER INDEX idx_51ceb7ee$d77_ordered_action_date_temp RENAME TO idx_51ceb7ee$d77_ordered_action_date;
ALTER INDEX idx_51ceb7ee$d77_type_temp RENAME TO idx_51ceb7ee$d77_type;
ALTER INDEX idx_51ceb7ee$d77_pop_country_code_temp RENAME TO idx_51ceb7ee$d77_pop_country_code;
ALTER INDEX idx_51ceb7ee$d77_pop_state_code_temp RENAME TO idx_51ceb7ee$d77_pop_state_code;
ALTER INDEX idx_51ceb7ee$d77_compound_geo_pop_temp RENAME TO idx_51ceb7ee$d77_compound_geo_pop;

