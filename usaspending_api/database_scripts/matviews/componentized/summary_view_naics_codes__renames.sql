--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_view_naics_codes RENAME TO summary_view_naics_codes_old;
ALTER INDEX IF EXISTS idx_3a7bb23c$5bf__action_date RENAME TO idx_3a7bb23c$5bf__action_date_old;
ALTER INDEX IF EXISTS idx_3a7bb23c$5bf__type RENAME TO idx_3a7bb23c$5bf__type_old;
ALTER INDEX IF EXISTS idx_3a7bb23c$5bf__naics RENAME TO idx_3a7bb23c$5bf__naics_old;
ALTER INDEX IF EXISTS idx_3a7bb23c$5bf__tuned_type_and_idv RENAME TO idx_3a7bb23c$5bf__tuned_type_and_idv_old;

ALTER MATERIALIZED VIEW summary_view_naics_codes_temp RENAME TO summary_view_naics_codes;
ALTER INDEX idx_3a7bb23c$5bf__action_date_temp RENAME TO idx_3a7bb23c$5bf__action_date;
ALTER INDEX idx_3a7bb23c$5bf__type_temp RENAME TO idx_3a7bb23c$5bf__type;
ALTER INDEX idx_3a7bb23c$5bf__naics_temp RENAME TO idx_3a7bb23c$5bf__naics;
ALTER INDEX idx_3a7bb23c$5bf__tuned_type_and_idv_temp RENAME TO idx_3a7bb23c$5bf__tuned_type_and_idv;
