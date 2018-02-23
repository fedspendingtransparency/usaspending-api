--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_view_naics_codes RENAME TO summary_view_naics_codes_old;
ALTER INDEX IF EXISTS idx_f1c47885__action_date RENAME TO idx_f1c47885__action_date_old;
ALTER INDEX IF EXISTS idx_f1c47885__type RENAME TO idx_f1c47885__type_old;
ALTER INDEX IF EXISTS idx_f1c47885__naics RENAME TO idx_f1c47885__naics_old;
ALTER INDEX IF EXISTS idx_f1c47885__tuned_type_and_idv RENAME TO idx_f1c47885__tuned_type_and_idv_old;

ALTER MATERIALIZED VIEW summary_view_naics_codes_temp RENAME TO summary_view_naics_codes;
ALTER INDEX idx_f1c47885__action_date_temp RENAME TO idx_f1c47885__action_date;
ALTER INDEX idx_f1c47885__type_temp RENAME TO idx_f1c47885__type;
ALTER INDEX idx_f1c47885__naics_temp RENAME TO idx_f1c47885__naics;
ALTER INDEX idx_f1c47885__tuned_type_and_idv_temp RENAME TO idx_f1c47885__tuned_type_and_idv;
