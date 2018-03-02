--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_view_cfda_number RENAME TO summary_view_cfda_number_old;
ALTER INDEX IF EXISTS idx_0b1c98f0__action_date RENAME TO idx_0b1c98f0__action_date_old;
ALTER INDEX IF EXISTS idx_0b1c98f0__type RENAME TO idx_0b1c98f0__type_old;
ALTER INDEX IF EXISTS idx_0b1c98f0__tuned_type_and_idv RENAME TO idx_0b1c98f0__tuned_type_and_idv_old;

ALTER MATERIALIZED VIEW summary_view_cfda_number_temp RENAME TO summary_view_cfda_number;
ALTER INDEX idx_0b1c98f0__action_date_temp RENAME TO idx_0b1c98f0__action_date;
ALTER INDEX idx_0b1c98f0__type_temp RENAME TO idx_0b1c98f0__type;
ALTER INDEX idx_0b1c98f0__tuned_type_and_idv_temp RENAME TO idx_0b1c98f0__tuned_type_and_idv;
