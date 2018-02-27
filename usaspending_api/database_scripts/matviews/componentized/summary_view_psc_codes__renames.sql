--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_view_psc_codes RENAME TO summary_view_psc_codes_old;
ALTER INDEX IF EXISTS idx_94831233__action_date RENAME TO idx_94831233__action_date_old;
ALTER INDEX IF EXISTS idx_94831233__type RENAME TO idx_94831233__type_old;
ALTER INDEX IF EXISTS idx_94831233__tuned_type_and_idv RENAME TO idx_94831233__tuned_type_and_idv_old;

ALTER MATERIALIZED VIEW summary_view_psc_codes_temp RENAME TO summary_view_psc_codes;
ALTER INDEX idx_94831233__action_date_temp RENAME TO idx_94831233__action_date;
ALTER INDEX idx_94831233__type_temp RENAME TO idx_94831233__type;
ALTER INDEX idx_94831233__tuned_type_and_idv_temp RENAME TO idx_94831233__tuned_type_and_idv;
