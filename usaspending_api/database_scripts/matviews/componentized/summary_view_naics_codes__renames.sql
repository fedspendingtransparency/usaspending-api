--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_view_naics_codes RENAME TO summary_view_naics_codes_old;
ALTER INDEX IF EXISTS idx_78684541$235__unique_pk RENAME TO idx_78684541$235__unique_pk_old;
ALTER INDEX IF EXISTS idx_78684541$235__action_date RENAME TO idx_78684541$235__action_date_old;
ALTER INDEX IF EXISTS idx_78684541$235__type RENAME TO idx_78684541$235__type_old;
ALTER INDEX IF EXISTS idx_78684541$235__naics RENAME TO idx_78684541$235__naics_old;
ALTER INDEX IF EXISTS idx_78684541$235__tuned_type_and_idv RENAME TO idx_78684541$235__tuned_type_and_idv_old;

ALTER MATERIALIZED VIEW summary_view_naics_codes_temp RENAME TO summary_view_naics_codes;
ALTER INDEX idx_78684541$235__unique_pk_temp RENAME TO idx_78684541$235__unique_pk;
ALTER INDEX idx_78684541$235__action_date_temp RENAME TO idx_78684541$235__action_date;
ALTER INDEX idx_78684541$235__type_temp RENAME TO idx_78684541$235__type;
ALTER INDEX idx_78684541$235__naics_temp RENAME TO idx_78684541$235__naics;
ALTER INDEX idx_78684541$235__tuned_type_and_idv_temp RENAME TO idx_78684541$235__tuned_type_and_idv;
