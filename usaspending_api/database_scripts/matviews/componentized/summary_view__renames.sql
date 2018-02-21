--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_view RENAME TO summary_view_old;
ALTER INDEX IF EXISTS idx_011752f0__action_date RENAME TO idx_011752f0__action_date_old;
ALTER INDEX IF EXISTS idx_011752f0__type RENAME TO idx_011752f0__type_old;
ALTER INDEX IF EXISTS idx_011752f0__fy RENAME TO idx_011752f0__fy_old;
ALTER INDEX IF EXISTS idx_011752f0__pulled_from RENAME TO idx_011752f0__pulled_from_old;
ALTER INDEX IF EXISTS idx_011752f0__awarding_agency_id RENAME TO idx_011752f0__awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_011752f0__funding_agency_id RENAME TO idx_011752f0__funding_agency_id_old;
ALTER INDEX IF EXISTS idx_011752f0__awarding_toptier_agency_name RENAME TO idx_011752f0__awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_011752f0__awarding_subtier_agency_name RENAME TO idx_011752f0__awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_011752f0__funding_toptier_agency_name RENAME TO idx_011752f0__funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_011752f0__funding_subtier_agency_name RENAME TO idx_011752f0__funding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_011752f0__tuned_type_and_idv RENAME TO idx_011752f0__tuned_type_and_idv_old;

ALTER MATERIALIZED VIEW summary_view_temp RENAME TO summary_view;
ALTER INDEX idx_011752f0__action_date_temp RENAME TO idx_011752f0__action_date;
ALTER INDEX idx_011752f0__type_temp RENAME TO idx_011752f0__type;
ALTER INDEX idx_011752f0__fy_temp RENAME TO idx_011752f0__fy;
ALTER INDEX idx_011752f0__pulled_from_temp RENAME TO idx_011752f0__pulled_from;
ALTER INDEX idx_011752f0__awarding_agency_id_temp RENAME TO idx_011752f0__awarding_agency_id;
ALTER INDEX idx_011752f0__funding_agency_id_temp RENAME TO idx_011752f0__funding_agency_id;
ALTER INDEX idx_011752f0__awarding_toptier_agency_name_temp RENAME TO idx_011752f0__awarding_toptier_agency_name;
ALTER INDEX idx_011752f0__awarding_subtier_agency_name_temp RENAME TO idx_011752f0__awarding_subtier_agency_name;
ALTER INDEX idx_011752f0__funding_toptier_agency_name_temp RENAME TO idx_011752f0__funding_toptier_agency_name;
ALTER INDEX idx_011752f0__funding_subtier_agency_name_temp RENAME TO idx_011752f0__funding_subtier_agency_name;
ALTER INDEX idx_011752f0__tuned_type_and_idv_temp RENAME TO idx_011752f0__tuned_type_and_idv;
