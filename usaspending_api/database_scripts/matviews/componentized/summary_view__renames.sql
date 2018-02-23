--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_view RENAME TO summary_view_old;
ALTER INDEX IF EXISTS idx_05bae726__action_date RENAME TO idx_05bae726__action_date_old;
ALTER INDEX IF EXISTS idx_05bae726__type RENAME TO idx_05bae726__type_old;
ALTER INDEX IF EXISTS idx_05bae726__fy RENAME TO idx_05bae726__fy_old;
ALTER INDEX IF EXISTS idx_05bae726__pulled_from RENAME TO idx_05bae726__pulled_from_old;
ALTER INDEX IF EXISTS idx_05bae726__awarding_agency_id RENAME TO idx_05bae726__awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_05bae726__funding_agency_id RENAME TO idx_05bae726__funding_agency_id_old;
ALTER INDEX IF EXISTS idx_05bae726__awarding_toptier_agency_name RENAME TO idx_05bae726__awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_05bae726__awarding_subtier_agency_name RENAME TO idx_05bae726__awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_05bae726__funding_toptier_agency_name RENAME TO idx_05bae726__funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_05bae726__funding_subtier_agency_name RENAME TO idx_05bae726__funding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_05bae726__awarding_toptier_agency_ts_vector RENAME TO idx_05bae726__awarding_toptier_agency_ts_vector_old;
ALTER INDEX IF EXISTS idx_05bae726__awarding_subtier_agency_ts_vector RENAME TO idx_05bae726__awarding_subtier_agency_ts_vector_old;
ALTER INDEX IF EXISTS idx_05bae726__funding_toptier_agency_ts_vector RENAME TO idx_05bae726__funding_toptier_agency_ts_vector_old;
ALTER INDEX IF EXISTS idx_05bae726__funding_subtier_agency_ts_vector RENAME TO idx_05bae726__funding_subtier_agency_ts_vector_old;

ALTER MATERIALIZED VIEW summary_view_temp RENAME TO summary_view;
ALTER INDEX idx_05bae726__action_date_temp RENAME TO idx_05bae726__action_date;
ALTER INDEX idx_05bae726__type_temp RENAME TO idx_05bae726__type;
ALTER INDEX idx_05bae726__fy_temp RENAME TO idx_05bae726__fy;
ALTER INDEX idx_05bae726__pulled_from_temp RENAME TO idx_05bae726__pulled_from;
ALTER INDEX idx_05bae726__awarding_agency_id_temp RENAME TO idx_05bae726__awarding_agency_id;
ALTER INDEX idx_05bae726__funding_agency_id_temp RENAME TO idx_05bae726__funding_agency_id;
ALTER INDEX idx_05bae726__awarding_toptier_agency_name_temp RENAME TO idx_05bae726__awarding_toptier_agency_name;
ALTER INDEX idx_05bae726__awarding_subtier_agency_name_temp RENAME TO idx_05bae726__awarding_subtier_agency_name;
ALTER INDEX idx_05bae726__funding_toptier_agency_name_temp RENAME TO idx_05bae726__funding_toptier_agency_name;
ALTER INDEX idx_05bae726__funding_subtier_agency_name_temp RENAME TO idx_05bae726__funding_subtier_agency_name;
ALTER INDEX idx_05bae726__awarding_toptier_agency_ts_vector_temp RENAME TO idx_05bae726__awarding_toptier_agency_ts_vector;
ALTER INDEX idx_05bae726__awarding_subtier_agency_ts_vector_temp RENAME TO idx_05bae726__awarding_subtier_agency_ts_vector;
ALTER INDEX idx_05bae726__funding_toptier_agency_ts_vector_temp RENAME TO idx_05bae726__funding_toptier_agency_ts_vector;
ALTER INDEX idx_05bae726__funding_subtier_agency_ts_vector_temp RENAME TO idx_05bae726__funding_subtier_agency_ts_vector;
