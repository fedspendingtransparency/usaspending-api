--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_view RENAME TO summary_view_old;
ALTER INDEX IF EXISTS idx_bf26125d$e1c__action_date RENAME TO idx_bf26125d$e1c__action_date_old;
ALTER INDEX IF EXISTS idx_bf26125d$e1c__type RENAME TO idx_bf26125d$e1c__type_old;
ALTER INDEX IF EXISTS idx_bf26125d$e1c__fy RENAME TO idx_bf26125d$e1c__fy_old;
ALTER INDEX IF EXISTS idx_bf26125d$e1c__pulled_from RENAME TO idx_bf26125d$e1c__pulled_from_old;
ALTER INDEX IF EXISTS idx_bf26125d$e1c__awarding_agency_id RENAME TO idx_bf26125d$e1c__awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_bf26125d$e1c__funding_agency_id RENAME TO idx_bf26125d$e1c__funding_agency_id_old;
ALTER INDEX IF EXISTS idx_bf26125d$e1c__awarding_toptier_agency_name RENAME TO idx_bf26125d$e1c__awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_bf26125d$e1c__awarding_subtier_agency_name RENAME TO idx_bf26125d$e1c__awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_bf26125d$e1c__funding_toptier_agency_name RENAME TO idx_bf26125d$e1c__funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_bf26125d$e1c__funding_subtier_agency_name RENAME TO idx_bf26125d$e1c__funding_subtier_agency_name_old;

ALTER MATERIALIZED VIEW summary_view_temp RENAME TO summary_view;
ALTER INDEX idx_bf26125d$e1c__action_date_temp RENAME TO idx_bf26125d$e1c__action_date;
ALTER INDEX idx_bf26125d$e1c__type_temp RENAME TO idx_bf26125d$e1c__type;
ALTER INDEX idx_bf26125d$e1c__fy_temp RENAME TO idx_bf26125d$e1c__fy;
ALTER INDEX idx_bf26125d$e1c__pulled_from_temp RENAME TO idx_bf26125d$e1c__pulled_from;
ALTER INDEX idx_bf26125d$e1c__awarding_agency_id_temp RENAME TO idx_bf26125d$e1c__awarding_agency_id;
ALTER INDEX idx_bf26125d$e1c__funding_agency_id_temp RENAME TO idx_bf26125d$e1c__funding_agency_id;
ALTER INDEX idx_bf26125d$e1c__awarding_toptier_agency_name_temp RENAME TO idx_bf26125d$e1c__awarding_toptier_agency_name;
ALTER INDEX idx_bf26125d$e1c__awarding_subtier_agency_name_temp RENAME TO idx_bf26125d$e1c__awarding_subtier_agency_name;
ALTER INDEX idx_bf26125d$e1c__funding_toptier_agency_name_temp RENAME TO idx_bf26125d$e1c__funding_toptier_agency_name;
ALTER INDEX idx_bf26125d$e1c__funding_subtier_agency_name_temp RENAME TO idx_bf26125d$e1c__funding_subtier_agency_name;
