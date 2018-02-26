--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_award_view RENAME TO summary_award_view_old;
ALTER INDEX IF EXISTS idx_bf26125d$d86__action_date RENAME TO idx_bf26125d$d86__action_date_old;
ALTER INDEX IF EXISTS idx_bf26125d$d86__type RENAME TO idx_bf26125d$d86__type_old;
ALTER INDEX IF EXISTS idx_bf26125d$d86__fy RENAME TO idx_bf26125d$d86__fy_old;
ALTER INDEX IF EXISTS idx_bf26125d$d86__pulled_from RENAME TO idx_bf26125d$d86__pulled_from_old;
ALTER INDEX IF EXISTS idx_bf26125d$d86__awarding_agency_id RENAME TO idx_bf26125d$d86__awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_bf26125d$d86__funding_agency_id RENAME TO idx_bf26125d$d86__funding_agency_id_old;
ALTER INDEX IF EXISTS idx_bf26125d$d86__awarding_toptier_agency_name RENAME TO idx_bf26125d$d86__awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_bf26125d$d86__awarding_subtier_agency_name RENAME TO idx_bf26125d$d86__awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_bf26125d$d86__funding_toptier_agency_name RENAME TO idx_bf26125d$d86__funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_bf26125d$d86__funding_subtier_agency_name RENAME TO idx_bf26125d$d86__funding_subtier_agency_name_old;

ALTER MATERIALIZED VIEW summary_award_view_temp RENAME TO summary_award_view;
ALTER INDEX idx_bf26125d$d86__action_date_temp RENAME TO idx_bf26125d$d86__action_date;
ALTER INDEX idx_bf26125d$d86__type_temp RENAME TO idx_bf26125d$d86__type;
ALTER INDEX idx_bf26125d$d86__fy_temp RENAME TO idx_bf26125d$d86__fy;
ALTER INDEX idx_bf26125d$d86__pulled_from_temp RENAME TO idx_bf26125d$d86__pulled_from;
ALTER INDEX idx_bf26125d$d86__awarding_agency_id_temp RENAME TO idx_bf26125d$d86__awarding_agency_id;
ALTER INDEX idx_bf26125d$d86__funding_agency_id_temp RENAME TO idx_bf26125d$d86__funding_agency_id;
ALTER INDEX idx_bf26125d$d86__awarding_toptier_agency_name_temp RENAME TO idx_bf26125d$d86__awarding_toptier_agency_name;
ALTER INDEX idx_bf26125d$d86__awarding_subtier_agency_name_temp RENAME TO idx_bf26125d$d86__awarding_subtier_agency_name;
ALTER INDEX idx_bf26125d$d86__funding_toptier_agency_name_temp RENAME TO idx_bf26125d$d86__funding_toptier_agency_name;
ALTER INDEX idx_bf26125d$d86__funding_subtier_agency_name_temp RENAME TO idx_bf26125d$d86__funding_subtier_agency_name;
