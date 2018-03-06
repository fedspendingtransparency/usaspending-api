--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_view RENAME TO summary_view_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$184_unique_pk RENAME TO idx_af8ca7ca$184_unique_pk_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$184_action_date RENAME TO idx_af8ca7ca$184_action_date_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$184_type RENAME TO idx_af8ca7ca$184_type_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$184_fy RENAME TO idx_af8ca7ca$184_fy_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$184_pulled_from RENAME TO idx_af8ca7ca$184_pulled_from_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$184_awarding_agency_id RENAME TO idx_af8ca7ca$184_awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$184_funding_agency_id RENAME TO idx_af8ca7ca$184_funding_agency_id_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$184_awarding_toptier_agency_name RENAME TO idx_af8ca7ca$184_awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$184_awarding_subtier_agency_name RENAME TO idx_af8ca7ca$184_awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$184_funding_toptier_agency_name RENAME TO idx_af8ca7ca$184_funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$184_funding_subtier_agency_name RENAME TO idx_af8ca7ca$184_funding_subtier_agency_name_old;

ALTER MATERIALIZED VIEW summary_view_temp RENAME TO summary_view;
ALTER INDEX idx_af8ca7ca$184_unique_pk_temp RENAME TO idx_af8ca7ca$184_unique_pk;
ALTER INDEX idx_af8ca7ca$184_action_date_temp RENAME TO idx_af8ca7ca$184_action_date;
ALTER INDEX idx_af8ca7ca$184_type_temp RENAME TO idx_af8ca7ca$184_type;
ALTER INDEX idx_af8ca7ca$184_fy_temp RENAME TO idx_af8ca7ca$184_fy;
ALTER INDEX idx_af8ca7ca$184_pulled_from_temp RENAME TO idx_af8ca7ca$184_pulled_from;
ALTER INDEX idx_af8ca7ca$184_awarding_agency_id_temp RENAME TO idx_af8ca7ca$184_awarding_agency_id;
ALTER INDEX idx_af8ca7ca$184_funding_agency_id_temp RENAME TO idx_af8ca7ca$184_funding_agency_id;
ALTER INDEX idx_af8ca7ca$184_awarding_toptier_agency_name_temp RENAME TO idx_af8ca7ca$184_awarding_toptier_agency_name;
ALTER INDEX idx_af8ca7ca$184_awarding_subtier_agency_name_temp RENAME TO idx_af8ca7ca$184_awarding_subtier_agency_name;
ALTER INDEX idx_af8ca7ca$184_funding_toptier_agency_name_temp RENAME TO idx_af8ca7ca$184_funding_toptier_agency_name;
ALTER INDEX idx_af8ca7ca$184_funding_subtier_agency_name_temp RENAME TO idx_af8ca7ca$184_funding_subtier_agency_name;
