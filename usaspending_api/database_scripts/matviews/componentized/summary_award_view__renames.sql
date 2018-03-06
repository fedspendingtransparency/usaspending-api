--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_award_view RENAME TO summary_award_view_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$a6a_unique_pk RENAME TO idx_af8ca7ca$a6a_unique_pk_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$a6a_action_date RENAME TO idx_af8ca7ca$a6a_action_date_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$a6a_type RENAME TO idx_af8ca7ca$a6a_type_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$a6a_fy RENAME TO idx_af8ca7ca$a6a_fy_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$a6a_pulled_from RENAME TO idx_af8ca7ca$a6a_pulled_from_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$a6a_awarding_agency_id RENAME TO idx_af8ca7ca$a6a_awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$a6a_funding_agency_id RENAME TO idx_af8ca7ca$a6a_funding_agency_id_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$a6a_awarding_toptier_agency_name RENAME TO idx_af8ca7ca$a6a_awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$a6a_awarding_subtier_agency_name RENAME TO idx_af8ca7ca$a6a_awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$a6a_funding_toptier_agency_name RENAME TO idx_af8ca7ca$a6a_funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$a6a_funding_subtier_agency_name RENAME TO idx_af8ca7ca$a6a_funding_subtier_agency_name_old;

ALTER MATERIALIZED VIEW summary_award_view_temp RENAME TO summary_award_view;
ALTER INDEX idx_af8ca7ca$a6a_unique_pk_temp RENAME TO idx_af8ca7ca$a6a_unique_pk;
ALTER INDEX idx_af8ca7ca$a6a_action_date_temp RENAME TO idx_af8ca7ca$a6a_action_date;
ALTER INDEX idx_af8ca7ca$a6a_type_temp RENAME TO idx_af8ca7ca$a6a_type;
ALTER INDEX idx_af8ca7ca$a6a_fy_temp RENAME TO idx_af8ca7ca$a6a_fy;
ALTER INDEX idx_af8ca7ca$a6a_pulled_from_temp RENAME TO idx_af8ca7ca$a6a_pulled_from;
ALTER INDEX idx_af8ca7ca$a6a_awarding_agency_id_temp RENAME TO idx_af8ca7ca$a6a_awarding_agency_id;
ALTER INDEX idx_af8ca7ca$a6a_funding_agency_id_temp RENAME TO idx_af8ca7ca$a6a_funding_agency_id;
ALTER INDEX idx_af8ca7ca$a6a_awarding_toptier_agency_name_temp RENAME TO idx_af8ca7ca$a6a_awarding_toptier_agency_name;
ALTER INDEX idx_af8ca7ca$a6a_awarding_subtier_agency_name_temp RENAME TO idx_af8ca7ca$a6a_awarding_subtier_agency_name;
ALTER INDEX idx_af8ca7ca$a6a_funding_toptier_agency_name_temp RENAME TO idx_af8ca7ca$a6a_funding_toptier_agency_name;
ALTER INDEX idx_af8ca7ca$a6a_funding_subtier_agency_name_temp RENAME TO idx_af8ca7ca$a6a_funding_subtier_agency_name;
