--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_transaction_geo_view RENAME TO summary_transaction_geo_view_old;
ALTER INDEX IF EXISTS idx_4185d7e2__date RENAME TO idx_4185d7e2__date_old;
ALTER INDEX IF EXISTS idx_4185d7e2__fy RENAME TO idx_4185d7e2__fy_old;
ALTER INDEX IF EXISTS idx_4185d7e2__fy_type RENAME TO idx_4185d7e2__fy_type_old;
ALTER INDEX IF EXISTS idx_4185d7e2__type RENAME TO idx_4185d7e2__type_old;
ALTER INDEX IF EXISTS idx_4185d7e2__pulled_from RENAME TO idx_4185d7e2__pulled_from_old;
ALTER INDEX IF EXISTS idx_4185d7e2__recipient_country_code RENAME TO idx_4185d7e2__recipient_country_code_old;
ALTER INDEX IF EXISTS idx_4185d7e2__recipient_state_code RENAME TO idx_4185d7e2__recipient_state_code_old;
ALTER INDEX IF EXISTS idx_4185d7e2__recipient_county_code RENAME TO idx_4185d7e2__recipient_county_code_old;
ALTER INDEX IF EXISTS idx_4185d7e2__recipient_zip RENAME TO idx_4185d7e2__recipient_zip_old;
ALTER INDEX IF EXISTS idx_4185d7e2__pop_country_code RENAME TO idx_4185d7e2__pop_country_code_old;
ALTER INDEX IF EXISTS idx_4185d7e2__pop_state_code RENAME TO idx_4185d7e2__pop_state_code_old;
ALTER INDEX IF EXISTS idx_4185d7e2__pop_county_code RENAME TO idx_4185d7e2__pop_county_code_old;
ALTER INDEX IF EXISTS idx_4185d7e2__pop_zip RENAME TO idx_4185d7e2__pop_zip_old;
ALTER INDEX IF EXISTS idx_4185d7e2__awarding_agency_id RENAME TO idx_4185d7e2__awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_4185d7e2__funding_agency_id RENAME TO idx_4185d7e2__funding_agency_id_old;
ALTER INDEX IF EXISTS idx_4185d7e2__awarding_toptier_agency_name RENAME TO idx_4185d7e2__awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_4185d7e2__awarding_subtier_agency_name RENAME TO idx_4185d7e2__awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_4185d7e2__funding_toptier_agency_name RENAME TO idx_4185d7e2__funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_4185d7e2__funding_subtier_agency_name RENAME TO idx_4185d7e2__funding_subtier_agency_name_old;

ALTER MATERIALIZED VIEW summary_transaction_geo_view_temp RENAME TO summary_transaction_geo_view;
ALTER INDEX idx_4185d7e2__date_temp RENAME TO idx_4185d7e2__date;
ALTER INDEX idx_4185d7e2__fy_temp RENAME TO idx_4185d7e2__fy;
ALTER INDEX idx_4185d7e2__fy_type_temp RENAME TO idx_4185d7e2__fy_type;
ALTER INDEX idx_4185d7e2__type_temp RENAME TO idx_4185d7e2__type;
ALTER INDEX idx_4185d7e2__pulled_from_temp RENAME TO idx_4185d7e2__pulled_from;
ALTER INDEX idx_4185d7e2__recipient_country_code_temp RENAME TO idx_4185d7e2__recipient_country_code;
ALTER INDEX idx_4185d7e2__recipient_state_code_temp RENAME TO idx_4185d7e2__recipient_state_code;
ALTER INDEX idx_4185d7e2__recipient_county_code_temp RENAME TO idx_4185d7e2__recipient_county_code;
ALTER INDEX idx_4185d7e2__recipient_zip_temp RENAME TO idx_4185d7e2__recipient_zip;
ALTER INDEX idx_4185d7e2__pop_country_code_temp RENAME TO idx_4185d7e2__pop_country_code;
ALTER INDEX idx_4185d7e2__pop_state_code_temp RENAME TO idx_4185d7e2__pop_state_code;
ALTER INDEX idx_4185d7e2__pop_county_code_temp RENAME TO idx_4185d7e2__pop_county_code;
ALTER INDEX idx_4185d7e2__pop_zip_temp RENAME TO idx_4185d7e2__pop_zip;
ALTER INDEX idx_4185d7e2__awarding_agency_id_temp RENAME TO idx_4185d7e2__awarding_agency_id;
ALTER INDEX idx_4185d7e2__funding_agency_id_temp RENAME TO idx_4185d7e2__funding_agency_id;
ALTER INDEX idx_4185d7e2__awarding_toptier_agency_name_temp RENAME TO idx_4185d7e2__awarding_toptier_agency_name;
ALTER INDEX idx_4185d7e2__awarding_subtier_agency_name_temp RENAME TO idx_4185d7e2__awarding_subtier_agency_name;
ALTER INDEX idx_4185d7e2__funding_toptier_agency_name_temp RENAME TO idx_4185d7e2__funding_toptier_agency_name;
ALTER INDEX idx_4185d7e2__funding_subtier_agency_name_temp RENAME TO idx_4185d7e2__funding_subtier_agency_name;
