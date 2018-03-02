--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_transaction_geo_view RENAME TO summary_transaction_geo_view_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__date RENAME TO idx_bf3a3d11__date_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__fy RENAME TO idx_bf3a3d11__fy_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__fy_type RENAME TO idx_bf3a3d11__fy_type_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__type RENAME TO idx_bf3a3d11__type_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__pulled_from RENAME TO idx_bf3a3d11__pulled_from_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__recipient_country_code RENAME TO idx_bf3a3d11__recipient_country_code_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__recipient_state_code RENAME TO idx_bf3a3d11__recipient_state_code_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__recipient_county_code RENAME TO idx_bf3a3d11__recipient_county_code_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__recipient_zip RENAME TO idx_bf3a3d11__recipient_zip_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__pop_country_code RENAME TO idx_bf3a3d11__pop_country_code_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__pop_state_code RENAME TO idx_bf3a3d11__pop_state_code_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__pop_county_code RENAME TO idx_bf3a3d11__pop_county_code_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__pop_zip RENAME TO idx_bf3a3d11__pop_zip_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__awarding_agency_id RENAME TO idx_bf3a3d11__awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__funding_agency_id RENAME TO idx_bf3a3d11__funding_agency_id_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__awarding_toptier_agency_name RENAME TO idx_bf3a3d11__awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__awarding_subtier_agency_name RENAME TO idx_bf3a3d11__awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__funding_toptier_agency_name RENAME TO idx_bf3a3d11__funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__funding_subtier_agency_name RENAME TO idx_bf3a3d11__funding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__tuned_type_and_idv RENAME TO idx_bf3a3d11__tuned_type_and_idv_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__compound_geo_pop_1 RENAME TO idx_bf3a3d11__compound_geo_pop_1_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__compound_geo_pop_2 RENAME TO idx_bf3a3d11__compound_geo_pop_2_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__compound_geo_pop_3 RENAME TO idx_bf3a3d11__compound_geo_pop_3_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__compound_geo_rl_1 RENAME TO idx_bf3a3d11__compound_geo_rl_1_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__compound_geo_rl_2 RENAME TO idx_bf3a3d11__compound_geo_rl_2_old;
ALTER INDEX IF EXISTS idx_bf3a3d11__compound_geo_rl_3 RENAME TO idx_bf3a3d11__compound_geo_rl_3_old;

ALTER MATERIALIZED VIEW summary_transaction_geo_view_temp RENAME TO summary_transaction_geo_view;
ALTER INDEX idx_bf3a3d11__date_temp RENAME TO idx_bf3a3d11__date;
ALTER INDEX idx_bf3a3d11__fy_temp RENAME TO idx_bf3a3d11__fy;
ALTER INDEX idx_bf3a3d11__fy_type_temp RENAME TO idx_bf3a3d11__fy_type;
ALTER INDEX idx_bf3a3d11__type_temp RENAME TO idx_bf3a3d11__type;
ALTER INDEX idx_bf3a3d11__pulled_from_temp RENAME TO idx_bf3a3d11__pulled_from;
ALTER INDEX idx_bf3a3d11__recipient_country_code_temp RENAME TO idx_bf3a3d11__recipient_country_code;
ALTER INDEX idx_bf3a3d11__recipient_state_code_temp RENAME TO idx_bf3a3d11__recipient_state_code;
ALTER INDEX idx_bf3a3d11__recipient_county_code_temp RENAME TO idx_bf3a3d11__recipient_county_code;
ALTER INDEX idx_bf3a3d11__recipient_zip_temp RENAME TO idx_bf3a3d11__recipient_zip;
ALTER INDEX idx_bf3a3d11__pop_country_code_temp RENAME TO idx_bf3a3d11__pop_country_code;
ALTER INDEX idx_bf3a3d11__pop_state_code_temp RENAME TO idx_bf3a3d11__pop_state_code;
ALTER INDEX idx_bf3a3d11__pop_county_code_temp RENAME TO idx_bf3a3d11__pop_county_code;
ALTER INDEX idx_bf3a3d11__pop_zip_temp RENAME TO idx_bf3a3d11__pop_zip;
ALTER INDEX idx_bf3a3d11__awarding_agency_id_temp RENAME TO idx_bf3a3d11__awarding_agency_id;
ALTER INDEX idx_bf3a3d11__funding_agency_id_temp RENAME TO idx_bf3a3d11__funding_agency_id;
ALTER INDEX idx_bf3a3d11__awarding_toptier_agency_name_temp RENAME TO idx_bf3a3d11__awarding_toptier_agency_name;
ALTER INDEX idx_bf3a3d11__awarding_subtier_agency_name_temp RENAME TO idx_bf3a3d11__awarding_subtier_agency_name;
ALTER INDEX idx_bf3a3d11__funding_toptier_agency_name_temp RENAME TO idx_bf3a3d11__funding_toptier_agency_name;
ALTER INDEX idx_bf3a3d11__funding_subtier_agency_name_temp RENAME TO idx_bf3a3d11__funding_subtier_agency_name;
ALTER INDEX idx_bf3a3d11__tuned_type_and_idv_temp RENAME TO idx_bf3a3d11__tuned_type_and_idv;
ALTER INDEX idx_bf3a3d11__compound_geo_pop_1_temp RENAME TO idx_bf3a3d11__compound_geo_pop_1;
ALTER INDEX idx_bf3a3d11__compound_geo_pop_2_temp RENAME TO idx_bf3a3d11__compound_geo_pop_2;
ALTER INDEX idx_bf3a3d11__compound_geo_pop_3_temp RENAME TO idx_bf3a3d11__compound_geo_pop_3;
ALTER INDEX idx_bf3a3d11__compound_geo_rl_1_temp RENAME TO idx_bf3a3d11__compound_geo_rl_1;
ALTER INDEX idx_bf3a3d11__compound_geo_rl_2_temp RENAME TO idx_bf3a3d11__compound_geo_rl_2;
ALTER INDEX idx_bf3a3d11__compound_geo_rl_3_temp RENAME TO idx_bf3a3d11__compound_geo_rl_3;
