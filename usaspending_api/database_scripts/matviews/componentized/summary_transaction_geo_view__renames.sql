--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_transaction_geo_view RENAME TO summary_transaction_geo_view_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_unique_pk RENAME TO idx_af8ca7ca$0d1_unique_pk_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_date RENAME TO idx_af8ca7ca$0d1_date_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_fy RENAME TO idx_af8ca7ca$0d1_fy_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_fy_type RENAME TO idx_af8ca7ca$0d1_fy_type_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_type RENAME TO idx_af8ca7ca$0d1_type_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_pulled_from RENAME TO idx_af8ca7ca$0d1_pulled_from_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_recipient_country_code RENAME TO idx_af8ca7ca$0d1_recipient_country_code_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_recipient_state_code RENAME TO idx_af8ca7ca$0d1_recipient_state_code_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_recipient_county_code RENAME TO idx_af8ca7ca$0d1_recipient_county_code_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_recipient_zip RENAME TO idx_af8ca7ca$0d1_recipient_zip_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_pop_country_code RENAME TO idx_af8ca7ca$0d1_pop_country_code_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_pop_state_code RENAME TO idx_af8ca7ca$0d1_pop_state_code_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_pop_county_code RENAME TO idx_af8ca7ca$0d1_pop_county_code_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_pop_zip RENAME TO idx_af8ca7ca$0d1_pop_zip_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_awarding_agency_id RENAME TO idx_af8ca7ca$0d1_awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_funding_agency_id RENAME TO idx_af8ca7ca$0d1_funding_agency_id_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_awarding_toptier_agency_name RENAME TO idx_af8ca7ca$0d1_awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_awarding_subtier_agency_name RENAME TO idx_af8ca7ca$0d1_awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_funding_toptier_agency_name RENAME TO idx_af8ca7ca$0d1_funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_funding_subtier_agency_name RENAME TO idx_af8ca7ca$0d1_funding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_compound_geo_pop_1 RENAME TO idx_af8ca7ca$0d1_compound_geo_pop_1_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_compound_geo_pop_2 RENAME TO idx_af8ca7ca$0d1_compound_geo_pop_2_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_compound_geo_pop_3 RENAME TO idx_af8ca7ca$0d1_compound_geo_pop_3_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_compound_geo_rl_1 RENAME TO idx_af8ca7ca$0d1_compound_geo_rl_1_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_compound_geo_rl_2 RENAME TO idx_af8ca7ca$0d1_compound_geo_rl_2_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_compound_geo_rl_3 RENAME TO idx_af8ca7ca$0d1_compound_geo_rl_3_old;

ALTER MATERIALIZED VIEW summary_transaction_geo_view_temp RENAME TO summary_transaction_geo_view;
ALTER INDEX idx_af8ca7ca$0d1_unique_pk_temp RENAME TO idx_af8ca7ca$0d1_unique_pk;
ALTER INDEX idx_af8ca7ca$0d1_date_temp RENAME TO idx_af8ca7ca$0d1_date;
ALTER INDEX idx_af8ca7ca$0d1_fy_temp RENAME TO idx_af8ca7ca$0d1_fy;
ALTER INDEX idx_af8ca7ca$0d1_fy_type_temp RENAME TO idx_af8ca7ca$0d1_fy_type;
ALTER INDEX idx_af8ca7ca$0d1_type_temp RENAME TO idx_af8ca7ca$0d1_type;
ALTER INDEX idx_af8ca7ca$0d1_pulled_from_temp RENAME TO idx_af8ca7ca$0d1_pulled_from;
ALTER INDEX idx_af8ca7ca$0d1_recipient_country_code_temp RENAME TO idx_af8ca7ca$0d1_recipient_country_code;
ALTER INDEX idx_af8ca7ca$0d1_recipient_state_code_temp RENAME TO idx_af8ca7ca$0d1_recipient_state_code;
ALTER INDEX idx_af8ca7ca$0d1_recipient_county_code_temp RENAME TO idx_af8ca7ca$0d1_recipient_county_code;
ALTER INDEX idx_af8ca7ca$0d1_recipient_zip_temp RENAME TO idx_af8ca7ca$0d1_recipient_zip;
ALTER INDEX idx_af8ca7ca$0d1_pop_country_code_temp RENAME TO idx_af8ca7ca$0d1_pop_country_code;
ALTER INDEX idx_af8ca7ca$0d1_pop_state_code_temp RENAME TO idx_af8ca7ca$0d1_pop_state_code;
ALTER INDEX idx_af8ca7ca$0d1_pop_county_code_temp RENAME TO idx_af8ca7ca$0d1_pop_county_code;
ALTER INDEX idx_af8ca7ca$0d1_pop_zip_temp RENAME TO idx_af8ca7ca$0d1_pop_zip;
ALTER INDEX idx_af8ca7ca$0d1_awarding_agency_id_temp RENAME TO idx_af8ca7ca$0d1_awarding_agency_id;
ALTER INDEX idx_af8ca7ca$0d1_funding_agency_id_temp RENAME TO idx_af8ca7ca$0d1_funding_agency_id;
ALTER INDEX idx_af8ca7ca$0d1_awarding_toptier_agency_name_temp RENAME TO idx_af8ca7ca$0d1_awarding_toptier_agency_name;
ALTER INDEX idx_af8ca7ca$0d1_awarding_subtier_agency_name_temp RENAME TO idx_af8ca7ca$0d1_awarding_subtier_agency_name;
ALTER INDEX idx_af8ca7ca$0d1_funding_toptier_agency_name_temp RENAME TO idx_af8ca7ca$0d1_funding_toptier_agency_name;
ALTER INDEX idx_af8ca7ca$0d1_funding_subtier_agency_name_temp RENAME TO idx_af8ca7ca$0d1_funding_subtier_agency_name;
ALTER INDEX idx_af8ca7ca$0d1_compound_geo_pop_1_temp RENAME TO idx_af8ca7ca$0d1_compound_geo_pop_1;
ALTER INDEX idx_af8ca7ca$0d1_compound_geo_pop_2_temp RENAME TO idx_af8ca7ca$0d1_compound_geo_pop_2;
ALTER INDEX idx_af8ca7ca$0d1_compound_geo_pop_3_temp RENAME TO idx_af8ca7ca$0d1_compound_geo_pop_3;
ALTER INDEX idx_af8ca7ca$0d1_compound_geo_rl_1_temp RENAME TO idx_af8ca7ca$0d1_compound_geo_rl_1;
ALTER INDEX idx_af8ca7ca$0d1_compound_geo_rl_2_temp RENAME TO idx_af8ca7ca$0d1_compound_geo_rl_2;
ALTER INDEX idx_af8ca7ca$0d1_compound_geo_rl_3_temp RENAME TO idx_af8ca7ca$0d1_compound_geo_rl_3;
