--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--         !!DO NOT DIRECTLY EDIT THIS FILE!!         --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_transaction_view RENAME TO summary_transaction_view_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_unique_pk RENAME TO idx_c2817a5a$fed_unique_pk_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_ordered_action_date RENAME TO idx_c2817a5a$fed_ordered_action_date_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_ordered_fy RENAME TO idx_c2817a5a$fed_ordered_fy_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_ordered_fy_type RENAME TO idx_c2817a5a$fed_ordered_fy_type_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_type RENAME TO idx_c2817a5a$fed_type_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_pulled_from RENAME TO idx_c2817a5a$fed_pulled_from_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_recipient_country_code RENAME TO idx_c2817a5a$fed_recipient_country_code_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_recipient_state_code RENAME TO idx_c2817a5a$fed_recipient_state_code_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_recipient_county_code RENAME TO idx_c2817a5a$fed_recipient_county_code_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_recipient_zip RENAME TO idx_c2817a5a$fed_recipient_zip_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_pop_country_code RENAME TO idx_c2817a5a$fed_pop_country_code_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_pop_state_code RENAME TO idx_c2817a5a$fed_pop_state_code_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_pop_county_code RENAME TO idx_c2817a5a$fed_pop_county_code_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_pop_zip RENAME TO idx_c2817a5a$fed_pop_zip_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_awarding_agency_id RENAME TO idx_c2817a5a$fed_awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_funding_agency_id RENAME TO idx_c2817a5a$fed_funding_agency_id_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_awarding_toptier_agency_name RENAME TO idx_c2817a5a$fed_awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_awarding_subtier_agency_name RENAME TO idx_c2817a5a$fed_awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_funding_toptier_agency_name RENAME TO idx_c2817a5a$fed_funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_funding_subtier_agency_name RENAME TO idx_c2817a5a$fed_funding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_cfda_num RENAME TO idx_c2817a5a$fed_cfda_num_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_cfda_title RENAME TO idx_c2817a5a$fed_cfda_title_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_psc RENAME TO idx_c2817a5a$fed_psc_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_naics RENAME TO idx_c2817a5a$fed_naics_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_total_obl_bin RENAME TO idx_c2817a5a$fed_total_obl_bin_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_type_of_contract RENAME TO idx_c2817a5a$fed_type_of_contract_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_ordered_fy_set_aside RENAME TO idx_c2817a5a$fed_ordered_fy_set_aside_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_extent_competed RENAME TO idx_c2817a5a$fed_extent_competed_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_type_set_aside RENAME TO idx_c2817a5a$fed_type_set_aside_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_business_categories RENAME TO idx_c2817a5a$fed_business_categories_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_compound_geo_pop_1 RENAME TO idx_c2817a5a$fed_compound_geo_pop_1_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_compound_geo_pop_2 RENAME TO idx_c2817a5a$fed_compound_geo_pop_2_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_compound_geo_pop_3 RENAME TO idx_c2817a5a$fed_compound_geo_pop_3_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_compound_geo_rl_1 RENAME TO idx_c2817a5a$fed_compound_geo_rl_1_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_compound_geo_rl_2 RENAME TO idx_c2817a5a$fed_compound_geo_rl_2_old;
ALTER INDEX IF EXISTS idx_c2817a5a$fed_compound_geo_rl_3 RENAME TO idx_c2817a5a$fed_compound_geo_rl_3_old;

ALTER MATERIALIZED VIEW summary_transaction_view_temp RENAME TO summary_transaction_view;
ALTER INDEX idx_c2817a5a$fed_unique_pk_temp RENAME TO idx_c2817a5a$fed_unique_pk;
ALTER INDEX idx_c2817a5a$fed_ordered_action_date_temp RENAME TO idx_c2817a5a$fed_ordered_action_date;
ALTER INDEX idx_c2817a5a$fed_ordered_fy_temp RENAME TO idx_c2817a5a$fed_ordered_fy;
ALTER INDEX idx_c2817a5a$fed_ordered_fy_type_temp RENAME TO idx_c2817a5a$fed_ordered_fy_type;
ALTER INDEX idx_c2817a5a$fed_type_temp RENAME TO idx_c2817a5a$fed_type;
ALTER INDEX idx_c2817a5a$fed_pulled_from_temp RENAME TO idx_c2817a5a$fed_pulled_from;
ALTER INDEX idx_c2817a5a$fed_recipient_country_code_temp RENAME TO idx_c2817a5a$fed_recipient_country_code;
ALTER INDEX idx_c2817a5a$fed_recipient_state_code_temp RENAME TO idx_c2817a5a$fed_recipient_state_code;
ALTER INDEX idx_c2817a5a$fed_recipient_county_code_temp RENAME TO idx_c2817a5a$fed_recipient_county_code;
ALTER INDEX idx_c2817a5a$fed_recipient_zip_temp RENAME TO idx_c2817a5a$fed_recipient_zip;
ALTER INDEX idx_c2817a5a$fed_pop_country_code_temp RENAME TO idx_c2817a5a$fed_pop_country_code;
ALTER INDEX idx_c2817a5a$fed_pop_state_code_temp RENAME TO idx_c2817a5a$fed_pop_state_code;
ALTER INDEX idx_c2817a5a$fed_pop_county_code_temp RENAME TO idx_c2817a5a$fed_pop_county_code;
ALTER INDEX idx_c2817a5a$fed_pop_zip_temp RENAME TO idx_c2817a5a$fed_pop_zip;
ALTER INDEX idx_c2817a5a$fed_awarding_agency_id_temp RENAME TO idx_c2817a5a$fed_awarding_agency_id;
ALTER INDEX idx_c2817a5a$fed_funding_agency_id_temp RENAME TO idx_c2817a5a$fed_funding_agency_id;
ALTER INDEX idx_c2817a5a$fed_awarding_toptier_agency_name_temp RENAME TO idx_c2817a5a$fed_awarding_toptier_agency_name;
ALTER INDEX idx_c2817a5a$fed_awarding_subtier_agency_name_temp RENAME TO idx_c2817a5a$fed_awarding_subtier_agency_name;
ALTER INDEX idx_c2817a5a$fed_funding_toptier_agency_name_temp RENAME TO idx_c2817a5a$fed_funding_toptier_agency_name;
ALTER INDEX idx_c2817a5a$fed_funding_subtier_agency_name_temp RENAME TO idx_c2817a5a$fed_funding_subtier_agency_name;
ALTER INDEX idx_c2817a5a$fed_cfda_num_temp RENAME TO idx_c2817a5a$fed_cfda_num;
ALTER INDEX idx_c2817a5a$fed_cfda_title_temp RENAME TO idx_c2817a5a$fed_cfda_title;
ALTER INDEX idx_c2817a5a$fed_psc_temp RENAME TO idx_c2817a5a$fed_psc;
ALTER INDEX idx_c2817a5a$fed_naics_temp RENAME TO idx_c2817a5a$fed_naics;
ALTER INDEX idx_c2817a5a$fed_total_obl_bin_temp RENAME TO idx_c2817a5a$fed_total_obl_bin;
ALTER INDEX idx_c2817a5a$fed_type_of_contract_temp RENAME TO idx_c2817a5a$fed_type_of_contract;
ALTER INDEX idx_c2817a5a$fed_ordered_fy_set_aside_temp RENAME TO idx_c2817a5a$fed_ordered_fy_set_aside;
ALTER INDEX idx_c2817a5a$fed_extent_competed_temp RENAME TO idx_c2817a5a$fed_extent_competed;
ALTER INDEX idx_c2817a5a$fed_type_set_aside_temp RENAME TO idx_c2817a5a$fed_type_set_aside;
ALTER INDEX idx_c2817a5a$fed_business_categories_temp RENAME TO idx_c2817a5a$fed_business_categories;
ALTER INDEX idx_c2817a5a$fed_compound_geo_pop_1_temp RENAME TO idx_c2817a5a$fed_compound_geo_pop_1;
ALTER INDEX idx_c2817a5a$fed_compound_geo_pop_2_temp RENAME TO idx_c2817a5a$fed_compound_geo_pop_2;
ALTER INDEX idx_c2817a5a$fed_compound_geo_pop_3_temp RENAME TO idx_c2817a5a$fed_compound_geo_pop_3;
ALTER INDEX idx_c2817a5a$fed_compound_geo_rl_1_temp RENAME TO idx_c2817a5a$fed_compound_geo_rl_1;
ALTER INDEX idx_c2817a5a$fed_compound_geo_rl_2_temp RENAME TO idx_c2817a5a$fed_compound_geo_rl_2;
ALTER INDEX idx_c2817a5a$fed_compound_geo_rl_3_temp RENAME TO idx_c2817a5a$fed_compound_geo_rl_3;
