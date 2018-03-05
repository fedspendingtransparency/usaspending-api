--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_transaction_view RENAME TO summary_transaction_view_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__unique_pk RENAME TO idx_f1967b82$4cb__unique_pk_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__ordered_action_date RENAME TO idx_f1967b82$4cb__ordered_action_date_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__ordered_fy RENAME TO idx_f1967b82$4cb__ordered_fy_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__ordered_fy_type RENAME TO idx_f1967b82$4cb__ordered_fy_type_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__type RENAME TO idx_f1967b82$4cb__type_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__pulled_from RENAME TO idx_f1967b82$4cb__pulled_from_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__recipient_country_code RENAME TO idx_f1967b82$4cb__recipient_country_code_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__recipient_state_code RENAME TO idx_f1967b82$4cb__recipient_state_code_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__recipient_county_code RENAME TO idx_f1967b82$4cb__recipient_county_code_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__recipient_zip RENAME TO idx_f1967b82$4cb__recipient_zip_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__pop_country_code RENAME TO idx_f1967b82$4cb__pop_country_code_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__pop_state_code RENAME TO idx_f1967b82$4cb__pop_state_code_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__pop_county_code RENAME TO idx_f1967b82$4cb__pop_county_code_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__pop_zip RENAME TO idx_f1967b82$4cb__pop_zip_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__awarding_agency_id RENAME TO idx_f1967b82$4cb__awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__funding_agency_id RENAME TO idx_f1967b82$4cb__funding_agency_id_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__awarding_toptier_agency_name RENAME TO idx_f1967b82$4cb__awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__awarding_subtier_agency_name RENAME TO idx_f1967b82$4cb__awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__funding_toptier_agency_name RENAME TO idx_f1967b82$4cb__funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__funding_subtier_agency_name RENAME TO idx_f1967b82$4cb__funding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__cfda_num RENAME TO idx_f1967b82$4cb__cfda_num_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__cfda_title RENAME TO idx_f1967b82$4cb__cfda_title_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__psc RENAME TO idx_f1967b82$4cb__psc_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__naics RENAME TO idx_f1967b82$4cb__naics_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__total_obl_bin RENAME TO idx_f1967b82$4cb__total_obl_bin_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__type_of_contract RENAME TO idx_f1967b82$4cb__type_of_contract_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__ordered_fy_set_aside RENAME TO idx_f1967b82$4cb__ordered_fy_set_aside_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__extent_competed RENAME TO idx_f1967b82$4cb__extent_competed_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__type_set_aside RENAME TO idx_f1967b82$4cb__type_set_aside_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__business_categories RENAME TO idx_f1967b82$4cb__business_categories_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__tuned_type_and_idv RENAME TO idx_f1967b82$4cb__tuned_type_and_idv_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__compound_geo_pop_1 RENAME TO idx_f1967b82$4cb__compound_geo_pop_1_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__compound_geo_pop_2 RENAME TO idx_f1967b82$4cb__compound_geo_pop_2_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__compound_geo_pop_3 RENAME TO idx_f1967b82$4cb__compound_geo_pop_3_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__compound_geo_rl_1 RENAME TO idx_f1967b82$4cb__compound_geo_rl_1_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__compound_geo_rl_2 RENAME TO idx_f1967b82$4cb__compound_geo_rl_2_old;
ALTER INDEX IF EXISTS idx_f1967b82$4cb__compound_geo_rl_3 RENAME TO idx_f1967b82$4cb__compound_geo_rl_3_old;

ALTER MATERIALIZED VIEW summary_transaction_view_temp RENAME TO summary_transaction_view;
ALTER INDEX idx_f1967b82$4cb__unique_pk_temp RENAME TO idx_f1967b82$4cb__unique_pk;
ALTER INDEX idx_f1967b82$4cb__ordered_action_date_temp RENAME TO idx_f1967b82$4cb__ordered_action_date;
ALTER INDEX idx_f1967b82$4cb__ordered_fy_temp RENAME TO idx_f1967b82$4cb__ordered_fy;
ALTER INDEX idx_f1967b82$4cb__ordered_fy_type_temp RENAME TO idx_f1967b82$4cb__ordered_fy_type;
ALTER INDEX idx_f1967b82$4cb__type_temp RENAME TO idx_f1967b82$4cb__type;
ALTER INDEX idx_f1967b82$4cb__pulled_from_temp RENAME TO idx_f1967b82$4cb__pulled_from;
ALTER INDEX idx_f1967b82$4cb__recipient_country_code_temp RENAME TO idx_f1967b82$4cb__recipient_country_code;
ALTER INDEX idx_f1967b82$4cb__recipient_state_code_temp RENAME TO idx_f1967b82$4cb__recipient_state_code;
ALTER INDEX idx_f1967b82$4cb__recipient_county_code_temp RENAME TO idx_f1967b82$4cb__recipient_county_code;
ALTER INDEX idx_f1967b82$4cb__recipient_zip_temp RENAME TO idx_f1967b82$4cb__recipient_zip;
ALTER INDEX idx_f1967b82$4cb__pop_country_code_temp RENAME TO idx_f1967b82$4cb__pop_country_code;
ALTER INDEX idx_f1967b82$4cb__pop_state_code_temp RENAME TO idx_f1967b82$4cb__pop_state_code;
ALTER INDEX idx_f1967b82$4cb__pop_county_code_temp RENAME TO idx_f1967b82$4cb__pop_county_code;
ALTER INDEX idx_f1967b82$4cb__pop_zip_temp RENAME TO idx_f1967b82$4cb__pop_zip;
ALTER INDEX idx_f1967b82$4cb__awarding_agency_id_temp RENAME TO idx_f1967b82$4cb__awarding_agency_id;
ALTER INDEX idx_f1967b82$4cb__funding_agency_id_temp RENAME TO idx_f1967b82$4cb__funding_agency_id;
ALTER INDEX idx_f1967b82$4cb__awarding_toptier_agency_name_temp RENAME TO idx_f1967b82$4cb__awarding_toptier_agency_name;
ALTER INDEX idx_f1967b82$4cb__awarding_subtier_agency_name_temp RENAME TO idx_f1967b82$4cb__awarding_subtier_agency_name;
ALTER INDEX idx_f1967b82$4cb__funding_toptier_agency_name_temp RENAME TO idx_f1967b82$4cb__funding_toptier_agency_name;
ALTER INDEX idx_f1967b82$4cb__funding_subtier_agency_name_temp RENAME TO idx_f1967b82$4cb__funding_subtier_agency_name;
ALTER INDEX idx_f1967b82$4cb__cfda_num_temp RENAME TO idx_f1967b82$4cb__cfda_num;
ALTER INDEX idx_f1967b82$4cb__cfda_title_temp RENAME TO idx_f1967b82$4cb__cfda_title;
ALTER INDEX idx_f1967b82$4cb__psc_temp RENAME TO idx_f1967b82$4cb__psc;
ALTER INDEX idx_f1967b82$4cb__naics_temp RENAME TO idx_f1967b82$4cb__naics;
ALTER INDEX idx_f1967b82$4cb__total_obl_bin_temp RENAME TO idx_f1967b82$4cb__total_obl_bin;
ALTER INDEX idx_f1967b82$4cb__type_of_contract_temp RENAME TO idx_f1967b82$4cb__type_of_contract;
ALTER INDEX idx_f1967b82$4cb__ordered_fy_set_aside_temp RENAME TO idx_f1967b82$4cb__ordered_fy_set_aside;
ALTER INDEX idx_f1967b82$4cb__extent_competed_temp RENAME TO idx_f1967b82$4cb__extent_competed;
ALTER INDEX idx_f1967b82$4cb__type_set_aside_temp RENAME TO idx_f1967b82$4cb__type_set_aside;
ALTER INDEX idx_f1967b82$4cb__business_categories_temp RENAME TO idx_f1967b82$4cb__business_categories;
ALTER INDEX idx_f1967b82$4cb__tuned_type_and_idv_temp RENAME TO idx_f1967b82$4cb__tuned_type_and_idv;
ALTER INDEX idx_f1967b82$4cb__compound_geo_pop_1_temp RENAME TO idx_f1967b82$4cb__compound_geo_pop_1;
ALTER INDEX idx_f1967b82$4cb__compound_geo_pop_2_temp RENAME TO idx_f1967b82$4cb__compound_geo_pop_2;
ALTER INDEX idx_f1967b82$4cb__compound_geo_pop_3_temp RENAME TO idx_f1967b82$4cb__compound_geo_pop_3;
ALTER INDEX idx_f1967b82$4cb__compound_geo_rl_1_temp RENAME TO idx_f1967b82$4cb__compound_geo_rl_1;
ALTER INDEX idx_f1967b82$4cb__compound_geo_rl_2_temp RENAME TO idx_f1967b82$4cb__compound_geo_rl_2;
ALTER INDEX idx_f1967b82$4cb__compound_geo_rl_3_temp RENAME TO idx_f1967b82$4cb__compound_geo_rl_3;
