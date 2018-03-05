--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
ALTER MATERIALIZED VIEW IF EXISTS summary_transaction_month_view RENAME TO summary_transaction_month_view_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__unique_pk RENAME TO idx_f1967b82$a69__unique_pk_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__date RENAME TO idx_f1967b82$a69__date_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__fy RENAME TO idx_f1967b82$a69__fy_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__fy_type RENAME TO idx_f1967b82$a69__fy_type_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__type RENAME TO idx_f1967b82$a69__type_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__pulled_from RENAME TO idx_f1967b82$a69__pulled_from_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__recipient_country_code RENAME TO idx_f1967b82$a69__recipient_country_code_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__recipient_state_code RENAME TO idx_f1967b82$a69__recipient_state_code_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__recipient_county_code RENAME TO idx_f1967b82$a69__recipient_county_code_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__recipient_zip RENAME TO idx_f1967b82$a69__recipient_zip_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__pop_country_code RENAME TO idx_f1967b82$a69__pop_country_code_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__pop_state_code RENAME TO idx_f1967b82$a69__pop_state_code_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__pop_county_code RENAME TO idx_f1967b82$a69__pop_county_code_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__pop_zip RENAME TO idx_f1967b82$a69__pop_zip_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__awarding_agency_id RENAME TO idx_f1967b82$a69__awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__funding_agency_id RENAME TO idx_f1967b82$a69__funding_agency_id_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__awarding_toptier_agency_name RENAME TO idx_f1967b82$a69__awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__awarding_subtier_agency_name RENAME TO idx_f1967b82$a69__awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__funding_toptier_agency_name RENAME TO idx_f1967b82$a69__funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__funding_subtier_agency_name RENAME TO idx_f1967b82$a69__funding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__cfda_number RENAME TO idx_f1967b82$a69__cfda_number_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__cfda_title RENAME TO idx_f1967b82$a69__cfda_title_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__psc RENAME TO idx_f1967b82$a69__psc_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__naics RENAME TO idx_f1967b82$a69__naics_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__total_obl_bin RENAME TO idx_f1967b82$a69__total_obl_bin_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__type_of_contract RENAME TO idx_f1967b82$a69__type_of_contract_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__fy_set_aside RENAME TO idx_f1967b82$a69__fy_set_aside_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__extent_competed RENAME TO idx_f1967b82$a69__extent_competed_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__type_set_aside RENAME TO idx_f1967b82$a69__type_set_aside_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__business_categories RENAME TO idx_f1967b82$a69__business_categories_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__tuned_type_and_idv RENAME TO idx_f1967b82$a69__tuned_type_and_idv_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__compound_geo_pop_1 RENAME TO idx_f1967b82$a69__compound_geo_pop_1_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__compound_geo_pop_2 RENAME TO idx_f1967b82$a69__compound_geo_pop_2_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__compound_geo_pop_3 RENAME TO idx_f1967b82$a69__compound_geo_pop_3_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__compound_geo_rl_1 RENAME TO idx_f1967b82$a69__compound_geo_rl_1_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__compound_geo_rl_2 RENAME TO idx_f1967b82$a69__compound_geo_rl_2_old;
ALTER INDEX IF EXISTS idx_f1967b82$a69__compound_geo_rl_3 RENAME TO idx_f1967b82$a69__compound_geo_rl_3_old;

ALTER MATERIALIZED VIEW summary_transaction_month_view_temp RENAME TO summary_transaction_month_view;
ALTER INDEX idx_f1967b82$a69__unique_pk_temp RENAME TO idx_f1967b82$a69__unique_pk;
ALTER INDEX idx_f1967b82$a69__date_temp RENAME TO idx_f1967b82$a69__date;
ALTER INDEX idx_f1967b82$a69__fy_temp RENAME TO idx_f1967b82$a69__fy;
ALTER INDEX idx_f1967b82$a69__fy_type_temp RENAME TO idx_f1967b82$a69__fy_type;
ALTER INDEX idx_f1967b82$a69__type_temp RENAME TO idx_f1967b82$a69__type;
ALTER INDEX idx_f1967b82$a69__pulled_from_temp RENAME TO idx_f1967b82$a69__pulled_from;
ALTER INDEX idx_f1967b82$a69__recipient_country_code_temp RENAME TO idx_f1967b82$a69__recipient_country_code;
ALTER INDEX idx_f1967b82$a69__recipient_state_code_temp RENAME TO idx_f1967b82$a69__recipient_state_code;
ALTER INDEX idx_f1967b82$a69__recipient_county_code_temp RENAME TO idx_f1967b82$a69__recipient_county_code;
ALTER INDEX idx_f1967b82$a69__recipient_zip_temp RENAME TO idx_f1967b82$a69__recipient_zip;
ALTER INDEX idx_f1967b82$a69__pop_country_code_temp RENAME TO idx_f1967b82$a69__pop_country_code;
ALTER INDEX idx_f1967b82$a69__pop_state_code_temp RENAME TO idx_f1967b82$a69__pop_state_code;
ALTER INDEX idx_f1967b82$a69__pop_county_code_temp RENAME TO idx_f1967b82$a69__pop_county_code;
ALTER INDEX idx_f1967b82$a69__pop_zip_temp RENAME TO idx_f1967b82$a69__pop_zip;
ALTER INDEX idx_f1967b82$a69__awarding_agency_id_temp RENAME TO idx_f1967b82$a69__awarding_agency_id;
ALTER INDEX idx_f1967b82$a69__funding_agency_id_temp RENAME TO idx_f1967b82$a69__funding_agency_id;
ALTER INDEX idx_f1967b82$a69__awarding_toptier_agency_name_temp RENAME TO idx_f1967b82$a69__awarding_toptier_agency_name;
ALTER INDEX idx_f1967b82$a69__awarding_subtier_agency_name_temp RENAME TO idx_f1967b82$a69__awarding_subtier_agency_name;
ALTER INDEX idx_f1967b82$a69__funding_toptier_agency_name_temp RENAME TO idx_f1967b82$a69__funding_toptier_agency_name;
ALTER INDEX idx_f1967b82$a69__funding_subtier_agency_name_temp RENAME TO idx_f1967b82$a69__funding_subtier_agency_name;
ALTER INDEX idx_f1967b82$a69__cfda_number_temp RENAME TO idx_f1967b82$a69__cfda_number;
ALTER INDEX idx_f1967b82$a69__cfda_title_temp RENAME TO idx_f1967b82$a69__cfda_title;
ALTER INDEX idx_f1967b82$a69__psc_temp RENAME TO idx_f1967b82$a69__psc;
ALTER INDEX idx_f1967b82$a69__naics_temp RENAME TO idx_f1967b82$a69__naics;
ALTER INDEX idx_f1967b82$a69__total_obl_bin_temp RENAME TO idx_f1967b82$a69__total_obl_bin;
ALTER INDEX idx_f1967b82$a69__type_of_contract_temp RENAME TO idx_f1967b82$a69__type_of_contract;
ALTER INDEX idx_f1967b82$a69__fy_set_aside_temp RENAME TO idx_f1967b82$a69__fy_set_aside;
ALTER INDEX idx_f1967b82$a69__extent_competed_temp RENAME TO idx_f1967b82$a69__extent_competed;
ALTER INDEX idx_f1967b82$a69__type_set_aside_temp RENAME TO idx_f1967b82$a69__type_set_aside;
ALTER INDEX idx_f1967b82$a69__business_categories_temp RENAME TO idx_f1967b82$a69__business_categories;
ALTER INDEX idx_f1967b82$a69__tuned_type_and_idv_temp RENAME TO idx_f1967b82$a69__tuned_type_and_idv;
ALTER INDEX idx_f1967b82$a69__compound_geo_pop_1_temp RENAME TO idx_f1967b82$a69__compound_geo_pop_1;
ALTER INDEX idx_f1967b82$a69__compound_geo_pop_2_temp RENAME TO idx_f1967b82$a69__compound_geo_pop_2;
ALTER INDEX idx_f1967b82$a69__compound_geo_pop_3_temp RENAME TO idx_f1967b82$a69__compound_geo_pop_3;
ALTER INDEX idx_f1967b82$a69__compound_geo_rl_1_temp RENAME TO idx_f1967b82$a69__compound_geo_rl_1;
ALTER INDEX idx_f1967b82$a69__compound_geo_rl_2_temp RENAME TO idx_f1967b82$a69__compound_geo_rl_2;
ALTER INDEX idx_f1967b82$a69__compound_geo_rl_3_temp RENAME TO idx_f1967b82$a69__compound_geo_rl_3;
