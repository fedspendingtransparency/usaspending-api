--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--         !!DO NOT DIRECTLY EDIT THIS FILE!!         --
--------------------------------------------------------
CREATE UNIQUE INDEX idx_cf458597$9e8_id_temp ON universal_award_matview_temp USING BTREE("award_id") WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_category_temp ON universal_award_matview_temp USING BTREE("category") WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_type_temp ON universal_award_matview_temp USING BTREE("type") WITH (fillfactor = 97) WHERE "type" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_ordered_type_temp ON universal_award_matview_temp USING BTREE("type" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_ordered_type_desc_temp ON universal_award_matview_temp USING BTREE("type_description" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_ordered_fain_temp ON universal_award_matview_temp USING BTREE(UPPER("fain") DESC NULLS LAST) WITH (fillfactor = 97) WHERE "fain" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_ordered_piid_temp ON universal_award_matview_temp USING BTREE(UPPER("piid") DESC NULLS LAST) WITH (fillfactor = 97) WHERE "piid" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_total_obligation_temp ON universal_award_matview_temp USING BTREE("total_obligation") WITH (fillfactor = 97) WHERE "total_obligation" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_ordered_total_obligation_temp ON universal_award_matview_temp USING BTREE("total_obligation" DESC) WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_total_obl_bin_temp ON universal_award_matview_temp USING BTREE("total_obl_bin") WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_total_subsidy_cost_temp ON universal_award_matview_temp USING BTREE("total_subsidy_cost") WITH (fillfactor = 97) WHERE "total_subsidy_cost" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_total_loan_value_temp ON universal_award_matview_temp USING BTREE("total_loan_value") WITH (fillfactor = 97) WHERE "total_loan_value" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_ordered_total_subsidy_cost_temp ON universal_award_matview_temp USING BTREE("total_subsidy_cost" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_ordered_total_loan_value_temp ON universal_award_matview_temp USING BTREE("total_loan_value" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_period_of_performance_start_date_temp ON universal_award_matview_temp USING BTREE("period_of_performance_start_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_period_of_performance_current_end_date_temp ON universal_award_matview_temp USING BTREE("period_of_performance_current_end_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_gin_recipient_name_temp ON universal_award_matview_temp USING GIN("recipient_name" gin_trgm_ops);
CREATE INDEX idx_cf458597$9e8_recipient_name_temp ON universal_award_matview_temp USING BTREE("recipient_name") WITH (fillfactor = 97) WHERE "recipient_name" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_recipient_unique_id_temp ON universal_award_matview_temp USING BTREE("recipient_unique_id") WITH (fillfactor = 97) WHERE "recipient_unique_id" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_parent_recipient_unique_id_temp ON universal_award_matview_temp USING BTREE("parent_recipient_unique_id") WITH (fillfactor = 97) WHERE "parent_recipient_unique_id" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_action_date_temp ON universal_award_matview_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_fiscal_year_temp ON universal_award_matview_temp USING BTREE("fiscal_year" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_awarding_agency_id_temp ON universal_award_matview_temp USING BTREE("awarding_agency_id" ASC NULLS LAST) WITH (fillfactor = 97) WHERE "awarding_agency_id" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_funding_agency_id_temp ON universal_award_matview_temp USING BTREE("funding_agency_id" ASC NULLS LAST) WITH (fillfactor = 97) WHERE "funding_agency_id" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_ordered_awarding_toptier_agency_name_temp ON universal_award_matview_temp USING BTREE("awarding_toptier_agency_name" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_ordered_awarding_subtier_agency_name_temp ON universal_award_matview_temp USING BTREE("awarding_subtier_agency_name" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_awarding_toptier_agency_name_temp ON universal_award_matview_temp USING BTREE("awarding_toptier_agency_name") WITH (fillfactor = 97) WHERE "awarding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_awarding_subtier_agency_name_temp ON universal_award_matview_temp USING BTREE("awarding_subtier_agency_name") WITH (fillfactor = 97) WHERE "awarding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_funding_toptier_agency_name_temp ON universal_award_matview_temp USING BTREE("funding_toptier_agency_name") WITH (fillfactor = 97) WHERE "funding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_funding_subtier_agency_name_temp ON universal_award_matview_temp USING BTREE("funding_subtier_agency_name") WITH (fillfactor = 97) WHERE "funding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_recipient_location_country_code_temp ON universal_award_matview_temp USING BTREE("recipient_location_country_code") WITH (fillfactor = 97) WHERE "recipient_location_country_code" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_recipient_location_state_code_temp ON universal_award_matview_temp USING BTREE("recipient_location_state_code") WITH (fillfactor = 97) WHERE "recipient_location_state_code" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_recipient_location_county_code_temp ON universal_award_matview_temp USING BTREE("recipient_location_county_code") WITH (fillfactor = 97) WHERE "recipient_location_county_code" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_recipient_location_zip5_temp ON universal_award_matview_temp USING BTREE("recipient_location_zip5") WITH (fillfactor = 97) WHERE "recipient_location_zip5" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_recipient_location_cong_code_temp ON universal_award_matview_temp USING BTREE("recipient_location_congressional_code") WITH (fillfactor = 97) WHERE "recipient_location_congressional_code" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_pop_country_code_temp ON universal_award_matview_temp USING BTREE("pop_country_code") WITH (fillfactor = 97) WHERE "pop_country_code" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_pop_state_code_temp ON universal_award_matview_temp USING BTREE("pop_state_code") WITH (fillfactor = 97) WHERE "pop_state_code" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_pop_county_code_temp ON universal_award_matview_temp USING BTREE("pop_county_code") WITH (fillfactor = 97) WHERE "pop_county_code" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_pop_zip5_temp ON universal_award_matview_temp USING BTREE("pop_zip5") WITH (fillfactor = 97) WHERE "pop_zip5" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_pop_congressional_code_temp ON universal_award_matview_temp USING BTREE("pop_congressional_code") WITH (fillfactor = 97) WHERE "pop_congressional_code" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_cfda_number_temp ON universal_award_matview_temp USING BTREE("cfda_number") WITH (fillfactor = 97) WHERE "cfda_number" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_pulled_from_temp ON universal_award_matview_temp USING BTREE("pulled_from") WITH (fillfactor = 97) WHERE "pulled_from" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_type_of_contract_pricing_temp ON universal_award_matview_temp USING BTREE("type_of_contract_pricing") WITH (fillfactor = 97) WHERE "type_of_contract_pricing" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_extent_competed_temp ON universal_award_matview_temp USING BTREE("extent_competed") WITH (fillfactor = 97) WHERE "extent_competed" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_type_set_aside_temp ON universal_award_matview_temp USING BTREE("type_set_aside") WITH (fillfactor = 97) WHERE "type_set_aside" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_product_or_service_code_temp ON universal_award_matview_temp USING BTREE("product_or_service_code") WITH (fillfactor = 97) WHERE "product_or_service_code" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_gin_product_or_service_description_temp ON universal_award_matview_temp USING GIN(("product_or_service_description") gin_trgm_ops);
CREATE INDEX idx_cf458597$9e8_naics_temp ON universal_award_matview_temp USING BTREE("naics_code") WITH (fillfactor = 97) WHERE "naics_code" IS NOT NULL;
CREATE INDEX idx_cf458597$9e8_gin_naics_code_temp ON universal_award_matview_temp USING GIN("naics_code" gin_trgm_ops);
CREATE INDEX idx_cf458597$9e8_gin_naics_description_temp ON universal_award_matview_temp USING GIN(UPPER("naics_description") gin_trgm_ops);
CREATE INDEX idx_cf458597$9e8_gin_business_categories_temp ON universal_award_matview_temp USING GIN("business_categories");
CREATE INDEX idx_cf458597$9e8_keyword_ts_vector_temp ON universal_award_matview_temp USING GIN("keyword_ts_vector");
CREATE INDEX idx_cf458597$9e8_award_ts_vector_temp ON universal_award_matview_temp USING GIN("award_ts_vector");
CREATE INDEX idx_cf458597$9e8_recipient_name_ts_vector_temp ON universal_award_matview_temp USING GIN("recipient_name_ts_vector");
CREATE INDEX idx_cf458597$9e8_keyword_id_temp ON universal_award_matview_temp USING GIN("keyword_string" gin_trgm_ops);
CREATE INDEX idx_cf458597$9e8_award_id_string_temp ON universal_award_matview_temp USING GIN("award_id_string" gin_trgm_ops);
CREATE INDEX idx_cf458597$9e8_compound_psc_fy_temp ON universal_award_matview_temp USING BTREE("product_or_service_code", "fiscal_year") WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_compound_naics_fy_temp ON universal_award_matview_temp USING BTREE("naics_code", "fiscal_year") WITH (fillfactor = 97);
CREATE INDEX idx_cf458597$9e8_compound_cfda_fy_temp ON universal_award_matview_temp USING BTREE("cfda_number", "fiscal_year") WITH (fillfactor = 97);
