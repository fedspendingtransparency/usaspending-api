--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
CREATE INDEX idx_a636961e__id_temp ON universal_award_matview_temp USING BTREE("award_id") WITH (fillfactor = 100);
CREATE INDEX idx_a636961e__category_temp ON universal_award_matview_temp USING BTREE("category") WITH (fillfactor = 100);
CREATE INDEX idx_a636961e__type_temp ON universal_award_matview_temp USING BTREE("type") WITH (fillfactor = 100) WHERE "type" IS NOT NULL;
CREATE INDEX idx_a636961e__ordered_type_temp ON universal_award_matview_temp USING BTREE("type" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_a636961e__ordered_type_desc_temp ON universal_award_matview_temp USING BTREE("type_description" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_a636961e__ordered_fain_temp ON universal_award_matview_temp USING BTREE(UPPER("fain") DESC NULLS LAST) WITH (fillfactor = 100) WHERE "fain" IS NOT NULL;
CREATE INDEX idx_a636961e__ordered_piid_temp ON universal_award_matview_temp USING BTREE(UPPER("piid") DESC NULLS LAST) WITH (fillfactor = 100) WHERE "piid" IS NOT NULL;
CREATE INDEX idx_a636961e__total_obligation_temp ON universal_award_matview_temp USING BTREE("total_obligation") WITH (fillfactor = 100) WHERE "total_obligation" IS NOT NULL;
CREATE INDEX idx_a636961e__ordered_total_obligation_temp ON universal_award_matview_temp USING BTREE("total_obligation" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_a636961e__total_obl_bin_temp ON universal_award_matview_temp USING BTREE("total_obl_bin") WITH (fillfactor = 100);
CREATE INDEX idx_a636961e__total_subsidy_cost_temp ON universal_award_matview_temp USING BTREE("total_subsidy_cost") WITH (fillfactor = 100) WHERE "total_subsidy_cost" IS NOT NULL;
CREATE INDEX idx_a636961e__ordered_total_subsidy_cost_temp ON universal_award_matview_temp USING BTREE("total_subsidy_cost" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_a636961e__period_of_performance_start_date_temp ON universal_award_matview_temp USING BTREE("period_of_performance_start_date" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_a636961e__period_of_performance_current_end_date_temp ON universal_award_matview_temp USING BTREE("period_of_performance_current_end_date" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_a636961e__gin_recipient_name_temp ON universal_award_matview_temp USING GIN("recipient_name" gin_trgm_ops);
CREATE INDEX idx_a636961e__recipient_name_temp ON universal_award_matview_temp USING BTREE("recipient_name") WITH (fillfactor = 100) WHERE "recipient_name" IS NOT NULL;
CREATE INDEX idx_a636961e__recipient_unique_id_temp ON universal_award_matview_temp USING BTREE("recipient_unique_id") WITH (fillfactor = 100) WHERE "recipient_unique_id" IS NOT NULL;
CREATE INDEX idx_a636961e__parent_recipient_unique_id_temp ON universal_award_matview_temp USING BTREE("parent_recipient_unique_id") WITH (fillfactor = 100) WHERE "parent_recipient_unique_id" IS NOT NULL;
CREATE INDEX idx_a636961e__action_date_temp ON universal_award_matview_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_a636961e__fiscal_year_temp ON universal_award_matview_temp USING BTREE("fiscal_year" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_a636961e__awarding_agency_id_temp ON universal_award_matview_temp USING BTREE("awarding_agency_id" ASC NULLS LAST) WITH (fillfactor = 100) WHERE "awarding_agency_id" IS NOT NULL;
CREATE INDEX idx_a636961e__funding_agency_id_temp ON universal_award_matview_temp USING BTREE("funding_agency_id" ASC NULLS LAST) WITH (fillfactor = 100) WHERE "funding_agency_id" IS NOT NULL;
CREATE INDEX idx_a636961e__ordered_awarding_toptier_agency_name_temp ON universal_award_matview_temp USING BTREE("awarding_toptier_agency_name" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_a636961e__ordered_awarding_subtier_agency_name_temp ON universal_award_matview_temp USING BTREE("awarding_subtier_agency_name" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_a636961e__awarding_toptier_agency_name_temp ON universal_award_matview_temp USING BTREE("awarding_toptier_agency_name") WITH (fillfactor = 100) WHERE "awarding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_a636961e__awarding_subtier_agency_name_temp ON universal_award_matview_temp USING BTREE("awarding_subtier_agency_name") WITH (fillfactor = 100) WHERE "awarding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_a636961e__funding_toptier_agency_name_temp ON universal_award_matview_temp USING BTREE("funding_toptier_agency_name") WITH (fillfactor = 100) WHERE "funding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_a636961e__funding_subtier_agency_name_temp ON universal_award_matview_temp USING BTREE("funding_subtier_agency_name") WITH (fillfactor = 100) WHERE "funding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_a636961e__recipient_location_country_code_temp ON universal_award_matview_temp USING BTREE("recipient_location_country_code") WITH (fillfactor = 100) WHERE "recipient_location_country_code" IS NOT NULL;
CREATE INDEX idx_a636961e__recipient_location_state_code_temp ON universal_award_matview_temp USING BTREE("recipient_location_state_code") WITH (fillfactor = 100) WHERE "recipient_location_state_code" IS NOT NULL;
CREATE INDEX idx_a636961e__recipient_location_county_code_temp ON universal_award_matview_temp USING BTREE("recipient_location_county_code") WITH (fillfactor = 100) WHERE "recipient_location_county_code" IS NOT NULL;
CREATE INDEX idx_a636961e__recipient_location_zip5_temp ON universal_award_matview_temp USING BTREE("recipient_location_zip5") WITH (fillfactor = 100) WHERE "recipient_location_zip5" IS NOT NULL;
CREATE INDEX idx_a636961e__recipient_location_cong_code_temp ON universal_award_matview_temp USING BTREE("recipient_location_congressional_code") WITH (fillfactor = 100) WHERE "recipient_location_congressional_code" IS NOT NULL;
CREATE INDEX idx_a636961e__pop_country_code_temp ON universal_award_matview_temp USING BTREE("pop_country_code") WITH (fillfactor = 100) WHERE "pop_country_code" IS NOT NULL;
CREATE INDEX idx_a636961e__pop_state_code_temp ON universal_award_matview_temp USING BTREE("pop_state_code") WITH (fillfactor = 100) WHERE "pop_state_code" IS NOT NULL;
CREATE INDEX idx_a636961e__pop_county_code_temp ON universal_award_matview_temp USING BTREE("pop_county_code") WITH (fillfactor = 100) WHERE "pop_county_code" IS NOT NULL;
CREATE INDEX idx_a636961e__pop_zip5_temp ON universal_award_matview_temp USING BTREE("pop_zip5") WITH (fillfactor = 100) WHERE "pop_zip5" IS NOT NULL;
CREATE INDEX idx_a636961e__pop_congressional_code_temp ON universal_award_matview_temp USING BTREE("pop_congressional_code") WITH (fillfactor = 100) WHERE "pop_congressional_code" IS NOT NULL;
CREATE INDEX idx_a636961e__cfda_number_temp ON universal_award_matview_temp USING BTREE("cfda_number") WITH (fillfactor = 100) WHERE "cfda_number" IS NOT NULL;
CREATE INDEX idx_a636961e__pulled_from_temp ON universal_award_matview_temp USING BTREE("pulled_from") WITH (fillfactor = 100) WHERE "pulled_from" IS NOT NULL;
CREATE INDEX idx_a636961e__type_of_contract_pricing_temp ON universal_award_matview_temp USING BTREE("type_of_contract_pricing") WITH (fillfactor = 100) WHERE "type_of_contract_pricing" IS NOT NULL;
CREATE INDEX idx_a636961e__extent_competed_temp ON universal_award_matview_temp USING BTREE("extent_competed") WITH (fillfactor = 100) WHERE "extent_competed" IS NOT NULL;
CREATE INDEX idx_a636961e__type_set_aside_temp ON universal_award_matview_temp USING BTREE("type_set_aside") WITH (fillfactor = 100) WHERE "type_set_aside" IS NOT NULL;
CREATE INDEX idx_a636961e__product_or_service_code_temp ON universal_award_matview_temp USING BTREE("product_or_service_code") WITH (fillfactor = 100) WHERE "product_or_service_code" IS NOT NULL;
CREATE INDEX idx_a636961e__gin_product_or_service_description_temp ON universal_award_matview_temp USING GIN(("product_or_service_description") gin_trgm_ops);
CREATE INDEX idx_a636961e__naics_temp ON universal_award_matview_temp USING BTREE("naics_code") WITH (fillfactor = 100) WHERE "naics_code" IS NOT NULL;
CREATE INDEX idx_a636961e__gin_naics_code_temp ON universal_award_matview_temp USING GIN("naics_code" gin_trgm_ops);
CREATE INDEX idx_a636961e__gin_naics_description_temp ON universal_award_matview_temp USING GIN(UPPER("naics_description") gin_trgm_ops);
CREATE INDEX idx_a636961e__gin_business_categories_temp ON universal_award_matview_temp USING GIN("business_categories");
CREATE INDEX idx_a636961e__gin_keyword_string_temp ON universal_award_matview_temp USING GIN("keyword_string" gin_trgm_ops);
CREATE INDEX idx_a636961e__gin_award_id_string_temp ON universal_award_matview_temp USING GIN("award_id_string" gin_trgm_ops);
CREATE INDEX idx_a636961e__tuned_type_and_idv_temp ON universal_award_matview_temp USING BTREE("type", "pulled_from") WITH (fillfactor = 100) WHERE "type" IS NULL AND "pulled_from" IS NOT NULL;
