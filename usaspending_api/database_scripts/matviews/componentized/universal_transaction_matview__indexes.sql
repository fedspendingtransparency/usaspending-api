--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
CREATE INDEX idx_3a7bb23c$383__transaction_id_temp ON universal_transaction_matview_temp USING BTREE("transaction_id" ASC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_3a7bb23c$383__action_date_temp ON universal_transaction_matview_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_3a7bb23c$383__fiscal_year_temp ON universal_transaction_matview_temp USING BTREE("fiscal_year" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_3a7bb23c$383__type_temp ON universal_transaction_matview_temp USING BTREE("type") WITH (fillfactor = 100) WHERE "type" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__ordered_type_temp ON universal_transaction_matview_temp USING BTREE("type" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_3a7bb23c$383__action_type_temp ON universal_transaction_matview_temp USING BTREE("action_type") WITH (fillfactor = 100);
CREATE INDEX idx_3a7bb23c$383__award_id_temp ON universal_transaction_matview_temp USING BTREE("award_id") WITH (fillfactor = 100);
CREATE INDEX idx_3a7bb23c$383__award_category_temp ON universal_transaction_matview_temp USING BTREE("award_category") WITH (fillfactor = 100) WHERE "award_category" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__total_obligation_temp ON universal_transaction_matview_temp USING BTREE("total_obligation") WITH (fillfactor = 100) WHERE "total_obligation" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__ordered_total_obligation_temp ON universal_transaction_matview_temp USING BTREE("total_obligation" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_3a7bb23c$383__total_obl_bin_temp ON universal_transaction_matview_temp USING BTREE("total_obl_bin") WITH (fillfactor = 100);
CREATE INDEX idx_3a7bb23c$383__total_subsidy_cost_temp ON universal_transaction_matview_temp USING BTREE("total_subsidy_cost") WITH (fillfactor = 100) WHERE "total_subsidy_cost" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__ordered_total_subsidy_cost_temp ON universal_transaction_matview_temp USING BTREE("total_subsidy_cost" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_3a7bb23c$383__pop_country_code_temp ON universal_transaction_matview_temp USING BTREE("pop_country_code") WITH (fillfactor = 100) WHERE "pop_country_code" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__pop_state_code_temp ON universal_transaction_matview_temp USING BTREE("pop_state_code") WITH (fillfactor = 100) WHERE "pop_state_code" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__pop_county_code_temp ON universal_transaction_matview_temp USING BTREE("pop_county_code") WITH (fillfactor = 100) WHERE "pop_county_code" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__pop_zip5_temp ON universal_transaction_matview_temp USING BTREE("pop_zip5") WITH (fillfactor = 100) WHERE "pop_zip5" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__pop_congressional_code_temp ON universal_transaction_matview_temp USING BTREE("pop_congressional_code") WITH (fillfactor = 100) WHERE "pop_congressional_code" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__gin_recipient_name_temp ON universal_transaction_matview_temp USING GIN("recipient_name" gin_trgm_ops);
CREATE INDEX idx_3a7bb23c$383__gin_recipient_unique_id_temp ON universal_transaction_matview_temp USING GIN("recipient_unique_id" gin_trgm_ops);
CREATE INDEX idx_3a7bb23c$383__gin_parent_recipient_unique_id_temp ON universal_transaction_matview_temp USING GIN("parent_recipient_unique_id" gin_trgm_ops);
CREATE INDEX idx_3a7bb23c$383__recipient_id_temp ON universal_transaction_matview_temp USING BTREE("recipient_id") WITH (fillfactor = 100) WHERE "recipient_id" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__recipient_name_temp ON universal_transaction_matview_temp USING BTREE("recipient_name") WITH (fillfactor = 100) WHERE "recipient_name" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__recipient_unique_id_temp ON universal_transaction_matview_temp USING BTREE("recipient_unique_id") WITH (fillfactor = 100) WHERE "recipient_unique_id" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__parent_recipient_unique_id_temp ON universal_transaction_matview_temp USING BTREE("parent_recipient_unique_id") WITH (fillfactor = 100) WHERE "parent_recipient_unique_id" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__awarding_agency_id_temp ON universal_transaction_matview_temp USING BTREE("awarding_agency_id" ASC NULLS LAST) WITH (fillfactor = 100) WHERE "awarding_agency_id" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__funding_agency_id_temp ON universal_transaction_matview_temp USING BTREE("funding_agency_id" ASC NULLS LAST) WITH (fillfactor = 100) WHERE "funding_agency_id" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__awarding_toptier_agency_name_temp ON universal_transaction_matview_temp USING BTREE("awarding_toptier_agency_name") WITH (fillfactor = 100) WHERE "awarding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__awarding_subtier_agency_name_temp ON universal_transaction_matview_temp USING BTREE("awarding_subtier_agency_name") WITH (fillfactor = 100) WHERE "awarding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__funding_toptier_agency_name_temp ON universal_transaction_matview_temp USING BTREE("funding_toptier_agency_name") WITH (fillfactor = 100) WHERE "funding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__funding_subtier_agency_name_temp ON universal_transaction_matview_temp USING BTREE("funding_subtier_agency_name") WITH (fillfactor = 100) WHERE "funding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__recipient_location_country_code_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_country_code") WITH (fillfactor = 100) WHERE "recipient_location_country_code" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__recipient_location_state_code_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_state_code") WITH (fillfactor = 100) WHERE "recipient_location_state_code" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__recipient_location_county_code_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_county_code") WITH (fillfactor = 100) WHERE "recipient_location_county_code" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__recipient_location_zip5_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_zip5") WITH (fillfactor = 100) WHERE "recipient_location_zip5" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__recipient_location_congressional_code_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_congressional_code") WITH (fillfactor = 100) WHERE "recipient_location_congressional_code" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__cfda_multi_temp ON universal_transaction_matview_temp USING BTREE("cfda_number", "cfda_title") WITH (fillfactor = 100) WHERE "cfda_number" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__pulled_from_temp ON universal_transaction_matview_temp USING BTREE("pulled_from") WITH (fillfactor = 100) WHERE "pulled_from" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__type_of_contract_pricing_temp ON universal_transaction_matview_temp USING BTREE("type_of_contract_pricing") WITH (fillfactor = 100) WHERE "type_of_contract_pricing" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__extent_competed_temp ON universal_transaction_matview_temp USING BTREE("extent_competed") WITH (fillfactor = 100) WHERE "extent_competed" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__type_set_aside_temp ON universal_transaction_matview_temp USING BTREE("type_set_aside") WITH (fillfactor = 100) WHERE "type_set_aside" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__product_or_service_code_temp ON universal_transaction_matview_temp USING BTREE("product_or_service_code") WITH (fillfactor = 100) WHERE "product_or_service_code" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__gin_naics_code_temp ON universal_transaction_matview_temp USING GIN("naics_code" gin_trgm_ops);
CREATE INDEX idx_3a7bb23c$383__naics_code_temp ON universal_transaction_matview_temp USING BTREE("naics_code") WITH (fillfactor = 100) WHERE "naics_code" IS NOT NULL;
CREATE INDEX idx_3a7bb23c$383__business_categories_temp ON universal_transaction_matview_temp USING GIN("business_categories");
CREATE INDEX idx_3a7bb23c$383__gin_keyword_string_temp ON universal_transaction_matview_temp USING GIN("keyword_string_old" gin_trgm_ops);
CREATE INDEX idx_3a7bb23c$383__gin_award_id_string_temp ON universal_transaction_matview_temp USING GIN("award_id_string_old" gin_trgm_ops);
CREATE INDEX idx_3a7bb23c$383__awarding_toptier_agency_ts_vector_temp ON universal_transaction_matview_temp USING GIN("awarding_toptier_agency_ts_vector");
CREATE INDEX idx_3a7bb23c$383__awarding_subtier_agency_ts_vector_temp ON universal_transaction_matview_temp USING GIN("awarding_subtier_agency_ts_vector");
CREATE INDEX idx_3a7bb23c$383__funding_toptier_agency_ts_vector_temp ON universal_transaction_matview_temp USING GIN("funding_toptier_agency_ts_vector");
CREATE INDEX idx_3a7bb23c$383__funding_subtier_agency_ts_vector_temp ON universal_transaction_matview_temp USING GIN("funding_subtier_agency_ts_vector");
CREATE INDEX idx_3a7bb23c$383__keyword_string_temp ON universal_transaction_matview_temp USING GIN("keyword_string");
CREATE INDEX idx_3a7bb23c$383__award_id_string_temp ON universal_transaction_matview_temp USING GIN("award_id_string");
CREATE INDEX idx_3a7bb23c$383__recipient_name_ts_vector_temp ON universal_transaction_matview_temp USING GIN("recipient_name_ts_vector");
