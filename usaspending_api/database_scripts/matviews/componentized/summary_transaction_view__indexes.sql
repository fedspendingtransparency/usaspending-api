--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
CREATE INDEX idx_5d43926a__ordered_action_date_temp ON summary_transaction_view_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_5d43926a__ordered_fy_temp ON summary_transaction_view_temp USING BTREE("fiscal_year" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_5d43926a__ordered_fy_type_temp ON summary_transaction_view_temp USING BTREE("fiscal_year" DESC NULLS LAST, "type") WITH (fillfactor = 100);
CREATE INDEX idx_5d43926a__type_temp ON summary_transaction_view_temp USING BTREE("type") WITH (fillfactor = 100) WHERE "type" IS NOT NULL;
CREATE INDEX idx_5d43926a__pulled_from_temp ON summary_transaction_view_temp USING BTREE("pulled_from" DESC NULLS LAST) WITH (fillfactor = 100) WHERE "pulled_from" IS NOT NULL;
CREATE INDEX idx_5d43926a__recipient_country_code_temp ON summary_transaction_view_temp USING BTREE("recipient_location_country_code") WITH (fillfactor = 100) WHERE "recipient_location_country_code" IS NOT NULL;
CREATE INDEX idx_5d43926a__recipient_state_code_temp ON summary_transaction_view_temp USING BTREE("recipient_location_state_code") WITH (fillfactor = 100) WHERE "recipient_location_state_code" IS NOT NULL;
CREATE INDEX idx_5d43926a__recipient_county_code_temp ON summary_transaction_view_temp USING BTREE("recipient_location_county_code") WITH (fillfactor = 100) WHERE "recipient_location_county_code" IS NOT NULL;
CREATE INDEX idx_5d43926a__recipient_zip_temp ON summary_transaction_view_temp USING BTREE("recipient_location_zip5") WITH (fillfactor = 100) WHERE "recipient_location_zip5" IS NOT NULL;
CREATE INDEX idx_5d43926a__pop_country_code_temp ON summary_transaction_view_temp USING BTREE("pop_country_code") WITH (fillfactor = 100) WHERE "pop_country_code" IS NOT NULL;
CREATE INDEX idx_5d43926a__pop_state_code_temp ON summary_transaction_view_temp USING BTREE("pop_state_code") WITH (fillfactor = 100) WHERE "pop_state_code" IS NOT NULL;
CREATE INDEX idx_5d43926a__pop_county_code_temp ON summary_transaction_view_temp USING BTREE("pop_county_code") WITH (fillfactor = 100) WHERE "pop_county_code" IS NOT NULL;
CREATE INDEX idx_5d43926a__pop_zip_temp ON summary_transaction_view_temp USING BTREE("pop_zip5") WITH (fillfactor = 100) WHERE "pop_zip5" IS NOT NULL;
CREATE INDEX idx_5d43926a__awarding_agency_id_temp ON summary_transaction_view_temp USING BTREE("awarding_agency_id" ASC NULLS LAST) WITH (fillfactor = 100) WHERE "awarding_agency_id" IS NOT NULL;
CREATE INDEX idx_5d43926a__funding_agency_id_temp ON summary_transaction_view_temp USING BTREE("funding_agency_id" ASC NULLS LAST) WITH (fillfactor = 100) WHERE "funding_agency_id" IS NOT NULL;
CREATE INDEX idx_5d43926a__awarding_toptier_agency_name_temp ON summary_transaction_view_temp USING BTREE("awarding_toptier_agency_name") WITH (fillfactor = 100) WHERE "awarding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_5d43926a__awarding_subtier_agency_name_temp ON summary_transaction_view_temp USING BTREE("awarding_subtier_agency_name") WITH (fillfactor = 100) WHERE "awarding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_5d43926a__funding_toptier_agency_name_temp ON summary_transaction_view_temp USING BTREE("funding_toptier_agency_name") WITH (fillfactor = 100) WHERE "funding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_5d43926a__funding_subtier_agency_name_temp ON summary_transaction_view_temp USING BTREE("funding_subtier_agency_name") WITH (fillfactor = 100) WHERE "funding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_5d43926a__cfda_num_temp ON summary_transaction_view_temp USING BTREE("cfda_number") WITH (fillfactor = 100) WHERE "cfda_number" IS NOT NULL;
CREATE INDEX idx_5d43926a__cfda_title_temp ON summary_transaction_view_temp USING BTREE("cfda_title") WITH (fillfactor = 100) WHERE "cfda_title" IS NOT NULL;
CREATE INDEX idx_5d43926a__psc_temp ON summary_transaction_view_temp USING BTREE("product_or_service_code") WITH (fillfactor = 100) WHERE "product_or_service_code" IS NOT NULL;
CREATE INDEX idx_5d43926a__naics_temp ON summary_transaction_view_temp USING BTREE("naics_code") WITH (fillfactor = 100) WHERE "naics_code" IS NOT NULL;
CREATE INDEX idx_5d43926a__total_obl_bin_temp ON summary_transaction_view_temp USING BTREE("total_obl_bin") WITH (fillfactor = 100) WHERE "total_obl_bin" IS NOT NULL;
CREATE INDEX idx_5d43926a__type_of_contract_temp ON summary_transaction_view_temp USING BTREE("type_of_contract_pricing") WITH (fillfactor = 100);
CREATE INDEX idx_5d43926a__ordered_fy_set_aside_temp ON summary_transaction_view_temp USING BTREE("fiscal_year" DESC NULLS LAST, "type_set_aside") WITH (fillfactor = 100);
CREATE INDEX idx_5d43926a__extent_competed_temp ON summary_transaction_view_temp USING BTREE("extent_competed") WITH (fillfactor = 100);
CREATE INDEX idx_5d43926a__type_set_aside_temp ON summary_transaction_view_temp USING BTREE("type_set_aside") WITH (fillfactor = 100) WHERE "type_set_aside" IS NOT NULL;
CREATE INDEX idx_5d43926a__business_categories_temp ON summary_transaction_view_temp USING GIN("business_categories");
CREATE INDEX idx_5d43926a__tuned_type_and_idv_temp ON summary_transaction_view_temp USING BTREE("pulled_from", "type") WITH (fillfactor = 100) WHERE "type" IS NULL AND "pulled_from" IS NOT NULL;
CREATE INDEX idx_5d43926a__compound_geo_pop_1_temp ON summary_transaction_view_temp USING BTREE("pop_country_code", "pop_state_code", "pop_county_code", "fiscal_year") WITH (fillfactor = 100) WHERE "pop_country_code" = 'USA';
CREATE INDEX idx_5d43926a__compound_geo_pop_2_temp ON summary_transaction_view_temp USING BTREE("pop_country_code", "pop_state_code", "pop_congressional_code", "fiscal_year") WITH (fillfactor = 100) WHERE "pop_country_code" = 'USA';
CREATE INDEX idx_5d43926a__compound_geo_pop_3_temp ON summary_transaction_view_temp USING BTREE("pop_country_code", "pop_zip5", "fiscal_year") WITH (fillfactor = 100) WHERE "pop_country_code" = 'USA';
CREATE INDEX idx_5d43926a__compound_geo_rl_1_temp ON summary_transaction_view_temp USING BTREE("recipient_location_country_code", "recipient_location_state_code", "recipient_location_county_code", "fiscal_year") WITH (fillfactor = 100) WHERE "recipient_location_country_code" = 'USA';
CREATE INDEX idx_5d43926a__compound_geo_rl_2_temp ON summary_transaction_view_temp USING BTREE("recipient_location_country_code", "recipient_location_state_code", "recipient_location_congressional_code", "fiscal_year") WITH (fillfactor = 100) WHERE "recipient_location_country_code" = 'USA';
CREATE INDEX idx_5d43926a__compound_geo_rl_3_temp ON summary_transaction_view_temp USING BTREE("recipient_location_country_code", "recipient_location_zip5", "fiscal_year") WITH (fillfactor = 100) WHERE "recipient_location_country_code" = 'USA';
