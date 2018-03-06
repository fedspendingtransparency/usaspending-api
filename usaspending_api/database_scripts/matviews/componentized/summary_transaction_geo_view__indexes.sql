--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
CREATE UNIQUE INDEX idx_af8ca7ca$0d1_unique_pk_temp ON summary_transaction_geo_view_temp USING BTREE("pk") WITH (fillfactor = 97);
CREATE INDEX idx_af8ca7ca$0d1_date_temp ON summary_transaction_geo_view_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_af8ca7ca$0d1_fy_temp ON summary_transaction_geo_view_temp USING BTREE("fiscal_year" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_af8ca7ca$0d1_fy_type_temp ON summary_transaction_geo_view_temp USING BTREE("fiscal_year" DESC NULLS LAST, "type") WITH (fillfactor = 97);
CREATE INDEX idx_af8ca7ca$0d1_type_temp ON summary_transaction_geo_view_temp USING BTREE("type") WITH (fillfactor = 97) WHERE "type" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_pulled_from_temp ON summary_transaction_geo_view_temp USING BTREE("pulled_from" DESC NULLS LAST) WITH (fillfactor = 97) WHERE "pulled_from" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_recipient_country_code_temp ON summary_transaction_geo_view_temp USING BTREE("recipient_location_country_code") WITH (fillfactor = 97) WHERE "recipient_location_country_code" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_recipient_state_code_temp ON summary_transaction_geo_view_temp USING BTREE("recipient_location_state_code") WITH (fillfactor = 97) WHERE "recipient_location_state_code" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_recipient_county_code_temp ON summary_transaction_geo_view_temp USING BTREE("recipient_location_county_code") WITH (fillfactor = 97) WHERE "recipient_location_county_code" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_recipient_zip_temp ON summary_transaction_geo_view_temp USING BTREE("recipient_location_zip5") WITH (fillfactor = 97) WHERE "recipient_location_zip5" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_pop_country_code_temp ON summary_transaction_geo_view_temp USING BTREE("pop_country_code") WITH (fillfactor = 97) WHERE "pop_country_code" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_pop_state_code_temp ON summary_transaction_geo_view_temp USING BTREE("pop_state_code") WITH (fillfactor = 97) WHERE "pop_state_code" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_pop_county_code_temp ON summary_transaction_geo_view_temp USING BTREE("pop_county_code") WITH (fillfactor = 97) WHERE "pop_county_code" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_pop_zip_temp ON summary_transaction_geo_view_temp USING BTREE("pop_zip5") WITH (fillfactor = 97) WHERE "pop_zip5" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_awarding_agency_id_temp ON summary_transaction_geo_view_temp USING BTREE("awarding_agency_id" ASC NULLS LAST) WITH (fillfactor = 97) WHERE "awarding_agency_id" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_funding_agency_id_temp ON summary_transaction_geo_view_temp USING BTREE("funding_agency_id" ASC NULLS LAST) WITH (fillfactor = 97) WHERE "funding_agency_id" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_awarding_toptier_agency_name_temp ON summary_transaction_geo_view_temp USING BTREE("awarding_toptier_agency_name") WITH (fillfactor = 97) WHERE "awarding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_awarding_subtier_agency_name_temp ON summary_transaction_geo_view_temp USING BTREE("awarding_subtier_agency_name") WITH (fillfactor = 97) WHERE "awarding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_funding_toptier_agency_name_temp ON summary_transaction_geo_view_temp USING BTREE("funding_toptier_agency_name") WITH (fillfactor = 97) WHERE "funding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_funding_subtier_agency_name_temp ON summary_transaction_geo_view_temp USING BTREE("funding_subtier_agency_name") WITH (fillfactor = 97) WHERE "funding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_af8ca7ca$0d1_compound_geo_pop_1_temp ON summary_transaction_geo_view_temp USING BTREE("pop_country_code", "pop_state_code", "pop_county_code", "fiscal_year") WITH (fillfactor = 97) WHERE "pop_country_code" = 'USA';
CREATE INDEX idx_af8ca7ca$0d1_compound_geo_pop_2_temp ON summary_transaction_geo_view_temp USING BTREE("pop_country_code", "pop_state_code", "pop_congressional_code", "fiscal_year") WITH (fillfactor = 97) WHERE "pop_country_code" = 'USA';
CREATE INDEX idx_af8ca7ca$0d1_compound_geo_pop_3_temp ON summary_transaction_geo_view_temp USING BTREE("pop_country_code", "pop_zip5", "fiscal_year") WITH (fillfactor = 97) WHERE "pop_country_code" = 'USA';
CREATE INDEX idx_af8ca7ca$0d1_compound_geo_rl_1_temp ON summary_transaction_geo_view_temp USING BTREE("recipient_location_country_code", "recipient_location_state_code", "recipient_location_county_code", "fiscal_year") WITH (fillfactor = 97) WHERE "recipient_location_country_code" = 'USA';
CREATE INDEX idx_af8ca7ca$0d1_compound_geo_rl_2_temp ON summary_transaction_geo_view_temp USING BTREE("recipient_location_country_code", "recipient_location_state_code", "recipient_location_congressional_code", "fiscal_year") WITH (fillfactor = 97) WHERE "recipient_location_country_code" = 'USA';
CREATE INDEX idx_af8ca7ca$0d1_compound_geo_rl_3_temp ON summary_transaction_geo_view_temp USING BTREE("recipient_location_country_code", "recipient_location_zip5", "fiscal_year") WITH (fillfactor = 97) WHERE "recipient_location_country_code" = 'USA';
