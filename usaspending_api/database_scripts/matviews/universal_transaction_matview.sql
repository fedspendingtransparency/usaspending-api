--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS universal_transaction_matview_temp CASCADE;
DROP MATERIALIZED VIEW IF EXISTS universal_transaction_matview_old CASCADE;

CREATE MATERIALIZED VIEW universal_transaction_matview_temp AS
SELECT
  UPPER(CONCAT(
    "legal_entity"."recipient_name",
    ' ', "transaction_fpds"."naics",
    ' ', "naics"."description",
    ' ', "psc"."description",
    ' ', "awards"."description")) AS keyword_string,
  UPPER(CONCAT(awards.piid, ' ', awards.fain, ' ', awards.uri)) AS award_id_string,
  "transaction_normalized"."id" AS transaction_id,
  "transaction_normalized"."action_date"::date,
  "transaction_normalized"."fiscal_year",
  "transaction_normalized"."type",
  "transaction_normalized"."action_type",
  "transaction_normalized"."award_id",
  "awards"."category" AS award_category,
  "awards"."total_obligation",
  "awards"."total_subsidy_cost",
  obligation_to_enum("awards"."total_obligation") AS total_obl_bin,
  "awards"."fain",
  "awards"."uri",
  "awards"."piid",
  COALESCE("transaction_normalized"."federal_action_obligation", 0)::NUMERIC(20, 2) AS "federal_action_obligation",
  COALESCE("transaction_normalized"."original_loan_subsidy_cost", 0)::NUMERIC(20, 2) AS "original_loan_subsidy_cost",
  "transaction_normalized"."description" AS transaction_description,
  "transaction_normalized"."modification_number",

  place_of_performance."location_country_code" AS pop_country_code,
  place_of_performance."country_name" AS pop_country_name,
  place_of_performance."state_code" AS pop_state_code,
  place_of_performance."county_code" AS pop_county_code,
  place_of_performance."county_name" AS pop_county_name,
  place_of_performance."zip5" AS pop_zip5,
  place_of_performance."congressional_code" AS pop_congressional_code,

  recipient_location."location_country_code" AS recipient_location_country_code,
  recipient_location."country_name" AS recipient_location_country_name,
  recipient_location."state_code" AS recipient_location_state_code,
  recipient_location."county_code" AS recipient_location_county_code,
  recipient_location."county_name" AS recipient_location_county_name,
  recipient_location."zip5" AS recipient_location_zip5,
  recipient_location."congressional_code" AS recipient_location_congressional_code,

  "transaction_fpds"."naics" AS naics_code,
  "naics"."description" AS naics_description,
  "transaction_fpds"."product_or_service_code",
  "psc"."description" AS product_or_service_description,
  "transaction_fpds"."pulled_from",
  "transaction_fpds"."type_of_contract_pricing",
  "transaction_fpds"."type_set_aside",
  "transaction_fpds"."extent_competed",
  "transaction_fabs"."cfda_number",
  "references_cfda"."program_title" AS cfda_title,
  "references_cfda"."popular_name" AS cfda_popular_name,
  "transaction_normalized"."recipient_id",
  UPPER("legal_entity"."recipient_name") AS recipient_name,
  "legal_entity"."recipient_unique_id",
  "legal_entity"."parent_recipient_unique_id",
  "legal_entity"."business_categories",

  "transaction_normalized"."awarding_agency_id",
  "transaction_normalized"."funding_agency_id",
  TAA."name" AS awarding_toptier_agency_name,
  TFA."name" AS funding_toptier_agency_name,
  SAA."name" AS awarding_subtier_agency_name,
  SFA."name" AS funding_subtier_agency_name,
  TAA."abbreviation" AS awarding_toptier_agency_abbreviation,
  TFA."abbreviation" AS funding_toptier_agency_abbreviation,
  SAA."abbreviation" AS awarding_subtier_agency_abbreviation,
  SFA."abbreviation" AS funding_subtier_agency_abbreviation
FROM
  "transaction_normalized"
LEFT OUTER JOIN
  "transaction_fabs" ON ("transaction_normalized"."id" = "transaction_fabs"."transaction_id")
LEFT OUTER JOIN
  "transaction_fpds" ON ("transaction_normalized"."id" = "transaction_fpds"."transaction_id")
LEFT OUTER JOIN
  "references_cfda" ON ("transaction_fabs"."cfda_number" = "references_cfda"."program_number")
LEFT OUTER JOIN
  "legal_entity" ON ("transaction_normalized"."recipient_id" = "legal_entity"."legal_entity_id")
LEFT OUTER JOIN
  "references_location" AS recipient_location ON ("legal_entity"."location_id" = recipient_location."location_id")
LEFT OUTER JOIN
  "awards" ON ("transaction_normalized"."award_id" = "awards"."id")
LEFT OUTER JOIN
  "references_location" AS place_of_performance ON ("transaction_normalized"."place_of_performance_id" = place_of_performance."location_id")
LEFT OUTER JOIN
  "agency" AS AA ON ("transaction_normalized"."awarding_agency_id" = AA."id")
LEFT OUTER JOIN
  "toptier_agency" AS TAA ON (AA."toptier_agency_id" = TAA."toptier_agency_id")
LEFT OUTER JOIN
  "subtier_agency" AS SAA ON (AA."subtier_agency_id" = SAA."subtier_agency_id")
LEFT OUTER JOIN
  "agency" AS FA ON ("transaction_normalized"."funding_agency_id" = FA."id")
LEFT OUTER JOIN
  "toptier_agency" AS TFA ON (FA."toptier_agency_id" = TFA."toptier_agency_id")
LEFT OUTER JOIN
  "subtier_agency" AS SFA ON (FA."subtier_agency_id" = SFA."subtier_agency_id")
LEFT OUTER JOIN
  "naics" ON ("transaction_fpds"."naics" = "naics"."code")
LEFT OUTER JOIN
  "psc" ON ("transaction_fpds"."product_or_service_code" = "psc"."code")
WHERE
  "transaction_normalized"."action_date" >= '2007-10-01'
ORDER BY
  "transaction_normalized"."action_date" DESC;

CREATE UNIQUE INDEX idx_7d34d470$77c_transaction_id_temp ON universal_transaction_matview_temp USING BTREE("transaction_id" ASC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$77c_action_date_temp ON universal_transaction_matview_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$77c_fiscal_year_temp ON universal_transaction_matview_temp USING BTREE("fiscal_year" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$77c_type_temp ON universal_transaction_matview_temp USING BTREE("type") WITH (fillfactor = 97) WHERE "type" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_ordered_type_temp ON universal_transaction_matview_temp USING BTREE("type" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$77c_action_type_temp ON universal_transaction_matview_temp USING BTREE("action_type") WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$77c_award_id_temp ON universal_transaction_matview_temp USING BTREE("award_id") WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$77c_award_category_temp ON universal_transaction_matview_temp USING BTREE("award_category") WITH (fillfactor = 97) WHERE "award_category" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_total_obligation_temp ON universal_transaction_matview_temp USING BTREE("total_obligation") WITH (fillfactor = 97) WHERE "total_obligation" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_ordered_total_obligation_temp ON universal_transaction_matview_temp USING BTREE("total_obligation" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$77c_total_obl_bin_temp ON universal_transaction_matview_temp USING BTREE("total_obl_bin") WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$77c_total_subsidy_cost_temp ON universal_transaction_matview_temp USING BTREE("total_subsidy_cost") WITH (fillfactor = 97) WHERE "total_subsidy_cost" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_ordered_total_subsidy_cost_temp ON universal_transaction_matview_temp USING BTREE("total_subsidy_cost" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$77c_pop_country_code_temp ON universal_transaction_matview_temp USING BTREE("pop_country_code") WITH (fillfactor = 97) WHERE "pop_country_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_pop_state_code_temp ON universal_transaction_matview_temp USING BTREE("pop_state_code") WITH (fillfactor = 97) WHERE "pop_state_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_pop_county_code_temp ON universal_transaction_matview_temp USING BTREE("pop_county_code") WITH (fillfactor = 97) WHERE "pop_county_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_pop_zip5_temp ON universal_transaction_matview_temp USING BTREE("pop_zip5") WITH (fillfactor = 97) WHERE "pop_zip5" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_pop_congressional_code_temp ON universal_transaction_matview_temp USING BTREE("pop_congressional_code") WITH (fillfactor = 97) WHERE "pop_congressional_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_gin_recipient_name_temp ON universal_transaction_matview_temp USING GIN("recipient_name" gin_trgm_ops);
CREATE INDEX idx_7d34d470$77c_gin_recipient_unique_id_temp ON universal_transaction_matview_temp USING GIN("recipient_unique_id" gin_trgm_ops);
CREATE INDEX idx_7d34d470$77c_gin_parent_recipient_unique_id_temp ON universal_transaction_matview_temp USING GIN("parent_recipient_unique_id" gin_trgm_ops);
CREATE INDEX idx_7d34d470$77c_recipient_id_temp ON universal_transaction_matview_temp USING BTREE("recipient_id") WITH (fillfactor = 97) WHERE "recipient_id" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_recipient_name_temp ON universal_transaction_matview_temp USING BTREE("recipient_name") WITH (fillfactor = 97) WHERE "recipient_name" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_recipient_unique_id_temp ON universal_transaction_matview_temp USING BTREE("recipient_unique_id") WITH (fillfactor = 97) WHERE "recipient_unique_id" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_parent_recipient_unique_id_temp ON universal_transaction_matview_temp USING BTREE("parent_recipient_unique_id") WITH (fillfactor = 97) WHERE "parent_recipient_unique_id" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_awarding_agency_id_temp ON universal_transaction_matview_temp USING BTREE("awarding_agency_id" ASC NULLS LAST) WITH (fillfactor = 97) WHERE "awarding_agency_id" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_funding_agency_id_temp ON universal_transaction_matview_temp USING BTREE("funding_agency_id" ASC NULLS LAST) WITH (fillfactor = 97) WHERE "funding_agency_id" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_awarding_toptier_agency_name_temp ON universal_transaction_matview_temp USING BTREE("awarding_toptier_agency_name") WITH (fillfactor = 97) WHERE "awarding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_awarding_subtier_agency_name_temp ON universal_transaction_matview_temp USING BTREE("awarding_subtier_agency_name") WITH (fillfactor = 97) WHERE "awarding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_funding_toptier_agency_name_temp ON universal_transaction_matview_temp USING BTREE("funding_toptier_agency_name") WITH (fillfactor = 97) WHERE "funding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_funding_subtier_agency_name_temp ON universal_transaction_matview_temp USING BTREE("funding_subtier_agency_name") WITH (fillfactor = 97) WHERE "funding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_recipient_location_country_code_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_country_code") WITH (fillfactor = 97) WHERE "recipient_location_country_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_recipient_location_state_code_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_state_code") WITH (fillfactor = 97) WHERE "recipient_location_state_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_recipient_location_county_code_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_county_code") WITH (fillfactor = 97) WHERE "recipient_location_county_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_recipient_location_zip5_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_zip5") WITH (fillfactor = 97) WHERE "recipient_location_zip5" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_recipient_location_congressional_code_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_congressional_code") WITH (fillfactor = 97) WHERE "recipient_location_congressional_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_cfda_multi_temp ON universal_transaction_matview_temp USING BTREE("cfda_number", "cfda_title") WITH (fillfactor = 97) WHERE "cfda_number" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_pulled_from_temp ON universal_transaction_matview_temp USING BTREE("pulled_from") WITH (fillfactor = 97) WHERE "pulled_from" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_type_of_contract_pricing_temp ON universal_transaction_matview_temp USING BTREE("type_of_contract_pricing") WITH (fillfactor = 97) WHERE "type_of_contract_pricing" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_extent_competed_temp ON universal_transaction_matview_temp USING BTREE("extent_competed") WITH (fillfactor = 97) WHERE "extent_competed" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_type_set_aside_temp ON universal_transaction_matview_temp USING BTREE("type_set_aside") WITH (fillfactor = 97) WHERE "type_set_aside" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_product_or_service_code_temp ON universal_transaction_matview_temp USING BTREE("product_or_service_code") WITH (fillfactor = 97) WHERE "product_or_service_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_gin_naics_code_temp ON universal_transaction_matview_temp USING GIN("naics_code" gin_trgm_ops);
CREATE INDEX idx_7d34d470$77c_naics_code_temp ON universal_transaction_matview_temp USING BTREE("naics_code") WITH (fillfactor = 97) WHERE "naics_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$77c_business_categories_temp ON universal_transaction_matview_temp USING GIN("business_categories");
CREATE INDEX idx_7d34d470$77c_keyword_string_temp ON universal_transaction_matview_temp USING GIN("keyword_string" gin_trgm_ops);
CREATE INDEX idx_7d34d470$77c_award_id_string_temp ON universal_transaction_matview_temp USING GIN("award_id_string" gin_trgm_ops);
CREATE INDEX idx_7d34d470$77c_compound_geo_pop_1_temp ON universal_transaction_matview_temp USING BTREE("pop_country_code", "pop_state_code", "pop_county_code", "fiscal_year") WITH (fillfactor = 97) WHERE "pop_country_code" = 'USA';
CREATE INDEX idx_7d34d470$77c_compound_geo_pop_2_temp ON universal_transaction_matview_temp USING BTREE("pop_country_code", "pop_state_code", "pop_congressional_code", "fiscal_year") WITH (fillfactor = 97) WHERE "pop_country_code" = 'USA';
CREATE INDEX idx_7d34d470$77c_compound_geo_pop_3_temp ON universal_transaction_matview_temp USING BTREE("pop_country_code", "pop_zip5", "fiscal_year") WITH (fillfactor = 97) WHERE "pop_country_code" = 'USA';
CREATE INDEX idx_7d34d470$77c_compound_geo_rl_1_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_country_code", "recipient_location_state_code", "recipient_location_county_code", "fiscal_year") WITH (fillfactor = 97) WHERE "recipient_location_country_code" = 'USA';
CREATE INDEX idx_7d34d470$77c_compound_geo_rl_2_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_country_code", "recipient_location_state_code", "recipient_location_congressional_code", "fiscal_year") WITH (fillfactor = 97) WHERE "recipient_location_country_code" = 'USA';
CREATE INDEX idx_7d34d470$77c_compound_geo_rl_3_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_country_code", "recipient_location_zip5", "fiscal_year") WITH (fillfactor = 97) WHERE "recipient_location_country_code" = 'USA';

ALTER MATERIALIZED VIEW IF EXISTS universal_transaction_matview RENAME TO universal_transaction_matview_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_transaction_id RENAME TO idx_7d34d470$77c_transaction_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_action_date RENAME TO idx_7d34d470$77c_action_date_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_fiscal_year RENAME TO idx_7d34d470$77c_fiscal_year_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_type RENAME TO idx_7d34d470$77c_type_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_ordered_type RENAME TO idx_7d34d470$77c_ordered_type_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_action_type RENAME TO idx_7d34d470$77c_action_type_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_award_id RENAME TO idx_7d34d470$77c_award_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_award_category RENAME TO idx_7d34d470$77c_award_category_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_total_obligation RENAME TO idx_7d34d470$77c_total_obligation_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_ordered_total_obligation RENAME TO idx_7d34d470$77c_ordered_total_obligation_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_total_obl_bin RENAME TO idx_7d34d470$77c_total_obl_bin_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_total_subsidy_cost RENAME TO idx_7d34d470$77c_total_subsidy_cost_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_ordered_total_subsidy_cost RENAME TO idx_7d34d470$77c_ordered_total_subsidy_cost_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_pop_country_code RENAME TO idx_7d34d470$77c_pop_country_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_pop_state_code RENAME TO idx_7d34d470$77c_pop_state_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_pop_county_code RENAME TO idx_7d34d470$77c_pop_county_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_pop_zip5 RENAME TO idx_7d34d470$77c_pop_zip5_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_pop_congressional_code RENAME TO idx_7d34d470$77c_pop_congressional_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_gin_recipient_name RENAME TO idx_7d34d470$77c_gin_recipient_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_gin_recipient_unique_id RENAME TO idx_7d34d470$77c_gin_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_gin_parent_recipient_unique_id RENAME TO idx_7d34d470$77c_gin_parent_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_recipient_id RENAME TO idx_7d34d470$77c_recipient_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_recipient_name RENAME TO idx_7d34d470$77c_recipient_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_recipient_unique_id RENAME TO idx_7d34d470$77c_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_parent_recipient_unique_id RENAME TO idx_7d34d470$77c_parent_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_awarding_agency_id RENAME TO idx_7d34d470$77c_awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_funding_agency_id RENAME TO idx_7d34d470$77c_funding_agency_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_awarding_toptier_agency_name RENAME TO idx_7d34d470$77c_awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_awarding_subtier_agency_name RENAME TO idx_7d34d470$77c_awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_funding_toptier_agency_name RENAME TO idx_7d34d470$77c_funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_funding_subtier_agency_name RENAME TO idx_7d34d470$77c_funding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_recipient_location_country_code RENAME TO idx_7d34d470$77c_recipient_location_country_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_recipient_location_state_code RENAME TO idx_7d34d470$77c_recipient_location_state_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_recipient_location_county_code RENAME TO idx_7d34d470$77c_recipient_location_county_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_recipient_location_zip5 RENAME TO idx_7d34d470$77c_recipient_location_zip5_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_recipient_location_congressional_code RENAME TO idx_7d34d470$77c_recipient_location_congressional_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_cfda_multi RENAME TO idx_7d34d470$77c_cfda_multi_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_pulled_from RENAME TO idx_7d34d470$77c_pulled_from_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_type_of_contract_pricing RENAME TO idx_7d34d470$77c_type_of_contract_pricing_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_extent_competed RENAME TO idx_7d34d470$77c_extent_competed_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_type_set_aside RENAME TO idx_7d34d470$77c_type_set_aside_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_product_or_service_code RENAME TO idx_7d34d470$77c_product_or_service_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_gin_naics_code RENAME TO idx_7d34d470$77c_gin_naics_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_naics_code RENAME TO idx_7d34d470$77c_naics_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_business_categories RENAME TO idx_7d34d470$77c_business_categories_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_keyword_string RENAME TO idx_7d34d470$77c_keyword_string_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_award_id_string RENAME TO idx_7d34d470$77c_award_id_string_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_compound_geo_pop_1 RENAME TO idx_7d34d470$77c_compound_geo_pop_1_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_compound_geo_pop_2 RENAME TO idx_7d34d470$77c_compound_geo_pop_2_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_compound_geo_pop_3 RENAME TO idx_7d34d470$77c_compound_geo_pop_3_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_compound_geo_rl_1 RENAME TO idx_7d34d470$77c_compound_geo_rl_1_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_compound_geo_rl_2 RENAME TO idx_7d34d470$77c_compound_geo_rl_2_old;
ALTER INDEX IF EXISTS idx_7d34d470$77c_compound_geo_rl_3 RENAME TO idx_7d34d470$77c_compound_geo_rl_3_old;

ALTER MATERIALIZED VIEW universal_transaction_matview_temp RENAME TO universal_transaction_matview;
ALTER INDEX idx_7d34d470$77c_transaction_id_temp RENAME TO idx_7d34d470$77c_transaction_id;
ALTER INDEX idx_7d34d470$77c_action_date_temp RENAME TO idx_7d34d470$77c_action_date;
ALTER INDEX idx_7d34d470$77c_fiscal_year_temp RENAME TO idx_7d34d470$77c_fiscal_year;
ALTER INDEX idx_7d34d470$77c_type_temp RENAME TO idx_7d34d470$77c_type;
ALTER INDEX idx_7d34d470$77c_ordered_type_temp RENAME TO idx_7d34d470$77c_ordered_type;
ALTER INDEX idx_7d34d470$77c_action_type_temp RENAME TO idx_7d34d470$77c_action_type;
ALTER INDEX idx_7d34d470$77c_award_id_temp RENAME TO idx_7d34d470$77c_award_id;
ALTER INDEX idx_7d34d470$77c_award_category_temp RENAME TO idx_7d34d470$77c_award_category;
ALTER INDEX idx_7d34d470$77c_total_obligation_temp RENAME TO idx_7d34d470$77c_total_obligation;
ALTER INDEX idx_7d34d470$77c_ordered_total_obligation_temp RENAME TO idx_7d34d470$77c_ordered_total_obligation;
ALTER INDEX idx_7d34d470$77c_total_obl_bin_temp RENAME TO idx_7d34d470$77c_total_obl_bin;
ALTER INDEX idx_7d34d470$77c_total_subsidy_cost_temp RENAME TO idx_7d34d470$77c_total_subsidy_cost;
ALTER INDEX idx_7d34d470$77c_ordered_total_subsidy_cost_temp RENAME TO idx_7d34d470$77c_ordered_total_subsidy_cost;
ALTER INDEX idx_7d34d470$77c_pop_country_code_temp RENAME TO idx_7d34d470$77c_pop_country_code;
ALTER INDEX idx_7d34d470$77c_pop_state_code_temp RENAME TO idx_7d34d470$77c_pop_state_code;
ALTER INDEX idx_7d34d470$77c_pop_county_code_temp RENAME TO idx_7d34d470$77c_pop_county_code;
ALTER INDEX idx_7d34d470$77c_pop_zip5_temp RENAME TO idx_7d34d470$77c_pop_zip5;
ALTER INDEX idx_7d34d470$77c_pop_congressional_code_temp RENAME TO idx_7d34d470$77c_pop_congressional_code;
ALTER INDEX idx_7d34d470$77c_gin_recipient_name_temp RENAME TO idx_7d34d470$77c_gin_recipient_name;
ALTER INDEX idx_7d34d470$77c_gin_recipient_unique_id_temp RENAME TO idx_7d34d470$77c_gin_recipient_unique_id;
ALTER INDEX idx_7d34d470$77c_gin_parent_recipient_unique_id_temp RENAME TO idx_7d34d470$77c_gin_parent_recipient_unique_id;
ALTER INDEX idx_7d34d470$77c_recipient_id_temp RENAME TO idx_7d34d470$77c_recipient_id;
ALTER INDEX idx_7d34d470$77c_recipient_name_temp RENAME TO idx_7d34d470$77c_recipient_name;
ALTER INDEX idx_7d34d470$77c_recipient_unique_id_temp RENAME TO idx_7d34d470$77c_recipient_unique_id;
ALTER INDEX idx_7d34d470$77c_parent_recipient_unique_id_temp RENAME TO idx_7d34d470$77c_parent_recipient_unique_id;
ALTER INDEX idx_7d34d470$77c_awarding_agency_id_temp RENAME TO idx_7d34d470$77c_awarding_agency_id;
ALTER INDEX idx_7d34d470$77c_funding_agency_id_temp RENAME TO idx_7d34d470$77c_funding_agency_id;
ALTER INDEX idx_7d34d470$77c_awarding_toptier_agency_name_temp RENAME TO idx_7d34d470$77c_awarding_toptier_agency_name;
ALTER INDEX idx_7d34d470$77c_awarding_subtier_agency_name_temp RENAME TO idx_7d34d470$77c_awarding_subtier_agency_name;
ALTER INDEX idx_7d34d470$77c_funding_toptier_agency_name_temp RENAME TO idx_7d34d470$77c_funding_toptier_agency_name;
ALTER INDEX idx_7d34d470$77c_funding_subtier_agency_name_temp RENAME TO idx_7d34d470$77c_funding_subtier_agency_name;
ALTER INDEX idx_7d34d470$77c_recipient_location_country_code_temp RENAME TO idx_7d34d470$77c_recipient_location_country_code;
ALTER INDEX idx_7d34d470$77c_recipient_location_state_code_temp RENAME TO idx_7d34d470$77c_recipient_location_state_code;
ALTER INDEX idx_7d34d470$77c_recipient_location_county_code_temp RENAME TO idx_7d34d470$77c_recipient_location_county_code;
ALTER INDEX idx_7d34d470$77c_recipient_location_zip5_temp RENAME TO idx_7d34d470$77c_recipient_location_zip5;
ALTER INDEX idx_7d34d470$77c_recipient_location_congressional_code_temp RENAME TO idx_7d34d470$77c_recipient_location_congressional_code;
ALTER INDEX idx_7d34d470$77c_cfda_multi_temp RENAME TO idx_7d34d470$77c_cfda_multi;
ALTER INDEX idx_7d34d470$77c_pulled_from_temp RENAME TO idx_7d34d470$77c_pulled_from;
ALTER INDEX idx_7d34d470$77c_type_of_contract_pricing_temp RENAME TO idx_7d34d470$77c_type_of_contract_pricing;
ALTER INDEX idx_7d34d470$77c_extent_competed_temp RENAME TO idx_7d34d470$77c_extent_competed;
ALTER INDEX idx_7d34d470$77c_type_set_aside_temp RENAME TO idx_7d34d470$77c_type_set_aside;
ALTER INDEX idx_7d34d470$77c_product_or_service_code_temp RENAME TO idx_7d34d470$77c_product_or_service_code;
ALTER INDEX idx_7d34d470$77c_gin_naics_code_temp RENAME TO idx_7d34d470$77c_gin_naics_code;
ALTER INDEX idx_7d34d470$77c_naics_code_temp RENAME TO idx_7d34d470$77c_naics_code;
ALTER INDEX idx_7d34d470$77c_business_categories_temp RENAME TO idx_7d34d470$77c_business_categories;
ALTER INDEX idx_7d34d470$77c_keyword_string_temp RENAME TO idx_7d34d470$77c_keyword_string;
ALTER INDEX idx_7d34d470$77c_award_id_string_temp RENAME TO idx_7d34d470$77c_award_id_string;
ALTER INDEX idx_7d34d470$77c_compound_geo_pop_1_temp RENAME TO idx_7d34d470$77c_compound_geo_pop_1;
ALTER INDEX idx_7d34d470$77c_compound_geo_pop_2_temp RENAME TO idx_7d34d470$77c_compound_geo_pop_2;
ALTER INDEX idx_7d34d470$77c_compound_geo_pop_3_temp RENAME TO idx_7d34d470$77c_compound_geo_pop_3;
ALTER INDEX idx_7d34d470$77c_compound_geo_rl_1_temp RENAME TO idx_7d34d470$77c_compound_geo_rl_1;
ALTER INDEX idx_7d34d470$77c_compound_geo_rl_2_temp RENAME TO idx_7d34d470$77c_compound_geo_rl_2;
ALTER INDEX idx_7d34d470$77c_compound_geo_rl_3_temp RENAME TO idx_7d34d470$77c_compound_geo_rl_3;

ANALYZE VERBOSE universal_transaction_matview;
GRANT SELECT ON universal_transaction_matview TO readonly;
