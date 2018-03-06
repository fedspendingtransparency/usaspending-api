--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS universal_award_matview_temp CASCADE;
DROP MATERIALIZED VIEW IF EXISTS universal_award_matview_old CASCADE;

CREATE MATERIALIZED VIEW universal_award_matview_temp AS
SELECT
  UPPER(CONCAT(
    recipient."recipient_name",
    ' ', contract_data."naics",
    ' ', contract_data."naics_description",
    ' ', psc."description",
    ' ', "awards"."description")) AS keyword_string,
  UPPER(CONCAT(awards.piid, ' ', awards.fain, ' ', awards.uri)) AS award_id_string,
  "awards"."id" AS award_id,
  "awards"."category",
  "awards"."type",
  "awards"."type_description",
  "awards"."piid",
  "awards"."fain",
  "awards"."uri",
  "awards"."total_obligation",
  obligation_to_enum("awards"."total_obligation") AS total_obl_bin,
  "awards"."total_subsidy_cost",

  "awards"."recipient_id",
  UPPER(recipient."recipient_name") AS recipient_name,
  recipient."recipient_unique_id",
  recipient."parent_recipient_unique_id",
  recipient."business_categories",

  latest_transaction."action_date",
  latest_transaction."fiscal_year",
  "awards"."period_of_performance_start_date",
  "awards"."period_of_performance_current_end_date",

  assistance_data."face_value_loan_guarantee",
  assistance_data."original_loan_subsidy_cost",

  latest_transaction."awarding_agency_id",
  latest_transaction."funding_agency_id",
  TAA."name" AS awarding_toptier_agency_name,
  TFA."name" AS funding_toptier_agency_name,
  SAA."name" AS awarding_subtier_agency_name,
  SFA."name" AS funding_subtier_agency_name,

  recipient_location."country_name" AS recipient_location_country_name,
  recipient_location."location_country_code" AS recipient_location_country_code,
  recipient_location."state_code" AS recipient_location_state_code,
  recipient_location."county_code" AS recipient_location_county_code,
  recipient_location."county_name" AS recipient_location_county_name,
  recipient_location."zip5" AS recipient_location_zip5,
  recipient_location."congressional_code" AS recipient_location_congressional_code,

  place_of_performance."country_name" AS pop_country_name,
  place_of_performance."location_country_code" AS pop_country_code,
  place_of_performance."state_code" AS pop_state_code,
  place_of_performance."county_code" AS pop_county_code,
  place_of_performance."county_name" AS pop_county_name,
  place_of_performance."zip5" AS pop_zip5,
  place_of_performance."congressional_code" AS pop_congressional_code,

  assistance_data."cfda_number",
  contract_data."pulled_from",
  contract_data."type_of_contract_pricing",
  contract_data."extent_competed",
  contract_data."type_set_aside",

  contract_data."product_or_service_code",
  "psc"."description" AS product_or_service_description,
  contract_data."naics" AS naics_code,
  contract_data."naics_description"
FROM
  "awards"
LEFT OUTER JOIN
  "transaction_normalized" AS latest_transaction
    ON ("awards"."latest_transaction_id" = latest_transaction."id")
LEFT OUTER JOIN
  "transaction_fabs" AS assistance_data
    ON (latest_transaction."id" = assistance_data."transaction_id")
LEFT OUTER JOIN
  "transaction_fpds" AS contract_data
    ON (latest_transaction."id" = contract_data."transaction_id")
LEFT OUTER JOIN
  "legal_entity" AS recipient
    ON ("awards"."recipient_id" = recipient."legal_entity_id")
LEFT OUTER JOIN
  "references_location" AS recipient_location
    ON (recipient."location_id" = recipient_location."location_id")
LEFT OUTER JOIN
  "references_location" AS place_of_performance
    ON ("awards"."place_of_performance_id" = place_of_performance."location_id")
LEFT OUTER JOIN
  "psc" ON (contract_data."product_or_service_code" = "psc"."code")
LEFT OUTER JOIN
  "agency" AS AA
    ON ("awards"."awarding_agency_id" = AA."id")
LEFT OUTER JOIN
  "toptier_agency" AS TAA
    ON (AA."toptier_agency_id" = TAA."toptier_agency_id")
LEFT OUTER JOIN
  "subtier_agency" AS SAA
    ON (AA."subtier_agency_id" = SAA."subtier_agency_id")
LEFT OUTER JOIN
  "office_agency" AS AAO
    ON (AA."office_agency_id" = AAO."office_agency_id")
LEFT OUTER JOIN
  "agency" AS FA ON ("awards"."funding_agency_id" = FA."id")
LEFT OUTER JOIN
  "toptier_agency" AS TFA
    ON (FA."toptier_agency_id" = TFA."toptier_agency_id")
LEFT OUTER JOIN
  "subtier_agency" AS SFA
    ON (FA."subtier_agency_id" = SFA."subtier_agency_id")
LEFT OUTER JOIN
  "office_agency" AS FAO
    ON (FA."office_agency_id" = FAO."office_agency_id")
WHERE
  "awards"."latest_transaction_id" IS NOT NULL AND
  ("awards"."category" IS NOT NULL or "contract_data"."pulled_from"='IDV') AND
  latest_transaction."action_date" >= '2007-10-01'
ORDER BY
  latest_transaction."action_date" DESC;

CREATE UNIQUE INDEX idx_7d34d470$5f5_id_temp ON universal_award_matview_temp USING BTREE("award_id") WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_category_temp ON universal_award_matview_temp USING BTREE("category") WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_type_temp ON universal_award_matview_temp USING BTREE("type") WITH (fillfactor = 97) WHERE "type" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_ordered_type_temp ON universal_award_matview_temp USING BTREE("type" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_ordered_type_desc_temp ON universal_award_matview_temp USING BTREE("type_description" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_ordered_fain_temp ON universal_award_matview_temp USING BTREE(UPPER("fain") DESC NULLS LAST) WITH (fillfactor = 97) WHERE "fain" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_ordered_piid_temp ON universal_award_matview_temp USING BTREE(UPPER("piid") DESC NULLS LAST) WITH (fillfactor = 97) WHERE "piid" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_total_obligation_temp ON universal_award_matview_temp USING BTREE("total_obligation") WITH (fillfactor = 97) WHERE "total_obligation" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_ordered_total_obligation_temp ON universal_award_matview_temp USING BTREE("total_obligation" DESC) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_total_obl_bin_temp ON universal_award_matview_temp USING BTREE("total_obl_bin") WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_total_subsidy_cost_temp ON universal_award_matview_temp USING BTREE("total_subsidy_cost") WITH (fillfactor = 97) WHERE "total_subsidy_cost" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_ordered_total_subsidy_cost_temp ON universal_award_matview_temp USING BTREE("total_subsidy_cost" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_period_of_performance_start_date_temp ON universal_award_matview_temp USING BTREE("period_of_performance_start_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_period_of_performance_current_end_date_temp ON universal_award_matview_temp USING BTREE("period_of_performance_current_end_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_gin_recipient_name_temp ON universal_award_matview_temp USING GIN("recipient_name" gin_trgm_ops);
CREATE INDEX idx_7d34d470$5f5_recipient_name_temp ON universal_award_matview_temp USING BTREE("recipient_name") WITH (fillfactor = 97) WHERE "recipient_name" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_recipient_unique_id_temp ON universal_award_matview_temp USING BTREE("recipient_unique_id") WITH (fillfactor = 97) WHERE "recipient_unique_id" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_parent_recipient_unique_id_temp ON universal_award_matview_temp USING BTREE("parent_recipient_unique_id") WITH (fillfactor = 97) WHERE "parent_recipient_unique_id" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_action_date_temp ON universal_award_matview_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_fiscal_year_temp ON universal_award_matview_temp USING BTREE("fiscal_year" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_awarding_agency_id_temp ON universal_award_matview_temp USING BTREE("awarding_agency_id" ASC NULLS LAST) WITH (fillfactor = 97) WHERE "awarding_agency_id" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_funding_agency_id_temp ON universal_award_matview_temp USING BTREE("funding_agency_id" ASC NULLS LAST) WITH (fillfactor = 97) WHERE "funding_agency_id" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_ordered_awarding_toptier_agency_name_temp ON universal_award_matview_temp USING BTREE("awarding_toptier_agency_name" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_ordered_awarding_subtier_agency_name_temp ON universal_award_matview_temp USING BTREE("awarding_subtier_agency_name" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_awarding_toptier_agency_name_temp ON universal_award_matview_temp USING BTREE("awarding_toptier_agency_name") WITH (fillfactor = 97) WHERE "awarding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_awarding_subtier_agency_name_temp ON universal_award_matview_temp USING BTREE("awarding_subtier_agency_name") WITH (fillfactor = 97) WHERE "awarding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_funding_toptier_agency_name_temp ON universal_award_matview_temp USING BTREE("funding_toptier_agency_name") WITH (fillfactor = 97) WHERE "funding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_funding_subtier_agency_name_temp ON universal_award_matview_temp USING BTREE("funding_subtier_agency_name") WITH (fillfactor = 97) WHERE "funding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_recipient_location_country_code_temp ON universal_award_matview_temp USING BTREE("recipient_location_country_code") WITH (fillfactor = 97) WHERE "recipient_location_country_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_recipient_location_state_code_temp ON universal_award_matview_temp USING BTREE("recipient_location_state_code") WITH (fillfactor = 97) WHERE "recipient_location_state_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_recipient_location_county_code_temp ON universal_award_matview_temp USING BTREE("recipient_location_county_code") WITH (fillfactor = 97) WHERE "recipient_location_county_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_recipient_location_zip5_temp ON universal_award_matview_temp USING BTREE("recipient_location_zip5") WITH (fillfactor = 97) WHERE "recipient_location_zip5" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_recipient_location_cong_code_temp ON universal_award_matview_temp USING BTREE("recipient_location_congressional_code") WITH (fillfactor = 97) WHERE "recipient_location_congressional_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_pop_country_code_temp ON universal_award_matview_temp USING BTREE("pop_country_code") WITH (fillfactor = 97) WHERE "pop_country_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_pop_state_code_temp ON universal_award_matview_temp USING BTREE("pop_state_code") WITH (fillfactor = 97) WHERE "pop_state_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_pop_county_code_temp ON universal_award_matview_temp USING BTREE("pop_county_code") WITH (fillfactor = 97) WHERE "pop_county_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_pop_zip5_temp ON universal_award_matview_temp USING BTREE("pop_zip5") WITH (fillfactor = 97) WHERE "pop_zip5" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_pop_congressional_code_temp ON universal_award_matview_temp USING BTREE("pop_congressional_code") WITH (fillfactor = 97) WHERE "pop_congressional_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_cfda_number_temp ON universal_award_matview_temp USING BTREE("cfda_number") WITH (fillfactor = 97) WHERE "cfda_number" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_pulled_from_temp ON universal_award_matview_temp USING BTREE("pulled_from") WITH (fillfactor = 97) WHERE "pulled_from" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_type_of_contract_pricing_temp ON universal_award_matview_temp USING BTREE("type_of_contract_pricing") WITH (fillfactor = 97) WHERE "type_of_contract_pricing" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_extent_competed_temp ON universal_award_matview_temp USING BTREE("extent_competed") WITH (fillfactor = 97) WHERE "extent_competed" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_type_set_aside_temp ON universal_award_matview_temp USING BTREE("type_set_aside") WITH (fillfactor = 97) WHERE "type_set_aside" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_product_or_service_code_temp ON universal_award_matview_temp USING BTREE("product_or_service_code") WITH (fillfactor = 97) WHERE "product_or_service_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_gin_product_or_service_description_temp ON universal_award_matview_temp USING GIN(("product_or_service_description") gin_trgm_ops);
CREATE INDEX idx_7d34d470$5f5_naics_temp ON universal_award_matview_temp USING BTREE("naics_code") WITH (fillfactor = 97) WHERE "naics_code" IS NOT NULL;
CREATE INDEX idx_7d34d470$5f5_gin_naics_code_temp ON universal_award_matview_temp USING GIN("naics_code" gin_trgm_ops);
CREATE INDEX idx_7d34d470$5f5_gin_naics_description_temp ON universal_award_matview_temp USING GIN(UPPER("naics_description") gin_trgm_ops);
CREATE INDEX idx_7d34d470$5f5_gin_business_categories_temp ON universal_award_matview_temp USING GIN("business_categories");
CREATE INDEX idx_7d34d470$5f5_gin_keyword_string_temp ON universal_award_matview_temp USING GIN("keyword_string" gin_trgm_ops);
CREATE INDEX idx_7d34d470$5f5_gin_award_id_string_temp ON universal_award_matview_temp USING GIN("award_id_string" gin_trgm_ops);
CREATE INDEX idx_7d34d470$5f5_compound_psc_fy_temp ON universal_award_matview_temp USING BTREE("product_or_service_code", "fiscal_year") WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_compound_naics_fy_temp ON universal_award_matview_temp USING BTREE("naics_code", "fiscal_year") WITH (fillfactor = 97);
CREATE INDEX idx_7d34d470$5f5_compound_cfda_fy_temp ON universal_award_matview_temp USING BTREE("cfda_number", "fiscal_year") WITH (fillfactor = 97);

ALTER MATERIALIZED VIEW IF EXISTS universal_award_matview RENAME TO universal_award_matview_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_id RENAME TO idx_7d34d470$5f5_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_category RENAME TO idx_7d34d470$5f5_category_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_type RENAME TO idx_7d34d470$5f5_type_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_ordered_type RENAME TO idx_7d34d470$5f5_ordered_type_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_ordered_type_desc RENAME TO idx_7d34d470$5f5_ordered_type_desc_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_ordered_fain RENAME TO idx_7d34d470$5f5_ordered_fain_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_ordered_piid RENAME TO idx_7d34d470$5f5_ordered_piid_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_total_obligation RENAME TO idx_7d34d470$5f5_total_obligation_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_ordered_total_obligation RENAME TO idx_7d34d470$5f5_ordered_total_obligation_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_total_obl_bin RENAME TO idx_7d34d470$5f5_total_obl_bin_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_total_subsidy_cost RENAME TO idx_7d34d470$5f5_total_subsidy_cost_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_ordered_total_subsidy_cost RENAME TO idx_7d34d470$5f5_ordered_total_subsidy_cost_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_period_of_performance_start_date RENAME TO idx_7d34d470$5f5_period_of_performance_start_date_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_period_of_performance_current_end_date RENAME TO idx_7d34d470$5f5_period_of_performance_current_end_date_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_gin_recipient_name RENAME TO idx_7d34d470$5f5_gin_recipient_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_recipient_name RENAME TO idx_7d34d470$5f5_recipient_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_recipient_unique_id RENAME TO idx_7d34d470$5f5_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_parent_recipient_unique_id RENAME TO idx_7d34d470$5f5_parent_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_action_date RENAME TO idx_7d34d470$5f5_action_date_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_fiscal_year RENAME TO idx_7d34d470$5f5_fiscal_year_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_awarding_agency_id RENAME TO idx_7d34d470$5f5_awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_funding_agency_id RENAME TO idx_7d34d470$5f5_funding_agency_id_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_ordered_awarding_toptier_agency_name RENAME TO idx_7d34d470$5f5_ordered_awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_ordered_awarding_subtier_agency_name RENAME TO idx_7d34d470$5f5_ordered_awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_awarding_toptier_agency_name RENAME TO idx_7d34d470$5f5_awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_awarding_subtier_agency_name RENAME TO idx_7d34d470$5f5_awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_funding_toptier_agency_name RENAME TO idx_7d34d470$5f5_funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_funding_subtier_agency_name RENAME TO idx_7d34d470$5f5_funding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_recipient_location_country_code RENAME TO idx_7d34d470$5f5_recipient_location_country_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_recipient_location_state_code RENAME TO idx_7d34d470$5f5_recipient_location_state_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_recipient_location_county_code RENAME TO idx_7d34d470$5f5_recipient_location_county_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_recipient_location_zip5 RENAME TO idx_7d34d470$5f5_recipient_location_zip5_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_recipient_location_cong_code RENAME TO idx_7d34d470$5f5_recipient_location_cong_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_pop_country_code RENAME TO idx_7d34d470$5f5_pop_country_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_pop_state_code RENAME TO idx_7d34d470$5f5_pop_state_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_pop_county_code RENAME TO idx_7d34d470$5f5_pop_county_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_pop_zip5 RENAME TO idx_7d34d470$5f5_pop_zip5_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_pop_congressional_code RENAME TO idx_7d34d470$5f5_pop_congressional_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_cfda_number RENAME TO idx_7d34d470$5f5_cfda_number_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_pulled_from RENAME TO idx_7d34d470$5f5_pulled_from_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_type_of_contract_pricing RENAME TO idx_7d34d470$5f5_type_of_contract_pricing_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_extent_competed RENAME TO idx_7d34d470$5f5_extent_competed_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_type_set_aside RENAME TO idx_7d34d470$5f5_type_set_aside_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_product_or_service_code RENAME TO idx_7d34d470$5f5_product_or_service_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_gin_product_or_service_description RENAME TO idx_7d34d470$5f5_gin_product_or_service_description_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_naics RENAME TO idx_7d34d470$5f5_naics_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_gin_naics_code RENAME TO idx_7d34d470$5f5_gin_naics_code_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_gin_naics_description RENAME TO idx_7d34d470$5f5_gin_naics_description_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_gin_business_categories RENAME TO idx_7d34d470$5f5_gin_business_categories_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_gin_keyword_string RENAME TO idx_7d34d470$5f5_gin_keyword_string_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_gin_award_id_string RENAME TO idx_7d34d470$5f5_gin_award_id_string_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_compound_psc_fy RENAME TO idx_7d34d470$5f5_compound_psc_fy_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_compound_naics_fy RENAME TO idx_7d34d470$5f5_compound_naics_fy_old;
ALTER INDEX IF EXISTS idx_7d34d470$5f5_compound_cfda_fy RENAME TO idx_7d34d470$5f5_compound_cfda_fy_old;

ALTER MATERIALIZED VIEW universal_award_matview_temp RENAME TO universal_award_matview;
ALTER INDEX idx_7d34d470$5f5_id_temp RENAME TO idx_7d34d470$5f5_id;
ALTER INDEX idx_7d34d470$5f5_category_temp RENAME TO idx_7d34d470$5f5_category;
ALTER INDEX idx_7d34d470$5f5_type_temp RENAME TO idx_7d34d470$5f5_type;
ALTER INDEX idx_7d34d470$5f5_ordered_type_temp RENAME TO idx_7d34d470$5f5_ordered_type;
ALTER INDEX idx_7d34d470$5f5_ordered_type_desc_temp RENAME TO idx_7d34d470$5f5_ordered_type_desc;
ALTER INDEX idx_7d34d470$5f5_ordered_fain_temp RENAME TO idx_7d34d470$5f5_ordered_fain;
ALTER INDEX idx_7d34d470$5f5_ordered_piid_temp RENAME TO idx_7d34d470$5f5_ordered_piid;
ALTER INDEX idx_7d34d470$5f5_total_obligation_temp RENAME TO idx_7d34d470$5f5_total_obligation;
ALTER INDEX idx_7d34d470$5f5_ordered_total_obligation_temp RENAME TO idx_7d34d470$5f5_ordered_total_obligation;
ALTER INDEX idx_7d34d470$5f5_total_obl_bin_temp RENAME TO idx_7d34d470$5f5_total_obl_bin;
ALTER INDEX idx_7d34d470$5f5_total_subsidy_cost_temp RENAME TO idx_7d34d470$5f5_total_subsidy_cost;
ALTER INDEX idx_7d34d470$5f5_ordered_total_subsidy_cost_temp RENAME TO idx_7d34d470$5f5_ordered_total_subsidy_cost;
ALTER INDEX idx_7d34d470$5f5_period_of_performance_start_date_temp RENAME TO idx_7d34d470$5f5_period_of_performance_start_date;
ALTER INDEX idx_7d34d470$5f5_period_of_performance_current_end_date_temp RENAME TO idx_7d34d470$5f5_period_of_performance_current_end_date;
ALTER INDEX idx_7d34d470$5f5_gin_recipient_name_temp RENAME TO idx_7d34d470$5f5_gin_recipient_name;
ALTER INDEX idx_7d34d470$5f5_recipient_name_temp RENAME TO idx_7d34d470$5f5_recipient_name;
ALTER INDEX idx_7d34d470$5f5_recipient_unique_id_temp RENAME TO idx_7d34d470$5f5_recipient_unique_id;
ALTER INDEX idx_7d34d470$5f5_parent_recipient_unique_id_temp RENAME TO idx_7d34d470$5f5_parent_recipient_unique_id;
ALTER INDEX idx_7d34d470$5f5_action_date_temp RENAME TO idx_7d34d470$5f5_action_date;
ALTER INDEX idx_7d34d470$5f5_fiscal_year_temp RENAME TO idx_7d34d470$5f5_fiscal_year;
ALTER INDEX idx_7d34d470$5f5_awarding_agency_id_temp RENAME TO idx_7d34d470$5f5_awarding_agency_id;
ALTER INDEX idx_7d34d470$5f5_funding_agency_id_temp RENAME TO idx_7d34d470$5f5_funding_agency_id;
ALTER INDEX idx_7d34d470$5f5_ordered_awarding_toptier_agency_name_temp RENAME TO idx_7d34d470$5f5_ordered_awarding_toptier_agency_name;
ALTER INDEX idx_7d34d470$5f5_ordered_awarding_subtier_agency_name_temp RENAME TO idx_7d34d470$5f5_ordered_awarding_subtier_agency_name;
ALTER INDEX idx_7d34d470$5f5_awarding_toptier_agency_name_temp RENAME TO idx_7d34d470$5f5_awarding_toptier_agency_name;
ALTER INDEX idx_7d34d470$5f5_awarding_subtier_agency_name_temp RENAME TO idx_7d34d470$5f5_awarding_subtier_agency_name;
ALTER INDEX idx_7d34d470$5f5_funding_toptier_agency_name_temp RENAME TO idx_7d34d470$5f5_funding_toptier_agency_name;
ALTER INDEX idx_7d34d470$5f5_funding_subtier_agency_name_temp RENAME TO idx_7d34d470$5f5_funding_subtier_agency_name;
ALTER INDEX idx_7d34d470$5f5_recipient_location_country_code_temp RENAME TO idx_7d34d470$5f5_recipient_location_country_code;
ALTER INDEX idx_7d34d470$5f5_recipient_location_state_code_temp RENAME TO idx_7d34d470$5f5_recipient_location_state_code;
ALTER INDEX idx_7d34d470$5f5_recipient_location_county_code_temp RENAME TO idx_7d34d470$5f5_recipient_location_county_code;
ALTER INDEX idx_7d34d470$5f5_recipient_location_zip5_temp RENAME TO idx_7d34d470$5f5_recipient_location_zip5;
ALTER INDEX idx_7d34d470$5f5_recipient_location_cong_code_temp RENAME TO idx_7d34d470$5f5_recipient_location_cong_code;
ALTER INDEX idx_7d34d470$5f5_pop_country_code_temp RENAME TO idx_7d34d470$5f5_pop_country_code;
ALTER INDEX idx_7d34d470$5f5_pop_state_code_temp RENAME TO idx_7d34d470$5f5_pop_state_code;
ALTER INDEX idx_7d34d470$5f5_pop_county_code_temp RENAME TO idx_7d34d470$5f5_pop_county_code;
ALTER INDEX idx_7d34d470$5f5_pop_zip5_temp RENAME TO idx_7d34d470$5f5_pop_zip5;
ALTER INDEX idx_7d34d470$5f5_pop_congressional_code_temp RENAME TO idx_7d34d470$5f5_pop_congressional_code;
ALTER INDEX idx_7d34d470$5f5_cfda_number_temp RENAME TO idx_7d34d470$5f5_cfda_number;
ALTER INDEX idx_7d34d470$5f5_pulled_from_temp RENAME TO idx_7d34d470$5f5_pulled_from;
ALTER INDEX idx_7d34d470$5f5_type_of_contract_pricing_temp RENAME TO idx_7d34d470$5f5_type_of_contract_pricing;
ALTER INDEX idx_7d34d470$5f5_extent_competed_temp RENAME TO idx_7d34d470$5f5_extent_competed;
ALTER INDEX idx_7d34d470$5f5_type_set_aside_temp RENAME TO idx_7d34d470$5f5_type_set_aside;
ALTER INDEX idx_7d34d470$5f5_product_or_service_code_temp RENAME TO idx_7d34d470$5f5_product_or_service_code;
ALTER INDEX idx_7d34d470$5f5_gin_product_or_service_description_temp RENAME TO idx_7d34d470$5f5_gin_product_or_service_description;
ALTER INDEX idx_7d34d470$5f5_naics_temp RENAME TO idx_7d34d470$5f5_naics;
ALTER INDEX idx_7d34d470$5f5_gin_naics_code_temp RENAME TO idx_7d34d470$5f5_gin_naics_code;
ALTER INDEX idx_7d34d470$5f5_gin_naics_description_temp RENAME TO idx_7d34d470$5f5_gin_naics_description;
ALTER INDEX idx_7d34d470$5f5_gin_business_categories_temp RENAME TO idx_7d34d470$5f5_gin_business_categories;
ALTER INDEX idx_7d34d470$5f5_gin_keyword_string_temp RENAME TO idx_7d34d470$5f5_gin_keyword_string;
ALTER INDEX idx_7d34d470$5f5_gin_award_id_string_temp RENAME TO idx_7d34d470$5f5_gin_award_id_string;
ALTER INDEX idx_7d34d470$5f5_compound_psc_fy_temp RENAME TO idx_7d34d470$5f5_compound_psc_fy;
ALTER INDEX idx_7d34d470$5f5_compound_naics_fy_temp RENAME TO idx_7d34d470$5f5_compound_naics_fy;
ALTER INDEX idx_7d34d470$5f5_compound_cfda_fy_temp RENAME TO idx_7d34d470$5f5_compound_cfda_fy;

ANALYZE VERBOSE universal_award_matview;
GRANT SELECT ON universal_award_matview TO readonly;
