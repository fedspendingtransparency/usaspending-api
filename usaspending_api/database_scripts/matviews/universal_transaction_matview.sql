--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS universal_transaction_matview_temp;
DROP MATERIALIZED VIEW IF EXISTS universal_transaction_matview_old;

CREATE MATERIALIZED VIEW universal_transaction_matview_temp AS
SELECT
  "transaction_normalized"."id" AS transaction_id,
  "transaction_normalized"."action_date"::date,
  "transaction_normalized"."fiscal_year",
  "transaction_normalized"."type",
  "transaction_normalized"."action_type",
  "transaction_normalized"."award_id",
  "awards"."category" AS award_category,
  "awards"."total_obligation",
  obligation_to_enum("awards"."total_obligation") AS total_obl_bin,
  "awards"."fain",
  "awards"."uri",
  "awards"."piid",
  "transaction_normalized"."federal_action_obligation",
  place_of_performance."location_id" AS pop_location_id,
  place_of_performance."country_name" AS pop_country_name,
  place_of_performance."location_country_code" AS pop_country_code,
  place_of_performance."zip5" AS pop_zip5,
  place_of_performance."county_code" AS pop_county_code,
  place_of_performance."county_name" AS pop_county_name,
  place_of_performance."state_code" AS pop_state_code,
  place_of_performance."congressional_code" AS pop_congressional_code,
  "transaction_fabs"."face_value_loan_guarantee",
  "transaction_fabs"."original_loan_subsidy_cost",
  "transaction_normalized"."description" AS transaction_description,
  "transaction_normalized"."awarding_agency_id",
  "transaction_fabs"."awarding_agency_code",
  "transaction_fabs"."awarding_agency_name",
  "transaction_normalized"."funding_agency_id",
  "transaction_fabs"."funding_agency_code",
  "transaction_fabs"."funding_agency_name",
  "transaction_fpds"."naics" AS naics_code,
  "naics"."description" AS naics_description,
  "transaction_fpds"."product_or_service_code",
  "psc"."description" AS product_or_service_description,
  -- DUPLICATED ON 12/4. REMOVE BY JAN 1, 2018!!!!!!!!!!!!!!!
  "transaction_fpds"."product_or_service_code" AS psc_code,
  "psc"."description" AS psc_description,
  -- ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
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
  recipient_location."location_id" AS recipient_location_id,
  recipient_location."location_country_code" AS recipient_location_country_code,
  recipient_location."country_name" AS recipient_location_country_name,
  recipient_location."zip5" AS recipient_location_zip5,
  recipient_location."state_code" AS recipient_location_state_code,
  recipient_location."state_name" AS recipient_location_state_name,
  recipient_location."county_code" AS recipient_location_county_code,
  recipient_location."county_name" AS recipient_location_county_name,
  recipient_location."congressional_code" AS recipient_location_congressional_code,
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
  "transaction_normalized"."action_date" >= '2007-10-01' AND
  "transaction_normalized"."federal_action_obligation" IS NOT NULL;

CREATE INDEX idx_09492a43__transaction_id_temp ON universal_transaction_matview_temp USING BTREE("transaction_id") WITH (fillfactor = 100);
CREATE INDEX idx_09492a43__action_date_temp ON universal_transaction_matview_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_09492a43__fiscal_year_temp ON universal_transaction_matview_temp USING BTREE("fiscal_year" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_09492a43__type_temp ON universal_transaction_matview_temp USING BTREE("type") WITH (fillfactor = 100) WHERE "type" IS NOT NULL;
CREATE INDEX idx_09492a43__ordered_type_temp ON universal_transaction_matview_temp USING BTREE("type" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_09492a43__action_type_temp ON universal_transaction_matview_temp USING BTREE("action_type") WITH (fillfactor = 100);
CREATE INDEX idx_09492a43__award_id_temp ON universal_transaction_matview_temp USING BTREE("award_id") WITH (fillfactor = 100);
CREATE INDEX idx_09492a43__award_category_temp ON universal_transaction_matview_temp USING BTREE("award_category") WITH (fillfactor = 100) WHERE "award_category" IS NOT NULL;
CREATE INDEX idx_09492a43__total_obligation_temp ON universal_transaction_matview_temp USING BTREE("total_obligation") WITH (fillfactor = 100) WHERE "total_obligation" IS NOT NULL;
CREATE INDEX idx_09492a43__ordered_total_obligation_temp ON universal_transaction_matview_temp USING BTREE("total_obligation" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_09492a43__total_obl_bin_temp ON universal_transaction_matview_temp USING BTREE("total_obl_bin") WITH (fillfactor = 100);
CREATE INDEX idx_09492a43__fain_temp ON universal_transaction_matview_temp USING BTREE("fain") WITH (fillfactor = 100) WHERE "fain" IS NOT NULL;
CREATE INDEX idx_09492a43__ordered_fain_temp ON universal_transaction_matview_temp USING BTREE(UPPER("fain") DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_09492a43__gin_fain_temp ON universal_transaction_matview_temp USING GIN(UPPER("fain") gin_trgm_ops);
CREATE INDEX idx_09492a43__piid_temp ON universal_transaction_matview_temp USING BTREE("piid") WITH (fillfactor = 100) WHERE "piid" IS NOT NULL;
CREATE INDEX idx_09492a43__ordered_piid_temp ON universal_transaction_matview_temp USING BTREE(UPPER("piid") DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_09492a43__gin_pid_temp ON universal_transaction_matview_temp USING GIN(UPPER("piid") gin_trgm_ops);
CREATE INDEX idx_09492a43__uri_temp ON universal_transaction_matview_temp USING BTREE("uri") WITH (fillfactor = 100) WHERE "uri" IS NOT NULL;
CREATE INDEX idx_09492a43__ordered_uri_temp ON universal_transaction_matview_temp USING BTREE(UPPER("uri") DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_09492a43__gin_uri_temp ON universal_transaction_matview_temp USING GIN(UPPER("uri") gin_trgm_ops);
CREATE INDEX idx_09492a43__pop_country_code_temp ON universal_transaction_matview_temp USING BTREE("pop_country_code") WITH (fillfactor = 100) WHERE "pop_country_code" IS NOT NULL;
CREATE INDEX idx_09492a43__pop_state_code_temp ON universal_transaction_matview_temp USING BTREE("pop_state_code") WITH (fillfactor = 100) WHERE "pop_state_code" IS NOT NULL;
CREATE INDEX idx_09492a43__pop_county_code_temp ON universal_transaction_matview_temp USING BTREE("pop_county_code") WITH (fillfactor = 100) WHERE "pop_county_code" IS NOT NULL;
CREATE INDEX idx_09492a43__pop_zip5_temp ON universal_transaction_matview_temp USING BTREE("pop_zip5") WITH (fillfactor = 100) WHERE "pop_zip5" IS NOT NULL;
CREATE INDEX idx_09492a43__pop_congressional_code_temp ON universal_transaction_matview_temp USING BTREE("pop_congressional_code") WITH (fillfactor = 100) WHERE "pop_congressional_code" IS NOT NULL;
CREATE INDEX idx_09492a43__latest_temp ON universal_transaction_matview_temp USING BTREE("latest_transaction_id") WITH (fillfactor = 100);
CREATE INDEX idx_09492a43__gin_recipient_name_temp ON universal_transaction_matview_temp USING GIN("recipient_name" gin_trgm_ops);
CREATE INDEX idx_09492a43__gin_recipient_unique_id_temp ON universal_transaction_matview_temp USING GIN("recipient_unique_id" gin_trgm_ops);
CREATE INDEX idx_09492a43__gin_parent_recipient_unique_id_temp ON universal_transaction_matview_temp USING GIN("parent_recipient_unique_id" gin_trgm_ops);
CREATE INDEX idx_09492a43__recipient_id_temp ON universal_transaction_matview_temp USING BTREE("recipient_id") WITH (fillfactor = 100) WHERE "recipient_id" IS NOT NULL;
CREATE INDEX idx_09492a43__recipient_name_temp ON universal_transaction_matview_temp USING BTREE("recipient_name") WITH (fillfactor = 100) WHERE "recipient_name" IS NOT NULL;
CREATE INDEX idx_09492a43__recipient_unique_id_temp ON universal_transaction_matview_temp USING BTREE("recipient_unique_id") WITH (fillfactor = 100) WHERE "recipient_unique_id" IS NOT NULL;
CREATE INDEX idx_09492a43__parent_recipient_unique_id_temp ON universal_transaction_matview_temp USING BTREE("parent_recipient_unique_id") WITH (fillfactor = 100) WHERE "parent_recipient_unique_id" IS NOT NULL;
CREATE INDEX idx_09492a43__awarding_toptier_agency_name_temp ON universal_transaction_matview_temp USING BTREE("awarding_toptier_agency_name" DESC NULLS LAST) WITH (fillfactor = 100) WHERE awarding_toptier_agency_name IS NOT NULL;
CREATE INDEX idx_09492a43__awarding_subtier_agency_name_temp ON universal_transaction_matview_temp USING BTREE("awarding_subtier_agency_name" DESC NULLS LAST) WITH (fillfactor = 100) WHERE awarding_subtier_agency_name IS NOT NULL;
CREATE INDEX idx_09492a43__funding_toptier_agency_name_temp ON universal_transaction_matview_temp USING BTREE("funding_toptier_agency_name" DESC NULLS LAST) WITH (fillfactor = 100) WHERE funding_toptier_agency_name IS NOT NULL;
CREATE INDEX idx_09492a43__funding_subtier_agency_name_temp ON universal_transaction_matview_temp USING BTREE("funding_subtier_agency_name" DESC NULLS LAST) WITH (fillfactor = 100) WHERE funding_subtier_agency_name IS NOT NULL;
CREATE INDEX idx_09492a43__awarding_toptier_agency_abbreviation_temp ON universal_transaction_matview_temp USING BTREE("awarding_toptier_agency_abbreviation" DESC NULLS LAST) WITH (fillfactor = 100) WHERE awarding_toptier_agency_abbreviation IS NOT NULL;
CREATE INDEX idx_09492a43__awarding_subtier_agency_abbreviation_temp ON universal_transaction_matview_temp USING BTREE("awarding_subtier_agency_abbreviation" DESC NULLS LAST) WITH (fillfactor = 100) WHERE awarding_subtier_agency_abbreviation IS NOT NULL;
CREATE INDEX idx_09492a43__funding_toptier_agency_abbreviation_temp ON universal_transaction_matview_temp USING BTREE("funding_toptier_agency_abbreviation" DESC NULLS LAST) WITH (fillfactor = 100) WHERE funding_toptier_agency_abbreviation IS NOT NULL;
CREATE INDEX idx_09492a43__funding_subtier_agency_abbreviation_temp ON universal_transaction_matview_temp USING BTREE("funding_subtier_agency_abbreviation" DESC NULLS LAST) WITH (fillfactor = 100) WHERE funding_subtier_agency_abbreviation IS NOT NULL;
CREATE INDEX idx_09492a43__gin_awarding_toptier_agency_name_temp ON universal_transaction_matview_temp USING GIN("awarding_toptier_agency_name" gin_trgm_ops);
CREATE INDEX idx_09492a43__gin_awarding_subtier_agency_name_temp ON universal_transaction_matview_temp USING GIN("awarding_subtier_agency_name" gin_trgm_ops);
CREATE INDEX idx_09492a43__gin_funding_toptier_agency_name_temp ON universal_transaction_matview_temp USING GIN("funding_toptier_agency_name" gin_trgm_ops);
CREATE INDEX idx_09492a43__gin_funding_subtier_agency_name_temp ON universal_transaction_matview_temp USING GIN("funding_subtier_agency_name" gin_trgm_ops);
CREATE INDEX idx_09492a43__recipient_location_country_code_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_country_code") WITH (fillfactor = 100) WHERE "recipient_location_country_code" IS NOT NULL;
CREATE INDEX idx_09492a43__recipient_location_state_code_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_state_code") WITH (fillfactor = 100) WHERE "recipient_location_state_code" IS NOT NULL;
CREATE INDEX idx_09492a43__recipient_location_county_code_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_county_code") WITH (fillfactor = 100) WHERE "recipient_location_county_code" IS NOT NULL;
CREATE INDEX idx_09492a43__recipient_location_zip5_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_zip5") WITH (fillfactor = 100) WHERE "recipient_location_zip5" IS NOT NULL;
CREATE INDEX idx_09492a43__recipient_location_congressional_code_temp ON universal_transaction_matview_temp USING BTREE("recipient_location_congressional_code") WITH (fillfactor = 100) WHERE "recipient_location_congressional_code" IS NOT NULL;
CREATE INDEX idx_09492a43__cfda_multi_temp ON universal_transaction_matview_temp USING BTREE("cfda_number", "cfda_title") WITH (fillfactor = 100) WHERE "cfda_number" IS NOT NULL;
CREATE INDEX idx_09492a43__pulled_from_temp ON universal_transaction_matview_temp USING BTREE("pulled_from") WITH (fillfactor = 100) WHERE "pulled_from" IS NOT NULL;
CREATE INDEX idx_09492a43__type_of_contract_pricing_temp ON universal_transaction_matview_temp USING BTREE("type_of_contract_pricing") WITH (fillfactor = 100) WHERE "type_of_contract_pricing" IS NOT NULL;
CREATE INDEX idx_09492a43__extent_competed_temp ON universal_transaction_matview_temp USING BTREE("extent_competed") WITH (fillfactor = 100) WHERE "extent_competed" IS NOT NULL;
CREATE INDEX idx_09492a43__type_set_aside_temp ON universal_transaction_matview_temp USING BTREE("type_set_aside") WITH (fillfactor = 100) WHERE "type_set_aside" IS NOT NULL;
CREATE INDEX idx_09492a43__psc_temp ON universal_transaction_matview_temp USING BTREE("psc_code") WITH (fillfactor = 100) WHERE "psc_code" IS NOT NULL;
CREATE INDEX idx_09492a43__psd_temp ON universal_transaction_matview_temp USING BTREE("psc_description") WITH (fillfactor = 100) WHERE "psc_description" IS NOT NULL;
CREATE INDEX idx_09492a43__product_or_service_code_temp ON universal_transaction_matview_temp USING BTREE("product_or_service_code") WITH (fillfactor = 100) WHERE "product_or_service_code" IS NOT NULL;
CREATE INDEX idx_09492a43__product_or_service_description_temp ON universal_transaction_matview_temp USING BTREE("product_or_service_description") WITH (fillfactor = 100) WHERE "product_or_service_description" IS NOT NULL;
CREATE INDEX idx_09492a43__gin_product_or_service_description_temp ON universal_transaction_matview_temp USING GIN(("product_or_service_description") gin_trgm_ops);
CREATE INDEX idx_09492a43__gin_naics_code_temp ON universal_transaction_matview_temp USING GIN("naics_code" gin_trgm_ops);
CREATE INDEX idx_09492a43__naics_code_temp ON universal_transaction_matview_temp USING BTREE("naics_code") WITH (fillfactor = 100) WHERE "naics_code" IS NOT NULL;
CREATE INDEX idx_09492a43__naics_description_temp ON universal_transaction_matview_temp USING GIN(UPPER("naics_description") gin_trgm_ops);
CREATE INDEX idx_09492a43__face_value_loan_guarantee_temp ON universal_transaction_matview_temp USING BTREE("face_value_loan_guarantee") WITH (fillfactor = 100) WHERE "face_value_loan_guarantee" IS NOT NULL;
CREATE INDEX idx_09492a43__original_loan_subsidy_cost_temp ON universal_transaction_matview_temp USING BTREE("original_loan_subsidy_cost") WITH (fillfactor = 100) WHERE "original_loan_subsidy_cost" IS NOT NULL;
CREATE INDEX idx_09492a43__business_categories_temp ON universal_transaction_matview_temp USING GIN("business_categories");

CLUSTER VERBOSE universal_transaction_matview_temp USING idx_09492a43__action_date_temp;

VACUUM ANALYZE VERBOSE universal_transaction_matview_temp;

ALTER MATERIALIZED VIEW IF EXISTS universal_transaction_matview RENAME TO universal_transaction_matview_old;
ALTER INDEX IF EXISTS idx_09492a43__transaction_id RENAME TO idx_09492a43__transaction_id_old;
ALTER INDEX IF EXISTS idx_09492a43__action_date RENAME TO idx_09492a43__action_date_old;
ALTER INDEX IF EXISTS idx_09492a43__fiscal_year RENAME TO idx_09492a43__fiscal_year_old;
ALTER INDEX IF EXISTS idx_09492a43__type RENAME TO idx_09492a43__type_old;
ALTER INDEX IF EXISTS idx_09492a43__ordered_type RENAME TO idx_09492a43__ordered_type_old;
ALTER INDEX IF EXISTS idx_09492a43__action_type RENAME TO idx_09492a43__action_type_old;
ALTER INDEX IF EXISTS idx_09492a43__award_id RENAME TO idx_09492a43__award_id_old;
ALTER INDEX IF EXISTS idx_09492a43__award_category RENAME TO idx_09492a43__award_category_old;
ALTER INDEX IF EXISTS idx_09492a43__total_obligation RENAME TO idx_09492a43__total_obligation_old;
ALTER INDEX IF EXISTS idx_09492a43__ordered_total_obligation RENAME TO idx_09492a43__ordered_total_obligation_old;
ALTER INDEX IF EXISTS idx_09492a43__total_obl_bin RENAME TO idx_09492a43__total_obl_bin_old;
ALTER INDEX IF EXISTS idx_09492a43__fain RENAME TO idx_09492a43__fain_old;
ALTER INDEX IF EXISTS idx_09492a43__ordered_fain RENAME TO idx_09492a43__ordered_fain_old;
ALTER INDEX IF EXISTS idx_09492a43__gin_fain RENAME TO idx_09492a43__gin_fain_old;
ALTER INDEX IF EXISTS idx_09492a43__piid RENAME TO idx_09492a43__piid_old;
ALTER INDEX IF EXISTS idx_09492a43__ordered_piid RENAME TO idx_09492a43__ordered_piid_old;
ALTER INDEX IF EXISTS idx_09492a43__gin_pid RENAME TO idx_09492a43__gin_pid_old;
ALTER INDEX IF EXISTS idx_09492a43__uri RENAME TO idx_09492a43__uri_old;
ALTER INDEX IF EXISTS idx_09492a43__ordered_uri RENAME TO idx_09492a43__ordered_uri_old;
ALTER INDEX IF EXISTS idx_09492a43__gin_uri RENAME TO idx_09492a43__gin_uri_old;
ALTER INDEX IF EXISTS idx_09492a43__pop_country_code RENAME TO idx_09492a43__pop_country_code_old;
ALTER INDEX IF EXISTS idx_09492a43__pop_state_code RENAME TO idx_09492a43__pop_state_code_old;
ALTER INDEX IF EXISTS idx_09492a43__pop_county_code RENAME TO idx_09492a43__pop_county_code_old;
ALTER INDEX IF EXISTS idx_09492a43__pop_zip5 RENAME TO idx_09492a43__pop_zip5_old;
ALTER INDEX IF EXISTS idx_09492a43__pop_congressional_code RENAME TO idx_09492a43__pop_congressional_code_old;
ALTER INDEX IF EXISTS idx_09492a43__latest RENAME TO idx_09492a43__latest_old;
ALTER INDEX IF EXISTS idx_09492a43__gin_recipient_name RENAME TO idx_09492a43__gin_recipient_name_old;
ALTER INDEX IF EXISTS idx_09492a43__gin_recipient_unique_id RENAME TO idx_09492a43__gin_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_09492a43__gin_parent_recipient_unique_id RENAME TO idx_09492a43__gin_parent_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_09492a43__recipient_id RENAME TO idx_09492a43__recipient_id_old;
ALTER INDEX IF EXISTS idx_09492a43__recipient_name RENAME TO idx_09492a43__recipient_name_old;
ALTER INDEX IF EXISTS idx_09492a43__recipient_unique_id RENAME TO idx_09492a43__recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_09492a43__parent_recipient_unique_id RENAME TO idx_09492a43__parent_recipient_unique_id_old;
ALTER INDEX IF EXISTS idx_09492a43__awarding_toptier_agency_name RENAME TO idx_09492a43__awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_09492a43__awarding_subtier_agency_name RENAME TO idx_09492a43__awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_09492a43__funding_toptier_agency_name RENAME TO idx_09492a43__funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_09492a43__funding_subtier_agency_name RENAME TO idx_09492a43__funding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_09492a43__awarding_toptier_agency_abbreviation RENAME TO idx_09492a43__awarding_toptier_agency_abbreviation_old;
ALTER INDEX IF EXISTS idx_09492a43__awarding_subtier_agency_abbreviation RENAME TO idx_09492a43__awarding_subtier_agency_abbreviation_old;
ALTER INDEX IF EXISTS idx_09492a43__funding_toptier_agency_abbreviation RENAME TO idx_09492a43__funding_toptier_agency_abbreviation_old;
ALTER INDEX IF EXISTS idx_09492a43__funding_subtier_agency_abbreviation RENAME TO idx_09492a43__funding_subtier_agency_abbreviation_old;
ALTER INDEX IF EXISTS idx_09492a43__gin_awarding_toptier_agency_name RENAME TO idx_09492a43__gin_awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_09492a43__gin_awarding_subtier_agency_name RENAME TO idx_09492a43__gin_awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_09492a43__gin_funding_toptier_agency_name RENAME TO idx_09492a43__gin_funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_09492a43__gin_funding_subtier_agency_name RENAME TO idx_09492a43__gin_funding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_09492a43__recipient_location_country_code RENAME TO idx_09492a43__recipient_location_country_code_old;
ALTER INDEX IF EXISTS idx_09492a43__recipient_location_state_code RENAME TO idx_09492a43__recipient_location_state_code_old;
ALTER INDEX IF EXISTS idx_09492a43__recipient_location_county_code RENAME TO idx_09492a43__recipient_location_county_code_old;
ALTER INDEX IF EXISTS idx_09492a43__recipient_location_zip5 RENAME TO idx_09492a43__recipient_location_zip5_old;
ALTER INDEX IF EXISTS idx_09492a43__recipient_location_congressional_code RENAME TO idx_09492a43__recipient_location_congressional_code_old;
ALTER INDEX IF EXISTS idx_09492a43__cfda_multi RENAME TO idx_09492a43__cfda_multi_old;
ALTER INDEX IF EXISTS idx_09492a43__pulled_from RENAME TO idx_09492a43__pulled_from_old;
ALTER INDEX IF EXISTS idx_09492a43__type_of_contract_pricing RENAME TO idx_09492a43__type_of_contract_pricing_old;
ALTER INDEX IF EXISTS idx_09492a43__extent_competed RENAME TO idx_09492a43__extent_competed_old;
ALTER INDEX IF EXISTS idx_09492a43__type_set_aside RENAME TO idx_09492a43__type_set_aside_old;
ALTER INDEX IF EXISTS idx_09492a43__psc RENAME TO idx_09492a43__psc_old;
ALTER INDEX IF EXISTS idx_09492a43__psd RENAME TO idx_09492a43__psd_old;
ALTER INDEX IF EXISTS idx_09492a43__product_or_service_code RENAME TO idx_09492a43__product_or_service_code_old;
ALTER INDEX IF EXISTS idx_09492a43__product_or_service_description RENAME TO idx_09492a43__product_or_service_description_old;
ALTER INDEX IF EXISTS idx_09492a43__gin_product_or_service_description RENAME TO idx_09492a43__gin_product_or_service_description_old;
ALTER INDEX IF EXISTS idx_09492a43__gin_naics_code RENAME TO idx_09492a43__gin_naics_code_old;
ALTER INDEX IF EXISTS idx_09492a43__naics_code RENAME TO idx_09492a43__naics_code_old;
ALTER INDEX IF EXISTS idx_09492a43__naics_description RENAME TO idx_09492a43__naics_description_old;
ALTER INDEX IF EXISTS idx_09492a43__face_value_loan_guarantee RENAME TO idx_09492a43__face_value_loan_guarantee_old;
ALTER INDEX IF EXISTS idx_09492a43__original_loan_subsidy_cost RENAME TO idx_09492a43__original_loan_subsidy_cost_old;
ALTER INDEX IF EXISTS idx_09492a43__business_categories RENAME TO idx_09492a43__business_categories_old;

ALTER MATERIALIZED VIEW universal_transaction_matview_temp RENAME TO universal_transaction_matview;
ALTER INDEX idx_09492a43__transaction_id_temp RENAME TO idx_09492a43__transaction_id;
ALTER INDEX idx_09492a43__action_date_temp RENAME TO idx_09492a43__action_date;
ALTER INDEX idx_09492a43__fiscal_year_temp RENAME TO idx_09492a43__fiscal_year;
ALTER INDEX idx_09492a43__type_temp RENAME TO idx_09492a43__type;
ALTER INDEX idx_09492a43__ordered_type_temp RENAME TO idx_09492a43__ordered_type;
ALTER INDEX idx_09492a43__action_type_temp RENAME TO idx_09492a43__action_type;
ALTER INDEX idx_09492a43__award_id_temp RENAME TO idx_09492a43__award_id;
ALTER INDEX idx_09492a43__award_category_temp RENAME TO idx_09492a43__award_category;
ALTER INDEX idx_09492a43__total_obligation_temp RENAME TO idx_09492a43__total_obligation;
ALTER INDEX idx_09492a43__ordered_total_obligation_temp RENAME TO idx_09492a43__ordered_total_obligation;
ALTER INDEX idx_09492a43__total_obl_bin_temp RENAME TO idx_09492a43__total_obl_bin;
ALTER INDEX idx_09492a43__fain_temp RENAME TO idx_09492a43__fain;
ALTER INDEX idx_09492a43__ordered_fain_temp RENAME TO idx_09492a43__ordered_fain;
ALTER INDEX idx_09492a43__gin_fain_temp RENAME TO idx_09492a43__gin_fain;
ALTER INDEX idx_09492a43__piid_temp RENAME TO idx_09492a43__piid;
ALTER INDEX idx_09492a43__ordered_piid_temp RENAME TO idx_09492a43__ordered_piid;
ALTER INDEX idx_09492a43__gin_pid_temp RENAME TO idx_09492a43__gin_pid;
ALTER INDEX idx_09492a43__uri_temp RENAME TO idx_09492a43__uri;
ALTER INDEX idx_09492a43__ordered_uri_temp RENAME TO idx_09492a43__ordered_uri;
ALTER INDEX idx_09492a43__gin_uri_temp RENAME TO idx_09492a43__gin_uri;
ALTER INDEX idx_09492a43__pop_country_code_temp RENAME TO idx_09492a43__pop_country_code;
ALTER INDEX idx_09492a43__pop_state_code_temp RENAME TO idx_09492a43__pop_state_code;
ALTER INDEX idx_09492a43__pop_county_code_temp RENAME TO idx_09492a43__pop_county_code;
ALTER INDEX idx_09492a43__pop_zip5_temp RENAME TO idx_09492a43__pop_zip5;
ALTER INDEX idx_09492a43__pop_congressional_code_temp RENAME TO idx_09492a43__pop_congressional_code;
ALTER INDEX idx_09492a43__latest_temp RENAME TO idx_09492a43__latest;
ALTER INDEX idx_09492a43__gin_recipient_name_temp RENAME TO idx_09492a43__gin_recipient_name;
ALTER INDEX idx_09492a43__gin_recipient_unique_id_temp RENAME TO idx_09492a43__gin_recipient_unique_id;
ALTER INDEX idx_09492a43__gin_parent_recipient_unique_id_temp RENAME TO idx_09492a43__gin_parent_recipient_unique_id;
ALTER INDEX idx_09492a43__recipient_id_temp RENAME TO idx_09492a43__recipient_id;
ALTER INDEX idx_09492a43__recipient_name_temp RENAME TO idx_09492a43__recipient_name;
ALTER INDEX idx_09492a43__recipient_unique_id_temp RENAME TO idx_09492a43__recipient_unique_id;
ALTER INDEX idx_09492a43__parent_recipient_unique_id_temp RENAME TO idx_09492a43__parent_recipient_unique_id;
ALTER INDEX idx_09492a43__awarding_toptier_agency_name_temp RENAME TO idx_09492a43__awarding_toptier_agency_name;
ALTER INDEX idx_09492a43__awarding_subtier_agency_name_temp RENAME TO idx_09492a43__awarding_subtier_agency_name;
ALTER INDEX idx_09492a43__funding_toptier_agency_name_temp RENAME TO idx_09492a43__funding_toptier_agency_name;
ALTER INDEX idx_09492a43__funding_subtier_agency_name_temp RENAME TO idx_09492a43__funding_subtier_agency_name;
ALTER INDEX idx_09492a43__awarding_toptier_agency_abbreviation_temp RENAME TO idx_09492a43__awarding_toptier_agency_abbreviation;
ALTER INDEX idx_09492a43__awarding_subtier_agency_abbreviation_temp RENAME TO idx_09492a43__awarding_subtier_agency_abbreviation;
ALTER INDEX idx_09492a43__funding_toptier_agency_abbreviation_temp RENAME TO idx_09492a43__funding_toptier_agency_abbreviation;
ALTER INDEX idx_09492a43__funding_subtier_agency_abbreviation_temp RENAME TO idx_09492a43__funding_subtier_agency_abbreviation;
ALTER INDEX idx_09492a43__gin_awarding_toptier_agency_name_temp RENAME TO idx_09492a43__gin_awarding_toptier_agency_name;
ALTER INDEX idx_09492a43__gin_awarding_subtier_agency_name_temp RENAME TO idx_09492a43__gin_awarding_subtier_agency_name;
ALTER INDEX idx_09492a43__gin_funding_toptier_agency_name_temp RENAME TO idx_09492a43__gin_funding_toptier_agency_name;
ALTER INDEX idx_09492a43__gin_funding_subtier_agency_name_temp RENAME TO idx_09492a43__gin_funding_subtier_agency_name;
ALTER INDEX idx_09492a43__recipient_location_country_code_temp RENAME TO idx_09492a43__recipient_location_country_code;
ALTER INDEX idx_09492a43__recipient_location_state_code_temp RENAME TO idx_09492a43__recipient_location_state_code;
ALTER INDEX idx_09492a43__recipient_location_county_code_temp RENAME TO idx_09492a43__recipient_location_county_code;
ALTER INDEX idx_09492a43__recipient_location_zip5_temp RENAME TO idx_09492a43__recipient_location_zip5;
ALTER INDEX idx_09492a43__recipient_location_congressional_code_temp RENAME TO idx_09492a43__recipient_location_congressional_code;
ALTER INDEX idx_09492a43__cfda_multi_temp RENAME TO idx_09492a43__cfda_multi;
ALTER INDEX idx_09492a43__pulled_from_temp RENAME TO idx_09492a43__pulled_from;
ALTER INDEX idx_09492a43__type_of_contract_pricing_temp RENAME TO idx_09492a43__type_of_contract_pricing;
ALTER INDEX idx_09492a43__extent_competed_temp RENAME TO idx_09492a43__extent_competed;
ALTER INDEX idx_09492a43__type_set_aside_temp RENAME TO idx_09492a43__type_set_aside;
ALTER INDEX idx_09492a43__psc_temp RENAME TO idx_09492a43__psc;
ALTER INDEX idx_09492a43__psd_temp RENAME TO idx_09492a43__psd;
ALTER INDEX idx_09492a43__product_or_service_code_temp RENAME TO idx_09492a43__product_or_service_code;
ALTER INDEX idx_09492a43__product_or_service_description_temp RENAME TO idx_09492a43__product_or_service_description;
ALTER INDEX idx_09492a43__gin_product_or_service_description_temp RENAME TO idx_09492a43__gin_product_or_service_description;
ALTER INDEX idx_09492a43__gin_naics_code_temp RENAME TO idx_09492a43__gin_naics_code;
ALTER INDEX idx_09492a43__naics_code_temp RENAME TO idx_09492a43__naics_code;
ALTER INDEX idx_09492a43__naics_description_temp RENAME TO idx_09492a43__naics_description;
ALTER INDEX idx_09492a43__face_value_loan_guarantee_temp RENAME TO idx_09492a43__face_value_loan_guarantee;
ALTER INDEX idx_09492a43__original_loan_subsidy_cost_temp RENAME TO idx_09492a43__original_loan_subsidy_cost;
ALTER INDEX idx_09492a43__business_categories_temp RENAME TO idx_09492a43__business_categories;
