--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--         !!DO NOT DIRECTLY EDIT THIS FILE!!         --
--------------------------------------------------------
CREATE MATERIALIZED VIEW universal_transaction_matview_temp AS
SELECT
  to_tsvector(CONCAT(
    "legal_entity"."recipient_name",
    ' ', "transaction_fpds"."naics",
    ' ', "naics"."description",
    ' ', "psc"."description",
    ' ', "awards"."description")) AS keyword_ts_vector,
  to_tsvector(CONCAT(awards.piid, ' ', awards.fain, ' ', awards.uri)) AS award_ts_vector,
  to_tsvector(coalesce("legal_entity"."recipient_name", '')) AS recipient_name_ts_vector,

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
