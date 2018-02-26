--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
CREATE MATERIALIZED VIEW universal_award_matview_temp AS
SELECT
  to_tsvector(CONCAT(
    recipient."recipient_name",
    ' ', contract_data."naics",
    ' ', contract_data."naics_description",
    ' ', "psc"."description",
    ' ', "awards"."description")) AS keyword_string,
  to_tsvector(CONCAT(awards.piid, ' ', awards.fain, ' ', awards.uri)) AS award_id_string,
  to_tsvector(coalesce(recipient."recipient_name", '')) AS recipient_name_ts_vector,

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
