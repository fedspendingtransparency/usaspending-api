--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS summary_transaction_view_temp;
DROP MATERIALIZED VIEW IF EXISTS summary_transaction_view_old;

CREATE MATERIALIZED VIEW summary_transaction_view_temp AS
SELECT
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "transaction_normalized"."type",
  "transaction_fpds"."pulled_from",

  recipient_location."location_country_code" AS "recipient_location_country_code",
  recipient_location."country_name" AS "recipient_location_country_name",
  recipient_location."state_code" AS "recipient_location_state_code",
  recipient_location."county_code" AS "recipient_location_county_code",
  recipient_location."county_name" AS "recipient_location_county_name",
  recipient_location."congressional_code" AS "recipient_location_congressional_code",
  recipient_location."zip5" AS "recipient_location_zip5",
  place_of_performance."location_country_code" AS "pop_country_code",
  place_of_performance."country_name" AS "pop_country_name",
  place_of_performance."state_code" AS "pop_state_code",
  place_of_performance."county_code" AS "pop_county_code",
  place_of_performance."county_name" AS "pop_county_name",
  place_of_performance."congressional_code" AS "pop_congressional_code",
  place_of_performance."zip5" AS "pop_zip5",

  TAA."name" AS "awarding_toptier_agency_name",
  TAA."abbreviation" AS "awarding_toptier_agency_abbreviation",
  TFA."name" AS "funding_toptier_agency_name",
  TFA."abbreviation" AS "funding_toptier_agency_abbreviation",
  SAA."name" AS awarding_subtier_agency_name,
  SAA."abbreviation" AS awarding_subtier_agency_abbreviation,
  SFA."name" AS funding_subtier_agency_name,
  SFA."abbreviation" AS funding_subtier_agency_abbreviation,

  "legal_entity"."business_categories",
  "transaction_fabs"."cfda_number",
  "references_cfda"."program_title" AS "cfda_title",
  "references_cfda"."popular_name" AS "cfda_popular_name",
  "transaction_fpds"."product_or_service_code",
  "psc"."description" AS product_or_service_description,
  "transaction_fpds"."naics" AS "naics_code",
  "naics"."description" AS "naics_description",
  obligation_to_enum("awards"."total_obligation") AS "total_obl_bin",
  "transaction_fpds"."type_of_contract_pricing",
  "transaction_fpds"."type_set_aside",
  "transaction_fpds"."extent_competed",
  SUM("transaction_normalized"."federal_action_obligation") AS "federal_action_obligation",
  count(*) AS counts
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
  "transaction_normalized"."federal_action_obligation" IS NOT NULL
GROUP BY
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "transaction_normalized"."type",
  "transaction_fpds"."pulled_from",
  recipient_location."location_country_code",
  recipient_location."country_name",
  recipient_location."state_code",
  recipient_location."county_code",
  recipient_location."county_name",
  recipient_location."congressional_code",
  recipient_location."zip5",
  place_of_performance."location_country_code",
  place_of_performance."country_name",
  place_of_performance."state_code",
  place_of_performance."county_code",
  place_of_performance."county_name",
  place_of_performance."congressional_code",
  place_of_performance."zip5",

  TAA."name",
  TAA."abbreviation",
  TFA."name",
  TFA."abbreviation",
  SAA."name",
  SAA."abbreviation",
  SFA."name",
  SFA."abbreviation",

  "legal_entity"."business_categories",
  "transaction_fabs"."cfda_number",
  "references_cfda"."program_title",
  "references_cfda"."popular_name",
  "transaction_fpds"."product_or_service_code",
  "psc"."description",
  "transaction_fpds"."naics",
  "naics"."description",
  obligation_to_enum("awards"."total_obligation"),
  "transaction_fpds"."type_of_contract_pricing",
  "transaction_fpds"."type_set_aside",
  "transaction_fpds"."extent_competed";

CREATE INDEX idx_de4a5a13__ordered_action_date_temp ON summary_transaction_view_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__ordered_fy_temp ON summary_transaction_view_temp USING BTREE("fiscal_year" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__ordered_fy_type_temp ON summary_transaction_view_temp USING BTREE("fiscal_year" DESC NULLS LAST, "type") WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__type_temp ON summary_transaction_view_temp USING BTREE("type") WITH (fillfactor = 100) WHERE "type" IS NOT NULL;
CREATE INDEX idx_de4a5a13__pulled_from_temp ON summary_transaction_view_temp USING BTREE("pulled_from" DESC NULLS LAST) WITH (fillfactor = 100) WHERE "pulled_from" IS NOT NULL;
CREATE INDEX idx_de4a5a13__recipient_country_code_temp ON summary_transaction_view_temp USING BTREE("recipient_location_country_code") WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__recipient_state_code_temp ON summary_transaction_view_temp USING BTREE("recipient_location_state_code") WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__recipient_county_code_temp ON summary_transaction_view_temp USING BTREE("recipient_location_county_code") WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__recipient_zip_temp ON summary_transaction_view_temp USING BTREE("recipient_location_zip5") WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__pop_country_code_temp ON summary_transaction_view_temp USING BTREE("pop_country_code") WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__pop_state_code_temp ON summary_transaction_view_temp USING BTREE("pop_state_code") WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__pop_county_code_temp ON summary_transaction_view_temp USING BTREE("pop_county_code") WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__pop_zip_temp ON summary_transaction_view_temp USING BTREE("pop_zip5") WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__award_agency_name_temp ON summary_transaction_view_temp USING BTREE("awarding_toptier_agency_name") WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__cfda_num_temp ON summary_transaction_view_temp USING BTREE("cfda_number") WITH (fillfactor = 100) WHERE "cfda_number" IS NOT NULL;
CREATE INDEX idx_de4a5a13__cfda_title_temp ON summary_transaction_view_temp USING BTREE("cfda_title") WITH (fillfactor = 100) WHERE "cfda_title" IS NOT NULL;
CREATE INDEX idx_de4a5a13__psc2_temp ON summary_transaction_view_temp USING BTREE("psc_code") WITH (fillfactor = 100) WHERE "psc_code" IS NOT NULL;
CREATE INDEX idx_de4a5a13__psc_temp ON summary_transaction_view_temp USING BTREE("product_or_service_code") WITH (fillfactor = 100) WHERE "product_or_service_code" IS NOT NULL;
CREATE INDEX idx_de4a5a13__naics_temp ON summary_transaction_view_temp USING BTREE("naics_code") WITH (fillfactor = 100) WHERE "naics_code" IS NOT NULL;
CREATE INDEX idx_de4a5a13__total_obl_bin_temp ON summary_transaction_view_temp USING BTREE("total_obl_bin") WITH (fillfactor = 100) WHERE "total_obl_bin" IS NOT NULL;
CREATE INDEX idx_de4a5a13__type_of_contract_temp ON summary_transaction_view_temp USING BTREE("type_of_contract_pricing") WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__ordered_fy_set_aside_temp ON summary_transaction_view_temp USING BTREE("fiscal_year" DESC NULLS LAST, "type_set_aside") WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__extent_competed_temp ON summary_transaction_view_temp USING BTREE("extent_competed") WITH (fillfactor = 100);
CREATE INDEX idx_de4a5a13__type_set_aside_temp ON summary_transaction_view_temp USING BTREE("type_set_aside") WITH (fillfactor = 100) WHERE "type_set_aside" IS NOT NULL;

VACUUM ANALYZE VERBOSE summary_transaction_view_temp;

ALTER MATERIALIZED VIEW IF EXISTS summary_transaction_view RENAME TO summary_transaction_view_old;
ALTER INDEX IF EXISTS idx_de4a5a13__ordered_action_date RENAME TO idx_de4a5a13__ordered_action_date_old;
ALTER INDEX IF EXISTS idx_de4a5a13__ordered_fy RENAME TO idx_de4a5a13__ordered_fy_old;
ALTER INDEX IF EXISTS idx_de4a5a13__ordered_fy_type RENAME TO idx_de4a5a13__ordered_fy_type_old;
ALTER INDEX IF EXISTS idx_de4a5a13__type RENAME TO idx_de4a5a13__type_old;
ALTER INDEX IF EXISTS idx_de4a5a13__pulled_from RENAME TO idx_de4a5a13__pulled_from_old;
ALTER INDEX IF EXISTS idx_de4a5a13__recipient_country_code RENAME TO idx_de4a5a13__recipient_country_code_old;
ALTER INDEX IF EXISTS idx_de4a5a13__recipient_state_code RENAME TO idx_de4a5a13__recipient_state_code_old;
ALTER INDEX IF EXISTS idx_de4a5a13__recipient_county_code RENAME TO idx_de4a5a13__recipient_county_code_old;
ALTER INDEX IF EXISTS idx_de4a5a13__recipient_zip RENAME TO idx_de4a5a13__recipient_zip_old;
ALTER INDEX IF EXISTS idx_de4a5a13__pop_country_code RENAME TO idx_de4a5a13__pop_country_code_old;
ALTER INDEX IF EXISTS idx_de4a5a13__pop_state_code RENAME TO idx_de4a5a13__pop_state_code_old;
ALTER INDEX IF EXISTS idx_de4a5a13__pop_county_code RENAME TO idx_de4a5a13__pop_county_code_old;
ALTER INDEX IF EXISTS idx_de4a5a13__pop_zip RENAME TO idx_de4a5a13__pop_zip_old;
ALTER INDEX IF EXISTS idx_de4a5a13__award_agency_name RENAME TO idx_de4a5a13__award_agency_name_old;
ALTER INDEX IF EXISTS idx_de4a5a13__cfda_num RENAME TO idx_de4a5a13__cfda_num_old;
ALTER INDEX IF EXISTS idx_de4a5a13__cfda_title RENAME TO idx_de4a5a13__cfda_title_old;
ALTER INDEX IF EXISTS idx_de4a5a13__psc2 RENAME TO idx_de4a5a13__psc2_old;
ALTER INDEX IF EXISTS idx_de4a5a13__psc RENAME TO idx_de4a5a13__psc_old;
ALTER INDEX IF EXISTS idx_de4a5a13__naics RENAME TO idx_de4a5a13__naics_old;
ALTER INDEX IF EXISTS idx_de4a5a13__total_obl_bin RENAME TO idx_de4a5a13__total_obl_bin_old;
ALTER INDEX IF EXISTS idx_de4a5a13__type_of_contract RENAME TO idx_de4a5a13__type_of_contract_old;
ALTER INDEX IF EXISTS idx_de4a5a13__ordered_fy_set_aside RENAME TO idx_de4a5a13__ordered_fy_set_aside_old;
ALTER INDEX IF EXISTS idx_de4a5a13__extent_competed RENAME TO idx_de4a5a13__extent_competed_old;
ALTER INDEX IF EXISTS idx_de4a5a13__type_set_aside RENAME TO idx_de4a5a13__type_set_aside_old;

ALTER MATERIALIZED VIEW summary_transaction_view_temp RENAME TO summary_transaction_view;
ALTER INDEX idx_de4a5a13__ordered_action_date_temp RENAME TO idx_de4a5a13__ordered_action_date;
ALTER INDEX idx_de4a5a13__ordered_fy_temp RENAME TO idx_de4a5a13__ordered_fy;
ALTER INDEX idx_de4a5a13__ordered_fy_type_temp RENAME TO idx_de4a5a13__ordered_fy_type;
ALTER INDEX idx_de4a5a13__type_temp RENAME TO idx_de4a5a13__type;
ALTER INDEX idx_de4a5a13__pulled_from_temp RENAME TO idx_de4a5a13__pulled_from;
ALTER INDEX idx_de4a5a13__recipient_country_code_temp RENAME TO idx_de4a5a13__recipient_country_code;
ALTER INDEX idx_de4a5a13__recipient_state_code_temp RENAME TO idx_de4a5a13__recipient_state_code;
ALTER INDEX idx_de4a5a13__recipient_county_code_temp RENAME TO idx_de4a5a13__recipient_county_code;
ALTER INDEX idx_de4a5a13__recipient_zip_temp RENAME TO idx_de4a5a13__recipient_zip;
ALTER INDEX idx_de4a5a13__pop_country_code_temp RENAME TO idx_de4a5a13__pop_country_code;
ALTER INDEX idx_de4a5a13__pop_state_code_temp RENAME TO idx_de4a5a13__pop_state_code;
ALTER INDEX idx_de4a5a13__pop_county_code_temp RENAME TO idx_de4a5a13__pop_county_code;
ALTER INDEX idx_de4a5a13__pop_zip_temp RENAME TO idx_de4a5a13__pop_zip;
ALTER INDEX idx_de4a5a13__award_agency_name_temp RENAME TO idx_de4a5a13__award_agency_name;
ALTER INDEX idx_de4a5a13__cfda_num_temp RENAME TO idx_de4a5a13__cfda_num;
ALTER INDEX idx_de4a5a13__cfda_title_temp RENAME TO idx_de4a5a13__cfda_title;
ALTER INDEX idx_de4a5a13__psc2_temp RENAME TO idx_de4a5a13__psc2;
ALTER INDEX idx_de4a5a13__psc_temp RENAME TO idx_de4a5a13__psc;
ALTER INDEX idx_de4a5a13__naics_temp RENAME TO idx_de4a5a13__naics;
ALTER INDEX idx_de4a5a13__total_obl_bin_temp RENAME TO idx_de4a5a13__total_obl_bin;
ALTER INDEX idx_de4a5a13__type_of_contract_temp RENAME TO idx_de4a5a13__type_of_contract;
ALTER INDEX idx_de4a5a13__ordered_fy_set_aside_temp RENAME TO idx_de4a5a13__ordered_fy_set_aside;
ALTER INDEX idx_de4a5a13__extent_competed_temp RENAME TO idx_de4a5a13__extent_competed;
ALTER INDEX idx_de4a5a13__type_set_aside_temp RENAME TO idx_de4a5a13__type_set_aside;
