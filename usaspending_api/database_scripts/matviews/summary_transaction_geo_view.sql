--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS summary_transaction_geo_view_temp CASCADE;
DROP MATERIALIZED VIEW IF EXISTS summary_transaction_geo_view_old CASCADE;

CREATE MATERIALIZED VIEW summary_transaction_geo_view_temp AS
SELECT
  MD5(array_to_string(sort(array_agg("transaction_normalized"."id"::int)), ' ')) AS pk,
  cast(date_trunc('month', "transaction_normalized"."action_date") as date) as "action_date",
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

  "transaction_normalized"."awarding_agency_id",
  "transaction_normalized"."funding_agency_id",
  TAA."name" AS awarding_toptier_agency_name,
  TFA."name" AS funding_toptier_agency_name,
  SAA."name" AS awarding_subtier_agency_name,
  SFA."name" AS funding_subtier_agency_name,
  TAA."abbreviation" AS awarding_toptier_agency_abbreviation,
  TFA."abbreviation" AS funding_toptier_agency_abbreviation,
  SAA."abbreviation" AS awarding_subtier_agency_abbreviation,
  SFA."abbreviation" AS funding_subtier_agency_abbreviation,

  SUM(COALESCE("transaction_normalized"."federal_action_obligation", 0))::NUMERIC(20, 2) AS "federal_action_obligation",
  SUM(COALESCE("transaction_normalized"."original_loan_subsidy_cost", 0))::NUMERIC(20, 2) AS "original_loan_subsidy_cost",
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
WHERE
  "transaction_normalized"."action_date" >= '2007-10-01'
GROUP BY
  cast(date_trunc('month', "transaction_normalized"."action_date") as date),
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

  "transaction_normalized"."awarding_agency_id",
  "transaction_normalized"."funding_agency_id",
  TAA."name",
  TFA."name",
  SAA."name",
  SFA."name",
  TAA."abbreviation",
  TFA."abbreviation",
  SAA."abbreviation",
  SFA."abbreviation"
ORDER BY
  cast(date_trunc('month', "transaction_normalized"."action_date") as date) DESC;

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

ALTER MATERIALIZED VIEW IF EXISTS summary_transaction_geo_view RENAME TO summary_transaction_geo_view_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_unique_pk RENAME TO idx_af8ca7ca$0d1_unique_pk_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_date RENAME TO idx_af8ca7ca$0d1_date_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_fy RENAME TO idx_af8ca7ca$0d1_fy_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_fy_type RENAME TO idx_af8ca7ca$0d1_fy_type_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_type RENAME TO idx_af8ca7ca$0d1_type_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_pulled_from RENAME TO idx_af8ca7ca$0d1_pulled_from_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_recipient_country_code RENAME TO idx_af8ca7ca$0d1_recipient_country_code_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_recipient_state_code RENAME TO idx_af8ca7ca$0d1_recipient_state_code_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_recipient_county_code RENAME TO idx_af8ca7ca$0d1_recipient_county_code_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_recipient_zip RENAME TO idx_af8ca7ca$0d1_recipient_zip_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_pop_country_code RENAME TO idx_af8ca7ca$0d1_pop_country_code_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_pop_state_code RENAME TO idx_af8ca7ca$0d1_pop_state_code_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_pop_county_code RENAME TO idx_af8ca7ca$0d1_pop_county_code_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_pop_zip RENAME TO idx_af8ca7ca$0d1_pop_zip_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_awarding_agency_id RENAME TO idx_af8ca7ca$0d1_awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_funding_agency_id RENAME TO idx_af8ca7ca$0d1_funding_agency_id_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_awarding_toptier_agency_name RENAME TO idx_af8ca7ca$0d1_awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_awarding_subtier_agency_name RENAME TO idx_af8ca7ca$0d1_awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_funding_toptier_agency_name RENAME TO idx_af8ca7ca$0d1_funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_funding_subtier_agency_name RENAME TO idx_af8ca7ca$0d1_funding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_compound_geo_pop_1 RENAME TO idx_af8ca7ca$0d1_compound_geo_pop_1_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_compound_geo_pop_2 RENAME TO idx_af8ca7ca$0d1_compound_geo_pop_2_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_compound_geo_pop_3 RENAME TO idx_af8ca7ca$0d1_compound_geo_pop_3_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_compound_geo_rl_1 RENAME TO idx_af8ca7ca$0d1_compound_geo_rl_1_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_compound_geo_rl_2 RENAME TO idx_af8ca7ca$0d1_compound_geo_rl_2_old;
ALTER INDEX IF EXISTS idx_af8ca7ca$0d1_compound_geo_rl_3 RENAME TO idx_af8ca7ca$0d1_compound_geo_rl_3_old;

ALTER MATERIALIZED VIEW summary_transaction_geo_view_temp RENAME TO summary_transaction_geo_view;
ALTER INDEX idx_af8ca7ca$0d1_unique_pk_temp RENAME TO idx_af8ca7ca$0d1_unique_pk;
ALTER INDEX idx_af8ca7ca$0d1_date_temp RENAME TO idx_af8ca7ca$0d1_date;
ALTER INDEX idx_af8ca7ca$0d1_fy_temp RENAME TO idx_af8ca7ca$0d1_fy;
ALTER INDEX idx_af8ca7ca$0d1_fy_type_temp RENAME TO idx_af8ca7ca$0d1_fy_type;
ALTER INDEX idx_af8ca7ca$0d1_type_temp RENAME TO idx_af8ca7ca$0d1_type;
ALTER INDEX idx_af8ca7ca$0d1_pulled_from_temp RENAME TO idx_af8ca7ca$0d1_pulled_from;
ALTER INDEX idx_af8ca7ca$0d1_recipient_country_code_temp RENAME TO idx_af8ca7ca$0d1_recipient_country_code;
ALTER INDEX idx_af8ca7ca$0d1_recipient_state_code_temp RENAME TO idx_af8ca7ca$0d1_recipient_state_code;
ALTER INDEX idx_af8ca7ca$0d1_recipient_county_code_temp RENAME TO idx_af8ca7ca$0d1_recipient_county_code;
ALTER INDEX idx_af8ca7ca$0d1_recipient_zip_temp RENAME TO idx_af8ca7ca$0d1_recipient_zip;
ALTER INDEX idx_af8ca7ca$0d1_pop_country_code_temp RENAME TO idx_af8ca7ca$0d1_pop_country_code;
ALTER INDEX idx_af8ca7ca$0d1_pop_state_code_temp RENAME TO idx_af8ca7ca$0d1_pop_state_code;
ALTER INDEX idx_af8ca7ca$0d1_pop_county_code_temp RENAME TO idx_af8ca7ca$0d1_pop_county_code;
ALTER INDEX idx_af8ca7ca$0d1_pop_zip_temp RENAME TO idx_af8ca7ca$0d1_pop_zip;
ALTER INDEX idx_af8ca7ca$0d1_awarding_agency_id_temp RENAME TO idx_af8ca7ca$0d1_awarding_agency_id;
ALTER INDEX idx_af8ca7ca$0d1_funding_agency_id_temp RENAME TO idx_af8ca7ca$0d1_funding_agency_id;
ALTER INDEX idx_af8ca7ca$0d1_awarding_toptier_agency_name_temp RENAME TO idx_af8ca7ca$0d1_awarding_toptier_agency_name;
ALTER INDEX idx_af8ca7ca$0d1_awarding_subtier_agency_name_temp RENAME TO idx_af8ca7ca$0d1_awarding_subtier_agency_name;
ALTER INDEX idx_af8ca7ca$0d1_funding_toptier_agency_name_temp RENAME TO idx_af8ca7ca$0d1_funding_toptier_agency_name;
ALTER INDEX idx_af8ca7ca$0d1_funding_subtier_agency_name_temp RENAME TO idx_af8ca7ca$0d1_funding_subtier_agency_name;
ALTER INDEX idx_af8ca7ca$0d1_compound_geo_pop_1_temp RENAME TO idx_af8ca7ca$0d1_compound_geo_pop_1;
ALTER INDEX idx_af8ca7ca$0d1_compound_geo_pop_2_temp RENAME TO idx_af8ca7ca$0d1_compound_geo_pop_2;
ALTER INDEX idx_af8ca7ca$0d1_compound_geo_pop_3_temp RENAME TO idx_af8ca7ca$0d1_compound_geo_pop_3;
ALTER INDEX idx_af8ca7ca$0d1_compound_geo_rl_1_temp RENAME TO idx_af8ca7ca$0d1_compound_geo_rl_1;
ALTER INDEX idx_af8ca7ca$0d1_compound_geo_rl_2_temp RENAME TO idx_af8ca7ca$0d1_compound_geo_rl_2;
ALTER INDEX idx_af8ca7ca$0d1_compound_geo_rl_3_temp RENAME TO idx_af8ca7ca$0d1_compound_geo_rl_3;

ANALYZE VERBOSE summary_transaction_geo_view;
GRANT SELECT ON summary_transaction_geo_view TO readonly;
