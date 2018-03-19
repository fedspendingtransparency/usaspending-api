--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--         !!DO NOT DIRECTLY EDIT THIS FILE!!         --
--------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS summary_award_view_temp CASCADE;
DROP MATERIALIZED VIEW IF EXISTS summary_award_view_old CASCADE;

CREATE MATERIALIZED VIEW summary_award_view_temp AS
SELECT
  MD5(array_to_string(sort(array_agg("awards"."id"::int)), ' ')) AS pk,
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "awards"."type",
  "transaction_fpds"."pulled_from",
  "awards"."category",

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
  SUM(COALESCE("transaction_normalized"."original_loan_subsidy_cost", 0))::NUMERIC(23, 2) AS "original_loan_subsidy_cost",
  SUM(COALESCE("transaction_normalized"."face_value_loan_guarantee", 0))::NUMERIC(23, 2) AS "face_value_loan_guarantee",
  COUNT(*) counts
FROM
  "awards"
LEFT OUTER JOIN
  "transaction_normalized" ON ("awards"."latest_transaction_id" = "transaction_normalized"."id")
LEFT OUTER JOIN
  "transaction_fpds" ON ("awards"."latest_transaction_id" = "transaction_fpds"."transaction_id")
LEFT OUTER JOIN
  "agency" AS AA ON ("transaction_normalized"."awarding_agency_id" = AA."id")
LEFT OUTER JOIN
  "agency" AS FA ON ("transaction_normalized"."funding_agency_id" = FA."id")
LEFT OUTER JOIN
  "toptier_agency" AS TAA ON (AA."toptier_agency_id" = TAA."toptier_agency_id")
LEFT OUTER JOIN
  "toptier_agency" AS TFA ON (FA."toptier_agency_id" = TFA."toptier_agency_id")
LEFT OUTER JOIN
  "subtier_agency" AS SAA ON (AA."subtier_agency_id" = SAA."subtier_agency_id")
LEFT OUTER JOIN
  "subtier_agency" AS SFA ON (FA."subtier_agency_id" = SFA."subtier_agency_id")
WHERE
  "transaction_normalized".action_date >= '2007-10-01'
GROUP BY
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "awards"."type",
  "transaction_fpds"."pulled_from",
  "awards"."category",
  "transaction_normalized"."awarding_agency_id",
  "transaction_normalized"."funding_agency_id",
  TAA."name",
  TFA."name",
  SAA."name",
  SFA."name",
  TAA."abbreviation",
  TFA."abbreviation",
  SAA."abbreviation",
  SFA."abbreviation";

CREATE UNIQUE INDEX idx_c2817a5a$7aa_unique_pk_temp ON summary_award_view_temp USING BTREE("pk") WITH (fillfactor = 97);
CREATE INDEX idx_c2817a5a$7aa_action_date_temp ON summary_award_view_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_c2817a5a$7aa_type_temp ON summary_award_view_temp USING BTREE("type") WITH (fillfactor = 97);
CREATE INDEX idx_c2817a5a$7aa_fy_temp ON summary_award_view_temp USING BTREE("fiscal_year" DESC NULLS LAST) WITH (fillfactor = 97);
CREATE INDEX idx_c2817a5a$7aa_pulled_from_temp ON summary_award_view_temp USING BTREE("pulled_from") WITH (fillfactor = 97) WHERE "pulled_from" IS NOT NULL;
CREATE INDEX idx_c2817a5a$7aa_awarding_agency_id_temp ON summary_award_view_temp USING BTREE("awarding_agency_id" ASC NULLS LAST) WITH (fillfactor = 97) WHERE "awarding_agency_id" IS NOT NULL;
CREATE INDEX idx_c2817a5a$7aa_funding_agency_id_temp ON summary_award_view_temp USING BTREE("funding_agency_id" ASC NULLS LAST) WITH (fillfactor = 97) WHERE "funding_agency_id" IS NOT NULL;
CREATE INDEX idx_c2817a5a$7aa_awarding_toptier_agency_name_temp ON summary_award_view_temp USING BTREE("awarding_toptier_agency_name") WITH (fillfactor = 97) WHERE "awarding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_c2817a5a$7aa_awarding_subtier_agency_name_temp ON summary_award_view_temp USING BTREE("awarding_subtier_agency_name") WITH (fillfactor = 97) WHERE "awarding_subtier_agency_name" IS NOT NULL;
CREATE INDEX idx_c2817a5a$7aa_funding_toptier_agency_name_temp ON summary_award_view_temp USING BTREE("funding_toptier_agency_name") WITH (fillfactor = 97) WHERE "funding_toptier_agency_name" IS NOT NULL;
CREATE INDEX idx_c2817a5a$7aa_funding_subtier_agency_name_temp ON summary_award_view_temp USING BTREE("funding_subtier_agency_name") WITH (fillfactor = 97) WHERE "funding_subtier_agency_name" IS NOT NULL;

ALTER MATERIALIZED VIEW IF EXISTS summary_award_view RENAME TO summary_award_view_old;
ALTER INDEX IF EXISTS idx_c2817a5a$7aa_unique_pk RENAME TO idx_c2817a5a$7aa_unique_pk_old;
ALTER INDEX IF EXISTS idx_c2817a5a$7aa_action_date RENAME TO idx_c2817a5a$7aa_action_date_old;
ALTER INDEX IF EXISTS idx_c2817a5a$7aa_type RENAME TO idx_c2817a5a$7aa_type_old;
ALTER INDEX IF EXISTS idx_c2817a5a$7aa_fy RENAME TO idx_c2817a5a$7aa_fy_old;
ALTER INDEX IF EXISTS idx_c2817a5a$7aa_pulled_from RENAME TO idx_c2817a5a$7aa_pulled_from_old;
ALTER INDEX IF EXISTS idx_c2817a5a$7aa_awarding_agency_id RENAME TO idx_c2817a5a$7aa_awarding_agency_id_old;
ALTER INDEX IF EXISTS idx_c2817a5a$7aa_funding_agency_id RENAME TO idx_c2817a5a$7aa_funding_agency_id_old;
ALTER INDEX IF EXISTS idx_c2817a5a$7aa_awarding_toptier_agency_name RENAME TO idx_c2817a5a$7aa_awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_c2817a5a$7aa_awarding_subtier_agency_name RENAME TO idx_c2817a5a$7aa_awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS idx_c2817a5a$7aa_funding_toptier_agency_name RENAME TO idx_c2817a5a$7aa_funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS idx_c2817a5a$7aa_funding_subtier_agency_name RENAME TO idx_c2817a5a$7aa_funding_subtier_agency_name_old;

ALTER MATERIALIZED VIEW summary_award_view_temp RENAME TO summary_award_view;
ALTER INDEX idx_c2817a5a$7aa_unique_pk_temp RENAME TO idx_c2817a5a$7aa_unique_pk;
ALTER INDEX idx_c2817a5a$7aa_action_date_temp RENAME TO idx_c2817a5a$7aa_action_date;
ALTER INDEX idx_c2817a5a$7aa_type_temp RENAME TO idx_c2817a5a$7aa_type;
ALTER INDEX idx_c2817a5a$7aa_fy_temp RENAME TO idx_c2817a5a$7aa_fy;
ALTER INDEX idx_c2817a5a$7aa_pulled_from_temp RENAME TO idx_c2817a5a$7aa_pulled_from;
ALTER INDEX idx_c2817a5a$7aa_awarding_agency_id_temp RENAME TO idx_c2817a5a$7aa_awarding_agency_id;
ALTER INDEX idx_c2817a5a$7aa_funding_agency_id_temp RENAME TO idx_c2817a5a$7aa_funding_agency_id;
ALTER INDEX idx_c2817a5a$7aa_awarding_toptier_agency_name_temp RENAME TO idx_c2817a5a$7aa_awarding_toptier_agency_name;
ALTER INDEX idx_c2817a5a$7aa_awarding_subtier_agency_name_temp RENAME TO idx_c2817a5a$7aa_awarding_subtier_agency_name;
ALTER INDEX idx_c2817a5a$7aa_funding_toptier_agency_name_temp RENAME TO idx_c2817a5a$7aa_funding_toptier_agency_name;
ALTER INDEX idx_c2817a5a$7aa_funding_subtier_agency_name_temp RENAME TO idx_c2817a5a$7aa_funding_subtier_agency_name;

ANALYZE VERBOSE summary_award_view;
GRANT SELECT ON summary_award_view TO readonly;
