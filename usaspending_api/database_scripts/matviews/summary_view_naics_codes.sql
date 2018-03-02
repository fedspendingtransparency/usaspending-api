--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS summary_view_naics_codes_temp CASCADE;
DROP MATERIALIZED VIEW IF EXISTS summary_view_naics_codes_old CASCADE;

CREATE MATERIALIZED VIEW summary_view_naics_codes_temp AS
SELECT
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "transaction_normalized"."type",
  "transaction_fpds"."pulled_from",
  "transaction_fpds"."naics" AS naics_code,
  "transaction_fpds"."naics_description",
  SUM(COALESCE("transaction_normalized"."federal_action_obligation", 0))::NUMERIC(20, 2) AS "federal_action_obligation",
  0::NUMERIC(20, 2) AS "original_loan_subsidy_cost",
  COUNT(*) counts
FROM
  "transaction_normalized"
INNER JOIN
  "transaction_fpds" ON ("transaction_normalized"."id" = "transaction_fpds"."transaction_id")
WHERE
  "transaction_normalized".action_date >= '2007-10-01'
GROUP BY
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "transaction_normalized"."type",
  "transaction_fpds"."pulled_from",
  "transaction_fpds"."naics",
  "transaction_fpds"."naics_description";

CREATE INDEX idx_e9bb5856__action_date_temp ON summary_view_naics_codes_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_e9bb5856__type_temp ON summary_view_naics_codes_temp USING BTREE("action_date" DESC NULLS LAST, "type") WITH (fillfactor = 100);
CREATE INDEX idx_e9bb5856__naics_temp ON summary_view_naics_codes_temp USING BTREE("naics_code") WITH (fillfactor = 100) WHERE "naics_code" IS NOT NULL;
CREATE INDEX idx_e9bb5856__tuned_type_and_idv_temp ON summary_view_naics_codes_temp USING BTREE("type", "pulled_from") WITH (fillfactor = 100) WHERE "type" IS NULL AND "pulled_from" IS NOT NULL;

ALTER MATERIALIZED VIEW IF EXISTS summary_view_naics_codes RENAME TO summary_view_naics_codes_old;
ALTER INDEX IF EXISTS idx_e9bb5856__action_date RENAME TO idx_e9bb5856__action_date_old;
ALTER INDEX IF EXISTS idx_e9bb5856__type RENAME TO idx_e9bb5856__type_old;
ALTER INDEX IF EXISTS idx_e9bb5856__naics RENAME TO idx_e9bb5856__naics_old;
ALTER INDEX IF EXISTS idx_e9bb5856__tuned_type_and_idv RENAME TO idx_e9bb5856__tuned_type_and_idv_old;

ALTER MATERIALIZED VIEW summary_view_naics_codes_temp RENAME TO summary_view_naics_codes;
ALTER INDEX idx_e9bb5856__action_date_temp RENAME TO idx_e9bb5856__action_date;
ALTER INDEX idx_e9bb5856__type_temp RENAME TO idx_e9bb5856__type;
ALTER INDEX idx_e9bb5856__naics_temp RENAME TO idx_e9bb5856__naics;
ALTER INDEX idx_e9bb5856__tuned_type_and_idv_temp RENAME TO idx_e9bb5856__tuned_type_and_idv;

ANALYZE VERBOSE summary_view_naics_codes;
GRANT SELECT ON summary_view_naics_codes TO readonly;
