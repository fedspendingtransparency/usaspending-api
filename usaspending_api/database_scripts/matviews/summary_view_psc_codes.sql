--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS summary_view_psc_codes_temp;
DROP MATERIALIZED VIEW IF EXISTS summary_view_psc_codes_old;

CREATE MATERIALIZED VIEW summary_view_psc_codes_temp AS
SELECT
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "transaction_normalized"."type",
  "transaction_fpds"."pulled_from",
  "transaction_fpds"."product_or_service_code",
  SUM("transaction_normalized"."federal_action_obligation") AS "federal_action_obligation",
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
  "transaction_fpds"."product_or_service_code";

CREATE INDEX idx_f4f9a77f__action_date_temp ON summary_view_psc_codes_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_f4f9a77f__type_temp ON summary_view_psc_codes_temp USING BTREE("action_date" DESC NULLS LAST, "type") WITH (fillfactor = 100);

ANALYZE VERBOSE summary_view_psc_codes_temp;

ALTER MATERIALIZED VIEW IF EXISTS summary_view_psc_codes RENAME TO summary_view_psc_codes_old;
ALTER INDEX IF EXISTS idx_f4f9a77f__action_date RENAME TO idx_f4f9a77f__action_date_old;
ALTER INDEX IF EXISTS idx_f4f9a77f__type RENAME TO idx_f4f9a77f__type_old;

ALTER MATERIALIZED VIEW summary_view_psc_codes_temp RENAME TO summary_view_psc_codes;
ALTER INDEX idx_f4f9a77f__action_date_temp RENAME TO idx_f4f9a77f__action_date;
ALTER INDEX idx_f4f9a77f__type_temp RENAME TO idx_f4f9a77f__type;

GRANT SELECT ON summary_view_psc_codes TO readonly;
