-----------------------------------------------------------------
-- SUMMARY VIEW CDFA NUMBERS (Action Date)                    ---
-----------------------------------------------------------------
-- Drop the temporary materialized views if they exist
DROP MATERIALIZED VIEW IF EXISTS summary_view_cfda_number_temp;
DROP MATERIALIZED VIEW IF EXISTS summary_view_cfda_number_old;

-- Temp matview
CREATE MATERIALIZED VIEW summary_view_cfda_number_temp AS
SELECT
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "transaction_normalized"."type",
  "transaction_fpds"."pulled_from",
  "transaction_fabs"."cfda_number",
  "transaction_fabs"."cfda_title",
  SUM("transaction_normalized"."federal_action_obligation") AS "federal_action_obligation",
  COUNT(*) counts
FROM "transaction_normalized"
LEFT OUTER JOIN "transaction_fabs" ON ("transaction_normalized"."id" = "transaction_fabs"."transaction_id")
LEFT OUTER JOIN "transaction_fpds" ON ("transaction_normalized"."id" = "transaction_fpds"."transaction_id")
WHERE "transaction_normalized"."action_date" >= '2007-10-01'
GROUP BY
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "transaction_normalized"."type",
  "transaction_fpds"."pulled_from",
  "transaction_fabs"."cfda_number",
  "transaction_fabs"."cfda_title";


-- Temp indexes
CREATE INDEX summary_view_cfda_number_action_date_idx_temp ON summary_view_cfda_number_temp("action_date" DESC);
CREATE INDEX summary_view_cfda_number_type_idx_temp        ON summary_view_cfda_number_temp("action_date" DESC, "type");

-- Rename old matview/indexes
ALTER MATERIALIZED VIEW IF EXISTS summary_view_cfda_number                 RENAME TO summary_view_cfda_number_old;
ALTER INDEX IF EXISTS             summary_view_cfda_number_action_date_idx RENAME TO summary_view_cfda_number_action_date_idx_old;
ALTER INDEX IF EXISTS             summary_view_cfda_number_type_idx        RENAME TO summary_view_cfda_number_type_idx_old;

-- Rename temp matview/indexes
ALTER MATERIALIZED VIEW summary_view_cfda_number_temp                 RENAME TO summary_view_cfda_number;
ALTER INDEX             summary_view_cfda_number_action_date_idx_temp RENAME TO summary_view_cfda_number_action_date_idx;
ALTER INDEX             summary_view_cfda_number_type_idx_temp        RENAME TO summary_view_cfda_number_type_idx;
