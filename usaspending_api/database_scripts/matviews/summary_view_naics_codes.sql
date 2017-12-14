-----------------------------------------------------------------
-- SUMMARY VIEW NAICS (Action Date)                           ---
-----------------------------------------------------------------
-- Drop the temporary materialized views if they exist
DROP MATERIALIZED VIEW IF EXISTS summary_view_naics_codes_temp;
DROP MATERIALIZED VIEW IF EXISTS summary_view_naics_codes_old;

-- Temp matview
CREATE MATERIALIZED VIEW summary_view_naics_codes_temp AS
SELECT
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "transaction_normalized"."type",
  "transaction_fpds"."pulled_from",
  "transaction_fpds"."naics",  -- DUPLICATED 12/4. REMOVE BY JAN 1, 2018
  "transaction_fpds"."naics" AS naics_code,
  "transaction_fpds"."naics_description",
  SUM("transaction_normalized"."federal_action_obligation") AS "federal_action_obligation",
  COUNT(*) counts
FROM "transaction_normalized"
INNER JOIN "transaction_fpds" ON ("transaction_normalized"."id" = "transaction_fpds"."transaction_id")
WHERE
  "transaction_normalized".action_date >= '2007-10-01'
GROUP BY
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "transaction_normalized"."type",
  "transaction_fpds"."pulled_from",
  "transaction_fpds"."naics",
  "transaction_fpds"."naics_description";

-- Temp indexes
CREATE INDEX summary_view_naics_codes_action_date_idx_temp ON summary_view_naics_codes_temp (action_date DESC);
CREATE INDEX summary_view_naics_codes_type_idx_temp ON summary_view_naics_codes_temp (action_date DESC, "type");

-- Rename old matview/indexes
ALTER MATERIALIZED VIEW IF EXISTS summary_view_naics_codes                 RENAME TO summary_view_naics_codes_old;
ALTER INDEX IF EXISTS             summary_view_naics_codes_action_date_idx RENAME TO summary_view_naics_codes_action_date_idx_old;
ALTER INDEX IF EXISTS             summary_view_naics_codes_type_idx        RENAME TO summary_view_naics_codes_type_idx_old;

-- Rename temp matview/indexes
ALTER MATERIALIZED VIEW summary_view_naics_codes_temp                    RENAME TO summary_view_naics_codes;
ALTER INDEX             summary_view_naics_codes_action_date_idx_temp    RENAME TO summary_view_naics_codes_action_date_idx;
ALTER INDEX             summary_view_naics_codes_type_idx_temp           RENAME TO summary_view_naics_codes_type_idx;
