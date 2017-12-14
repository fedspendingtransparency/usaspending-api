-----------------------------------------------------------------
-- SUMMARY VIEW PSC CODES (Action Date)                       ---
-----------------------------------------------------------------
-- Drop the temporary materialized views if they exist
DROP MATERIALIZED VIEW IF EXISTS summary_view_psc_codes_temp;
DROP MATERIALIZED VIEW IF EXISTS summary_view_psc_codes_old;

-- Temp matview
CREATE MATERIALIZED VIEW summary_view_psc_codes_temp AS
SELECT
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "transaction_normalized"."type",
  "transaction_fpds"."pulled_from",
  "transaction_fpds"."product_or_service_code",
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
  "transaction_fpds"."product_or_service_code";


-- Temp indexes
CREATE INDEX summary_view_psc_codes_action_date_idx_temp ON summary_view_psc_codes_temp ("action_date" DESC);
CREATE INDEX summary_view_psc_codes_type_idx_temp        ON summary_view_psc_codes_temp ("action_date" DESC, "type");

-- Rename old matview/indexes
ALTER MATERIALIZED VIEW IF EXISTS summary_view_psc_codes                 RENAME TO summary_view_psc_codes_old;
ALTER INDEX IF EXISTS             summary_view_psc_codes_action_date_idx RENAME TO summary_view_psc_codes_action_date_idx_old;
ALTER INDEX IF EXISTS             summary_view_psc_codes_type_idx        RENAME TO summary_view_psc_codes_type_idx_old;

-- Rename temp matview/indexes
ALTER MATERIALIZED VIEW summary_view_psc_codes_temp                    RENAME TO summary_view_psc_codes;
ALTER INDEX             summary_view_psc_codes_action_date_idx_temp    RENAME TO summary_view_psc_codes_action_date_idx;
ALTER INDEX             summary_view_psc_codes_type_idx_temp           RENAME TO summary_view_psc_codes_type_idx;
