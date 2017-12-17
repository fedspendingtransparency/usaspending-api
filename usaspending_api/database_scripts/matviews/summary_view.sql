---------------------------------------------------------------
-- SUMMARY VIEW WITH COUNTS (Action Date)                   ---
---------------------------------------------------------------
-- Drop the temporary materialized views if they exist
DROP MATERIALIZED VIEW IF EXISTS summary_view_temp;
DROP MATERIALIZED VIEW IF EXISTS summary_view_old;

-- Temp matview
CREATE MATERIALIZED VIEW summary_view_temp AS
SELECT
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "awards"."type",
  "transaction_fpds"."pulled_from",
  -- Duplicated the next lines 12/5/2017 Remove Jan 1, 2018
  AT.toptier_agency_id AS awarding_agency_id,
  AT.name AS awarding_agency_name,
  AT.abbreviation AS awarding_agency_abbr,
  FT.toptier_agency_id AS funding_agency_id,
  FT.name AS funding_agency_name,
  FT.abbreviation AS funding_agency_abbr,
  -- ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  AT.toptier_agency_id AS awarding_toptier_agency_id,
  AT.name AS awarding_toptier_agency_name,
  AT.abbreviation AS awarding_toptier_agency_abbreviation,
  FT.toptier_agency_id AS funding_toptier_agency_id,
  FT.name AS funding_toptier_agency_name,
  FT.abbreviation AS funding_toptier_agency_abbreviation,
  SUM("transaction_normalized"."federal_action_obligation") AS "federal_action_obligation",
  COUNT(*) counts
FROM "transaction_normalized"
INNER JOIN "awards" ON ("transaction_normalized"."award_id" = "awards"."id")
LEFT OUTER JOIN "transaction_fpds" ON ("transaction_normalized"."id" = "transaction_fpds"."transaction_id")
LEFT OUTER JOIN "agency" AA ON ("transaction_normalized"."awarding_agency_id" = AA."id")
LEFT OUTER JOIN "agency" FA ON ("transaction_normalized"."funding_agency_id" = FA."id")
LEFT OUTER JOIN "toptier_agency" AT ON (AA."toptier_agency_id" = AT."toptier_agency_id")
LEFT OUTER JOIN "toptier_agency" FT ON (FA."toptier_agency_id" = FT."toptier_agency_id")
WHERE "transaction_normalized"."action_date" >= '2007-10-01'
GROUP BY
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "awards"."type",
  "transaction_fpds"."pulled_from",
  AT.toptier_agency_id,
  AT.name,
  AT.abbreviation,
  FT.toptier_agency_id,
  FT.name,
  FT.abbreviation;


-- Temp indexes
CREATE INDEX summary_view_action_date_temp ON summary_view_temp("action_date" DESC);
CREATE INDEX summary_view_type_temp        ON summary_view_temp("action_date" DESC, "type");

-- Rename old matview/indexes
ALTER MATERIALIZED VIEW IF EXISTS summary_view             RENAME TO summary_view_old;
ALTER INDEX IF EXISTS             summary_view_action_date RENAME TO summary_view_action_date_old;
ALTER INDEX IF EXISTS             summary_view_type        RENAME TO summary_view_type_old;

-- Rename temp matview/indexes
ALTER MATERIALIZED VIEW summary_view_temp              RENAME TO summary_view;
ALTER INDEX             summary_view_action_date_temp  RENAME TO summary_view_action_date;
ALTER INDEX             summary_view_type_temp         RENAME TO summary_view_type;
