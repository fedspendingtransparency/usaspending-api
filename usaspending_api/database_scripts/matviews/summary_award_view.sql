--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS summary_award_view_temp;
DROP MATERIALIZED VIEW IF EXISTS summary_award_view_old;

CREATE MATERIALIZED VIEW summary_award_view_temp AS
SELECT
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "awards"."type",
  "transaction_fpds"."pulled_from",
  "awards"."category",
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
  "toptier_agency" AS AT ON (AA."toptier_agency_id" = AT."toptier_agency_id")
LEFT OUTER JOIN
  "toptier_agency" AS FT ON (FA."toptier_agency_id" = FT."toptier_agency_id")
WHERE
  "transaction_normalized".action_date >= '2007-10-01'
GROUP BY
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "awards"."type",
  "transaction_fpds"."pulled_from",
  "awards"."category",
  AT.toptier_agency_id,
  AT.name,
  AT.abbreviation,
  FT.toptier_agency_id,
  FT.name,
  FT.abbreviation;

CREATE INDEX idx_8d6d41c0__action_date_temp ON summary_award_view_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_8d6d41c0__type_temp ON summary_award_view_temp USING BTREE("action_date" DESC NULLS LAST, "type") WITH (fillfactor = 100);

VACUUM ANALYZE VERBOSE summary_award_view_temp;

ALTER MATERIALIZED VIEW IF EXISTS summary_award_view RENAME TO summary_award_view_old;
ALTER INDEX IF EXISTS idx_8d6d41c0__action_date RENAME TO idx_8d6d41c0__action_date_old;
ALTER INDEX IF EXISTS idx_8d6d41c0__type RENAME TO idx_8d6d41c0__type_old;

ALTER MATERIALIZED VIEW summary_award_view_temp RENAME TO summary_award_view;
ALTER INDEX idx_8d6d41c0__action_date_temp RENAME TO idx_8d6d41c0__action_date;
ALTER INDEX idx_8d6d41c0__type_temp RENAME TO idx_8d6d41c0__type;
