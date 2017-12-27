--------------------------------------------------------
-- Created using matview_sql_generator.py             --
--    The SQL definition is stored in a json file     --
--    Look in matview_generator for the code.         --
--                                                    --
--  DO NOT DIRECTLY EDIT THIS FILE!!!                 --
--------------------------------------------------------
DROP MATERIALIZED VIEW IF EXISTS summary_view_temp;
DROP MATERIALIZED VIEW IF EXISTS summary_view_old;

CREATE MATERIALIZED VIEW summary_view_temp AS
SELECT
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "awards"."type",
  "transaction_fpds"."pulled_from",
  TAA.name AS awarding_toptier_agency_name,
  TAA.abbreviation AS awarding_toptier_agency_abbreviation,
  TFA.name AS funding_toptier_agency_name,
  TFA.abbreviation AS funding_toptier_agency_abbreviation,

  SAA."name" AS awarding_subtier_agency_name,
  SAA."abbreviation" AS awarding_subtier_agency_abbreviation,
  SFA."name" AS funding_subtier_agency_name,
  SFA."abbreviation" AS funding_subtier_agency_abbreviation,

  SUM("transaction_normalized"."federal_action_obligation") AS "federal_action_obligation",
  COUNT(*) AS counts
FROM
  "transaction_normalized"
INNER JOIN
  "awards" ON ("transaction_normalized"."award_id" = "awards"."id")
LEFT OUTER JOIN
  "transaction_fpds" ON ("transaction_normalized"."id" = "transaction_fpds"."transaction_id")
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
  "transaction_normalized"."action_date" >= '2007-10-01'
GROUP BY
  "transaction_normalized"."action_date",
  "transaction_normalized"."fiscal_year",
  "awards"."type",
  "transaction_fpds"."pulled_from",
  TAA.name,
  TAA.abbreviation,
  TFA.name,
  TFA.abbreviation,
  SAA.name,
  SAA.abbreviation,
  SFA.name,
  SFA.abbreviation;

CREATE INDEX idx_f2a62d56__action_date_temp ON summary_view_temp USING BTREE("action_date" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_f2a62d56__type_temp ON summary_view_temp USING BTREE("type") WITH (fillfactor = 100);
CREATE INDEX idx_f2a62d56__fy_temp ON summary_view_temp USING BTREE("fiscal_year" DESC NULLS LAST) WITH (fillfactor = 100);
CREATE INDEX idx_f2a62d56__pulled_from_temp ON summary_view_temp USING BTREE("pulled_from") WITH (fillfactor = 100) WHERE "pulled_from" IS NOT NULL;

VACUUM ANALYZE VERBOSE summary_view_temp;

ALTER MATERIALIZED VIEW IF EXISTS summary_view RENAME TO summary_view_old;
ALTER INDEX IF EXISTS idx_f2a62d56__action_date RENAME TO idx_f2a62d56__action_date_old;
ALTER INDEX IF EXISTS idx_f2a62d56__type RENAME TO idx_f2a62d56__type_old;
ALTER INDEX IF EXISTS idx_f2a62d56__fy RENAME TO idx_f2a62d56__fy_old;
ALTER INDEX IF EXISTS idx_f2a62d56__pulled_from RENAME TO idx_f2a62d56__pulled_from_old;

ALTER MATERIALIZED VIEW summary_view_temp RENAME TO summary_view;
ALTER INDEX idx_f2a62d56__action_date_temp RENAME TO idx_f2a62d56__action_date;
ALTER INDEX idx_f2a62d56__type_temp RENAME TO idx_f2a62d56__type;
ALTER INDEX idx_f2a62d56__fy_temp RENAME TO idx_f2a62d56__fy;
ALTER INDEX idx_f2a62d56__pulled_from_temp RENAME TO idx_f2a62d56__pulled_from;
