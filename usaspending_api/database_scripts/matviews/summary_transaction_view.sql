-- Drop the temporary materialized views if they exist
DROP MATERIALIZED VIEW IF EXISTS summary_transaction_view_temp;
DROP MATERIALIZED VIEW IF EXISTS summary_transaction_view_old;

-- Temp matview
CREATE MATERIALIZED VIEW summary_transaction_view_temp AS
SELECT
  "transaction_normalized"."action_date",
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
  place_of_performance."county_name" AS "pop_country_name",
  place_of_performance."state_code" AS "pop_state_code",
  place_of_performance."county_code" AS "pop_county_code",
  place_of_performance."county_name" AS "pop_county_name",
  place_of_performance."congressional_code" AS "pop_congressional_code",
  place_of_performance."zip5" AS "pop_zip5",

  TAA."name" AS "awarding_toptier_agency_name",
  TAA."abbreviation" AS "awarding_toptier_agency_abbreviation",
  TFA."name" AS "funding_toptier_agency_name",
  TFA."abbreviation" AS "funding_toptier_agency_abbreviation",

  "legal_entity"."business_categories",
  "transaction_fabs"."cfda_number",
  "references_cfda"."program_title" AS "cfda_title",
  "references_cfda"."popular_name" AS "cfda_popular_name",
  -- Added duplicate rows 12/5 remove by Jan 1, 2018
  "transaction_fpds"."product_or_service_code" AS "psc_code",
  "psc"."description" AS "psc_description",
  -- ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  "transaction_fpds"."product_or_service_code",
  "psc"."description" AS product_or_service_description,

  "transaction_fpds"."naics" AS "naics_code",
  "naics"."description" AS "naics_description",

  obligation_to_enum("awards"."total_obligation") AS "total_obl_bin",
  "transaction_fpds"."type_of_contract_pricing",
  "transaction_fpds"."type_set_aside",
  "transaction_fpds"."extent_competed",
  SUM("transaction_normalized"."federal_action_obligation") AS "federal_action_obligation",
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
  "awards" ON ("transaction_normalized"."award_id" = "awards"."id")
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
LEFT OUTER JOIN
  "naics" ON ("transaction_fpds"."naics" = "naics"."code")
LEFT OUTER JOIN
  "psc" ON ("transaction_fpds"."product_or_service_code" = "psc"."code")
WHERE
  "transaction_normalized"."action_date" >= '2007-10-01' AND
  "transaction_normalized"."federal_action_obligation" IS NOT NULL
GROUP BY
  "transaction_normalized"."action_date",
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
  place_of_performance."county_name",
  place_of_performance."state_code",
  place_of_performance."county_code",
  place_of_performance."county_name",
  place_of_performance."congressional_code",
  place_of_performance."zip5",

  TAA."name",
  TAA."abbreviation",
  TFA."name",
  TFA."abbreviation",

  "legal_entity"."business_categories",
  "transaction_fabs"."cfda_number",
  "references_cfda"."program_title",
  "references_cfda"."popular_name",
  "transaction_fpds"."product_or_service_code",
  "psc"."description",
  "transaction_fpds"."naics",
  "naics"."description",

  obligation_to_enum("awards"."total_obligation"),
  "transaction_fpds"."type_of_contract_pricing",
  "transaction_fpds"."type_set_aside",
  "transaction_fpds"."extent_competed";

-- Temp indexes
CREATE INDEX summary_transaction_view_temp_date                   ON summary_transaction_view_temp("action_date" DESC);
CREATE INDEX summary_transaction_view_temp_fy                     ON summary_transaction_view_temp(fiscal_year DESC);
CREATE INDEX summary_transaction_view_temp_fy_type                ON summary_transaction_view_temp(fiscal_year DESC, "type");
CREATE INDEX summary_transaction_view_temp_type                   ON summary_transaction_view_temp("type") WHERE "type" IS NOT NULL;
CREATE INDEX summary_transaction_view_temp_pulled_from            ON summary_transaction_view_temp("pulled_from") WHERE "pulled_from" IS NOT NULL;

CREATE INDEX summary_transaction_view_temp_recipient_country_code ON summary_transaction_view_temp("recipient_location_country_code");
CREATE INDEX summary_transaction_view_temp_recipient_state_code   ON summary_transaction_view_temp("recipient_location_state_code");
CREATE INDEX summary_transaction_view_temp_recipient_county_code  ON summary_transaction_view_temp("recipient_location_county_code");
CREATE INDEX summary_transaction_view_temp_recipient_zip          ON summary_transaction_view_temp("recipient_location_zip5");

CREATE INDEX summary_transaction_view_temp_pop_country_code       ON summary_transaction_view_temp("pop_country_code");
CREATE INDEX summary_transaction_view_temp_pop_state_code         ON summary_transaction_view_temp("pop_state_code");
CREATE INDEX summary_transaction_view_temp_pop_county_code        ON summary_transaction_view_temp("pop_county_code");
CREATE INDEX summary_transaction_view_temp_pop_zip                ON summary_transaction_view_temp("pop_zip5");

CREATE INDEX summary_transaction_view_temp_award_agency_top       ON summary_transaction_view_temp(awarding_toptier_agency_name);

CREATE INDEX summary_transaction_view_temp_cfda_num               ON summary_transaction_view_temp("cfda_number") WHERE "cfda_number" IS NOT NULL;
CREATE INDEX summary_transaction_view_temp_cfda_title             ON summary_transaction_view_temp("cfda_title") WHERE "cfda_title" IS NOT NULL;
CREATE INDEX summary_transaction_view_temp_psc0                   ON summary_transaction_view_temp("psc_code") WHERE "psc_code" IS NOT NULL;
CREATE INDEX summary_transaction_view_temp_psc                    ON summary_transaction_view_temp("product_or_service_code") WHERE "product_or_service_code" IS NOT NULL;
CREATE INDEX summary_transaction_view_temp_naics                  ON summary_transaction_view_temp("naics_code") WHERE "naics_code" IS NOT NULL;

CREATE INDEX summary_transaction_view_temp_total_obl_bin          ON summary_transaction_view_temp("total_obl_bin") WHERE "total_obl_bin" IS NOT NULL;
CREATE INDEX summary_transaction_view_temp_type_of_contract       ON summary_transaction_view_temp(type_of_contract_pricing);
CREATE INDEX summary_transaction_view_temp_fy_set_aside           ON summary_transaction_view_temp(fiscal_year DESC, type_set_aside);
CREATE INDEX summary_transaction_view_temp_extent_competed        ON summary_transaction_view_temp(extent_competed);


-- Rename old matview/indexes
ALTER MATERIALIZED VIEW IF EXISTS summary_transaction_view             RENAME TO summary_transaction_view_old;
ALTER INDEX IF EXISTS summary_transaction_view_date                    RENAME TO summary_transaction_view_date_old;
ALTER INDEX IF EXISTS summary_transaction_view_fy                      RENAME TO summary_transaction_view_fy_old;
ALTER INDEX IF EXISTS summary_transaction_view_fy_type                 RENAME TO summary_transaction_view_fy_type_old;
ALTER INDEX IF EXISTS summary_transaction_view_type                    RENAME TO summary_transaction_view_type_old;
ALTER INDEX IF EXISTS summary_transaction_view_pulled_from             RENAME TO summary_transaction_view_pulled_from_old;

ALTER INDEX IF EXISTS summary_transaction_view_recipient_country_code  RENAME TO summary_transaction_view_recipient_country_code_old;
ALTER INDEX IF EXISTS summary_transaction_view_recipient_state_code    RENAME TO summary_transaction_view_recipient_state_code_old;
ALTER INDEX IF EXISTS summary_transaction_view_recipient_county_code   RENAME TO summary_transaction_view_recipient_county_code_old;
ALTER INDEX IF EXISTS summary_transaction_view_recipient_zip           RENAME TO summary_transaction_view_recipient_zip_old;

ALTER INDEX IF EXISTS summary_transaction_view_pop_country_code        RENAME TO summary_transaction_view_pop_country_code_old;
ALTER INDEX IF EXISTS summary_transaction_view_pop_state_code          RENAME TO summary_transaction_view_pop_state_code_old;
ALTER INDEX IF EXISTS summary_transaction_view_pop_county_code         RENAME TO summary_transaction_view_pop_county_code_old;
ALTER INDEX IF EXISTS summary_transaction_view_pop_zip                 RENAME TO summary_transaction_view_pop_zip_old;

ALTER INDEX IF EXISTS summary_transaction_view_award_agency_top        RENAME TO summary_transaction_view_award_agency_top_old;

ALTER INDEX IF EXISTS summary_transaction_view_cfda_num                RENAME TO summary_transaction_view_cfda_num_old;
ALTER INDEX IF EXISTS summary_transaction_view_cfda_title              RENAME TO summary_transaction_view_cfda_title_old;
ALTER INDEX IF EXISTS summary_transaction_view_psc0                    RENAME TO summary_transaction_view_psc0_old;
ALTER INDEX IF EXISTS summary_transaction_view_psc                     RENAME TO summary_transaction_view_psc_old;
ALTER INDEX IF EXISTS summary_transaction_view_naics                   RENAME TO summary_transaction_view_naics_old;

ALTER INDEX IF EXISTS summary_transaction_view_total_obl_bin           RENAME TO summary_transaction_view_total_obl_bin_old;
ALTER INDEX IF EXISTS summary_transaction_view_type_of_contract        RENAME TO summary_transaction_view_type_of_contract_old;
ALTER INDEX IF EXISTS summary_transaction_view_fy_set_aside            RENAME TO summary_transaction_view_fy_set_aside_old;
ALTER INDEX IF EXISTS summary_transaction_view_extent_competed         RENAME TO summary_transaction_view_extent_competed_old;


-- Rename temp matview/indexes
ALTER MATERIALIZED VIEW summary_transaction_view_temp             RENAME TO summary_transaction_view;
ALTER INDEX summary_transaction_view_temp_date                    RENAME TO summary_transaction_view_date;
ALTER INDEX summary_transaction_view_temp_fy                      RENAME TO summary_transaction_view_fy;
ALTER INDEX summary_transaction_view_temp_fy_type                 RENAME TO summary_transaction_view_fy_type;
ALTER INDEX summary_transaction_view_temp_type                    RENAME TO summary_transaction_view_type;
ALTER INDEX summary_transaction_view_temp_pulled_from             RENAME TO summary_transaction_view_pulled_from;

ALTER INDEX summary_transaction_view_temp_pop_country_code        RENAME TO summary_transaction_view_pop_country_code;
ALTER INDEX summary_transaction_view_temp_pop_state_code          RENAME TO summary_transaction_view_pop_state_code;
ALTER INDEX summary_transaction_view_temp_pop_county_code         RENAME TO summary_transaction_view_pop_county_code;
ALTER INDEX summary_transaction_view_temp_pop_zip                 RENAME TO summary_transaction_view_pop_zip;

ALTER INDEX summary_transaction_view_temp_recipient_country_code  RENAME TO summary_transaction_view_recipient_country_code;
ALTER INDEX summary_transaction_view_temp_recipient_state_code    RENAME TO summary_transaction_view_recipient_state_code;
ALTER INDEX summary_transaction_view_temp_recipient_county_code   RENAME TO summary_transaction_view_recipient_county_code;
ALTER INDEX summary_transaction_view_temp_recipient_zip           RENAME TO summary_transaction_view_recipient_zip;

ALTER INDEX summary_transaction_view_temp_award_agency_top        RENAME TO summary_transaction_view_award_agency_top;

ALTER INDEX summary_transaction_view_temp_cfda_num                RENAME TO summary_transaction_view_cfda_num;
ALTER INDEX summary_transaction_view_temp_cfda_title              RENAME TO summary_transaction_view_cfda_title;
ALTER INDEX summary_transaction_view_temp_psc0                    RENAME TO summary_transaction_view_psc0;
ALTER INDEX summary_transaction_view_temp_psc                     RENAME TO summary_transaction_view_psc;
ALTER INDEX summary_transaction_view_temp_naics                   RENAME TO summary_transaction_view_naics;

ALTER INDEX summary_transaction_view_temp_total_obl_bin           RENAME TO summary_transaction_view_total_obl_bin;
ALTER INDEX summary_transaction_view_temp_type_of_contract        RENAME TO summary_transaction_view_type_of_contract;
ALTER INDEX summary_transaction_view_temp_fy_set_aside            RENAME TO summary_transaction_view_fy_set_aside;
ALTER INDEX summary_transaction_view_temp_extent_competed         RENAME TO summary_transaction_view_extent_competed;
