-- Relies on functions_and_enums.sql --
-- Drop the temporary materialized views if they exist
DROP MATERIALIZED VIEW IF EXISTS universal_transaction_matview_temp;
DROP MATERIALIZED VIEW IF EXISTS universal_transaction_matview_old;

-- Temp matview
CREATE MATERIALIZED VIEW universal_transaction_matview_temp AS SELECT
  "transaction_normalized"."id" AS transaction_id,
  "transaction_normalized"."action_date"::date,
  "transaction_normalized"."fiscal_year",
  "transaction_normalized"."type",
  "transaction_normalized"."action_type",
  "transaction_normalized"."award_id",
  "awards"."category" AS award_category,
  "awards"."total_obligation",
  obligation_to_enum("awards"."total_obligation") AS total_obl_bin,
  "awards"."fain",
  "awards"."uri",
  "awards"."piid",
  "transaction_normalized"."federal_action_obligation",

  place_of_performance."location_id" AS pop_location_id,
  place_of_performance."country_name" AS pop_country_name,
  place_of_performance."location_country_code" AS pop_country_code,
  place_of_performance."zip5" AS pop_zip5,
  place_of_performance."county_code" AS pop_county_code,
  place_of_performance."county_name" AS pop_county_name,
  place_of_performance."state_code" AS pop_state_code,
  place_of_performance."congressional_code" AS pop_congressional_code,

  "transaction_fabs"."face_value_loan_guarantee",
  "transaction_fabs"."original_loan_subsidy_cost",
  "transaction_normalized"."description" AS transaction_description,
  "transaction_normalized"."awarding_agency_id",
  "transaction_fabs"."awarding_agency_code",
  "transaction_fabs"."awarding_agency_name",
  "transaction_normalized"."funding_agency_id",
  "transaction_fabs"."funding_agency_code",
  "transaction_fabs"."funding_agency_name",

  "transaction_fpds"."naics" AS naics_code,
  "naics"."description" AS naics_description,

  "transaction_fpds"."product_or_service_code",
  "psc"."description" AS product_or_service_description,
  -- DUPLICATED ON 12/4. REMOVE BY JAN 1, 2018!!!!!!!!!!!!!!!
  "transaction_fpds"."product_or_service_code" AS psc_code,
  "psc"."description" AS psc_description,
  -- ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

  "transaction_fpds"."pulled_from",
  "transaction_fpds"."type_of_contract_pricing",
  "transaction_fpds"."type_set_aside",
  "transaction_fpds"."extent_competed",
  "transaction_fabs"."cfda_number",
  "references_cfda"."program_title" AS cfda_title,
  "references_cfda"."popular_name" AS cfda_popular_name,

  "transaction_normalized"."recipient_id",
  "legal_entity"."recipient_name",
  "legal_entity"."recipient_unique_id",
  "legal_entity"."parent_recipient_unique_id",
  "legal_entity"."business_categories",

  recipient_location."location_id" AS recipient_location_id,
  recipient_location."location_country_code" AS recipient_location_country_code,
  recipient_location."country_name" AS recipient_location_country_name,
  recipient_location."zip5" AS recipient_location_zip5,
  recipient_location."state_code" AS recipient_location_state_code,
  recipient_location."state_name" AS recipient_location_state_name,
  recipient_location."county_code" AS recipient_location_county_code,
  recipient_location."county_name" AS recipient_location_county_name,
  recipient_location."congressional_code" AS recipient_location_congressional_code,

  TAA."name" AS awarding_toptier_agency_name,
  TFA."name" AS funding_toptier_agency_name,
  SAA."name" AS awarding_subtier_agency_name,
  SFA."name" AS funding_subtier_agency_name,
  TAA."abbreviation" AS awarding_toptier_agency_abbreviation,
  TFA."abbreviation" AS funding_toptier_agency_abbreviation,
  SAA."abbreviation" AS awarding_subtier_agency_abbreviation,
  SFA."abbreviation" AS funding_subtier_agency_abbreviation

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
  "transaction_normalized"."federal_action_obligation" IS NOT NULL;


-- Temp indexes
CREATE INDEX utm_id                         ON universal_transaction_matview_temp("transaction_id");
CREATE INDEX utm_date                       ON universal_transaction_matview_temp("action_date");
CREATE INDEX utm_fy_id                      ON universal_transaction_matview_temp("fiscal_year");
CREATE INDEX utm_type                       ON universal_transaction_matview_temp("type")                       WHERE "type" IS NOT NULL;
CREATE INDEX utm_action_type                ON universal_transaction_matview_temp("action_type")                WHERE "action_type" IS NOT NULL;
CREATE INDEX utm_award_id                   ON universal_transaction_matview_temp("award_id")                   WHERE "award_id" IS NOT NULL;
CREATE INDEX utm_award_category             ON universal_transaction_matview_temp("award_category")             WHERE "award_category" IS NOT NULL;
CREATE INDEX utm_total_obl                  ON universal_transaction_matview_temp("total_obligation");
CREATE INDEX utm_total_obl_bin              ON universal_transaction_matview_temp("total_obl_bin")              WHERE "total_obl_bin" IS NOT NULL;

CREATE INDEX utm_fain                       ON universal_transaction_matview_temp(UPPER("fain"))                WHERE "fain" IS NOT NULL;
CREATE INDEX utm_fain_gin_trgm              ON universal_transaction_matview_temp                               USING gin (UPPER(fain) gin_trgm_ops);
CREATE INDEX utm_piid                       ON universal_transaction_matview_temp(UPPER("piid"))                WHERE "piid" IS NOT NULL;
CREATE INDEX utm_piid_gin_trgm              ON universal_transaction_matview_temp                               USING gin (UPPER(piid) gin_trgm_ops);
CREATE INDEX utm_uri                        ON universal_transaction_matview_temp(UPPER("uri"))                 WHERE "uri" IS NOT NULL;
CREATE INDEX utm_uri_gin_trgm               ON universal_transaction_matview_temp                               USING gin (UPPER(uri) gin_trgm_ops);

CREATE INDEX utm_pop_country_code           ON universal_transaction_matview_temp("pop_country_code")           WHERE "pop_country_code" IS NOT NULL;
CREATE INDEX utm_pop_zip5                   ON universal_transaction_matview_temp("pop_zip5")                   WHERE "pop_zip5" IS NOT NULL;
CREATE INDEX utm_pop_county_code            ON universal_transaction_matview_temp("pop_county_code")            WHERE "pop_county_code" IS NOT NULL;
CREATE INDEX utm_pop_state                  ON universal_transaction_matview_temp("pop_state_code")             WHERE "pop_state_code" IS NOT NULL;
CREATE INDEX utm_pop_congress               ON universal_transaction_matview_temp("pop_congressional_code")     WHERE "pop_congressional_code" IS NOT NULL;

CREATE INDEX utm_face_value_loan_guarantee  ON universal_transaction_matview_temp("face_value_loan_guarantee")  WHERE "face_value_loan_guarantee" IS NOT NULL;
CREATE INDEX utm_original_loan_subsidy_cost ON universal_transaction_matview_temp("original_loan_subsidy_cost") WHERE "original_loan_subsidy_cost" IS NOT NULL;

CREATE INDEX utm_naics_code             ON universal_transaction_matview_temp("naics_code")                     WHERE "naics_code" IS NOT NULL;
CREATE INDEX utm_naics_desc             ON universal_transaction_matview_temp("naics_description")              USING gin("naics_code" gin_trgm_ops);
CREATE INDEX utm_psc_code               ON universal_transaction_matview_temp(UPPER("product_or_service_code")) WHERE "product_or_service_code" IS NOT NULL;

CREATE INDEX utm_psc_code_0             ON universal_transaction_matview_temp("psc_code")                       WHERE "psc_code" IS NOT NULL;
CREATE INDEX utm_psc_desc_0             ON universal_transaction_matview_temp("psc_description")                WHERE "psc_description" IS NOT NULL;
CREATE INDEX utm_psc_desc_uppr_tmp      ON universal_transaction_matview_temp                                   USING gin(UPPER("product_or_service_description") gin_trgm_ops);
CREATE INDEX utm_naics_desc_uppr_tmp    ON universal_transaction_matview_temp                                   USING gin(UPPER("naics_description") gin_trgm_ops);

CREATE INDEX utm_pulled_from                ON universal_transaction_matview_temp("pulled_from")                WHERE "pulled_from" IS NOT NULL;
CREATE INDEX utm_type_of_contract_pricing   ON universal_transaction_matview_temp("type_of_contract_pricing")   WHERE "type_of_contract_pricing" IS NOT NULL;
CREATE INDEX utm_type_set_aside             ON universal_transaction_matview_temp("type_set_aside")             WHERE "type_set_aside" IS NOT NULL;
CREATE INDEX utm_extent_competed            ON universal_transaction_matview_temp("extent_competed")            WHERE "extent_competed" IS NOT NULL;
CREATE INDEX utm_cfda_multi                 ON universal_transaction_matview_temp("cfda_number", "cfda_title")  WHERE "cfda_number" IS NOT NULL;

-- GIN Index to help with LIKE queries
CREATE INDEX utm_recipient_name_gin_trgm    ON universal_transaction_matview_temp                                      USING gin (UPPER(recipient_name) gin_trgm_ops);
CREATE INDEX utm_recipient                  ON universal_transaction_matview_temp("recipient_id")                      WHERE "recipient_id" IS NOT NULL;
CREATE INDEX utm_recipient_uniq             ON universal_transaction_matview_temp(UPPER("recipient_unique_id"))        WHERE "recipient_unique_id" IS NOT NULL;
CREATE INDEX utm_parent_recipient_uniq      ON universal_transaction_matview_temp(UPPER("parent_recipient_unique_id")) WHERE "parent_recipient_unique_id" IS NOT NULL;

CREATE INDEX utm_rl_country_code            ON universal_transaction_matview_temp("recipient_location_country_code") WHERE "recipient_location_country_code" IS NOT NULL;
CREATE INDEX utm_rl_zip5                    ON universal_transaction_matview_temp("recipient_location_zip5")         WHERE "recipient_location_zip5" IS NOT NULL;
CREATE INDEX utm_rl_state_code              ON universal_transaction_matview_temp("recipient_location_state_code")   WHERE "recipient_location_state_code" IS NOT NULL;
CREATE INDEX utm_rl_county_code             ON universal_transaction_matview_temp("recipient_location_county_code")  WHERE "recipient_location_county_code" IS NOT NULL;
CREATE INDEX utm_rl_congress                ON universal_transaction_matview_temp("recipient_location_congressional_code")
                                            WHERE "recipient_location_congressional_code" IS NOT NULL;

CREATE INDEX utm_awarding_toptier_agency_name      ON universal_transaction_matview_temp("awarding_toptier_agency_name", "awarding_toptier_agency_abbreviation") WHERE "awarding_toptier_agency_name" IS NOT NULL;
CREATE INDEX utm_funding_toptier_agency_name       ON universal_transaction_matview_temp("funding_toptier_agency_name", "funding_toptier_agency_abbreviation") WHERE "funding_toptier_agency_name" IS NOT NULL;
CREATE INDEX utm_awarding_subtier_agency_name      ON universal_transaction_matview_temp("awarding_subtier_agency_name", "awarding_subtier_agency_abbreviation") WHERE "awarding_subtier_agency_name" IS NOT NULL;
CREATE INDEX utm_funding_subtier_agency_name       ON universal_transaction_matview_temp("funding_subtier_agency_name", "funding_subtier_agency_abbreviation") WHERE "funding_subtier_agency_name" IS NOT NULL;

CREATE INDEX utm_awarding_toptier_agency_name_trgm ON universal_transaction_matview_temp USING gin(UPPER("awarding_toptier_agency_name") gin_trgm_ops);
CREATE INDEX utm_funding_toptier_agency_name_trgm  ON universal_transaction_matview_temp USING gin(UPPER("funding_toptier_agency_name") gin_trgm_ops);
CREATE INDEX utm_awarding_subtier_agency_name_trgm ON universal_transaction_matview_temp USING gin(UPPER("awarding_subtier_agency_name") gin_trgm_ops);
CREATE INDEX utm_funding_subtier_agency_name_trgm  ON universal_transaction_matview_temp USING gin(UPPER("funding_subtier_agency_name") gin_trgm_ops);


-- Rename old matview/indexes
ALTER MATERIALIZED VIEW IF EXISTS universal_transaction_matview RENAME TO universal_transaction_matview_old;
ALTER INDEX IF EXISTS universal_transaction_matview_id                           RENAME TO universal_transaction_matview_id_old;
ALTER INDEX IF EXISTS universal_transaction_matview_date                         RENAME TO universal_transaction_matview_date_old;
ALTER INDEX IF EXISTS universal_transaction_matview_fy_id                        RENAME TO universal_transaction_matview_fy_id_old;
ALTER INDEX IF EXISTS universal_transaction_matview_type                         RENAME TO universal_transaction_matview_type_old;
ALTER INDEX IF EXISTS universal_transaction_matview_action_type                  RENAME TO universal_transaction_matview_action_type_old;
ALTER INDEX IF EXISTS universal_transaction_matview_award_id                     RENAME TO universal_transaction_matview_award_id_old;
ALTER INDEX IF EXISTS universal_transaction_matview_award_category               RENAME TO universal_transaction_matview_award_category_old;
ALTER INDEX IF EXISTS universal_transaction_matview_total_obl                    RENAME TO universal_transaction_matview_total_obl_old;
ALTER INDEX IF EXISTS universal_transaction_matview_total_obl_bin                RENAME TO universal_transaction_matview_total_obl_bin_old;

ALTER INDEX IF EXISTS universal_transaction_matview_fain                         RENAME TO universal_transaction_matview_fain_old;
ALTER INDEX IF EXISTS universal_transaction_matview_fain_gin_trgm                RENAME TO universal_transaction_matview_fain_gin_trgm_old;
ALTER INDEX IF EXISTS universal_transaction_matview_piid                         RENAME TO universal_transaction_matview_piid_old;
ALTER INDEX IF EXISTS universal_transaction_matview_piid_gin_trgm                RENAME TO universal_transaction_matview_piid_gin_trgm_old;
ALTER INDEX IF EXISTS universal_transaction_matview_uri                          RENAME TO universal_transaction_matview_uri_old;
ALTER INDEX IF EXISTS universal_transaction_matview_uri_gin_trgm                 RENAME TO universal_transaction_matview_uri_gin_trgm_old;

ALTER INDEX IF EXISTS universal_transaction_matview_pop_country_code             RENAME TO universal_transaction_matview_pop_country_code_old;
ALTER INDEX IF EXISTS universal_transaction_matview_pop_zip5                     RENAME TO universal_transaction_matview_pop_zip5_old;
ALTER INDEX IF EXISTS universal_transaction_matview_pop_county_code              RENAME TO universal_transaction_matview_pop_county_code_old;
ALTER INDEX IF EXISTS universal_transaction_matview_pop_state                    RENAME TO universal_transaction_matview_pop_state_old;
ALTER INDEX IF EXISTS universal_transaction_matview_pop_congress                 RENAME TO universal_transaction_matview_pop_congress_old;

ALTER INDEX IF EXISTS universal_transaction_matview_face_value_loan_guarantee    RENAME TO universal_transaction_matview_face_value_loan_guarantee_old;
ALTER INDEX IF EXISTS universal_transaction_matview_original_loan_subsidy_cost   RENAME TO universal_transaction_matview_original_loan_subsidy_cost_old;

ALTER INDEX IF EXISTS universal_transaction_matview_naics_code                   RENAME TO universal_transaction_matview_naics_code_old;
ALTER INDEX IF EXISTS universal_transaction_matview_naics_desc                   RENAME TO universal_transaction_matview_naics_desc_old;
ALTER INDEX IF EXISTS universal_transaction_matview_psc_code                     RENAME TO universal_transaction_matview_psc_code_old;
ALTER INDEX IF EXISTS universal_transaction_matview_psc_desc                     RENAME TO universal_transaction_matview_psc_desc_old;
ALTER INDEX IF EXISTS universal_transaction_matview_psc_code_0                   RENAME TO universal_transaction_matview_psc_code_0_old;
ALTER INDEX IF EXISTS universal_transaction_matview_psc_desc_0                   RENAME TO universal_transaction_matview_psc_desc_0_old;
ALTER INDEX IF EXISTS universal_transaction_matview_psc_desc_uppr_idx            RENAME TO universal_transaction_matview_psc_desc_uppr_old;
ALTER INDEX IF EXISTS universal_transaction_matview_naics_desc_uppr_idx          RENAME TO universal_transaction_matview_naics_desc_uppr_old;

ALTER INDEX IF EXISTS universal_transaction_matview_pulled_from                  RENAME TO universal_transaction_matview_pulled_from_old;
ALTER INDEX IF EXISTS universal_transaction_matview_type_of_contract_pricing     RENAME TO universal_transaction_matview_type_of_contract_pricing_old;
ALTER INDEX IF EXISTS universal_transaction_matview_type_set_aside               RENAME TO universal_transaction_matview_type_set_aside_old;
ALTER INDEX IF EXISTS universal_transaction_matview_extent_competed              RENAME TO universal_transaction_matview_extent_competed_old;
ALTER INDEX IF EXISTS universal_transaction_matview_cfda_multi                   RENAME TO universal_transaction_matview_cfda_multi_old;

ALTER INDEX IF EXISTS universal_transaction_matview_recipient_name_gin_trgm      RENAME TO universal_transaction_matview_recipient_name_gin_trgm_old;
ALTER INDEX IF EXISTS universal_transaction_matview_recipient                    RENAME TO universal_transaction_matview_recipient_old;
ALTER INDEX IF EXISTS universal_transaction_matview_recipient_uniq               RENAME TO universal_transaction_matview_recipient_uniq_old;
ALTER INDEX IF EXISTS universal_transaction_matview_parent_recipient_uniq        RENAME TO universal_transaction_matview_parent_recipient_uniq_old;

ALTER INDEX IF EXISTS universal_transaction_matview_rl_country_code              RENAME TO universal_transaction_matview_rl_country_code_old;
ALTER INDEX IF EXISTS universal_transaction_matview_rl_zip5                      RENAME TO universal_transaction_matview_rl_zip5_old;
ALTER INDEX IF EXISTS universal_transaction_matview_rl_state_code                RENAME TO universal_transaction_matview_rl_state_code_old;
ALTER INDEX IF EXISTS universal_transaction_matview_rl_county_code               RENAME TO universal_transaction_matview_rl_county_code_old;
ALTER INDEX IF EXISTS universal_transaction_matview_rl_congress                  RENAME TO universal_transaction_matview_rl_congress_old;

ALTER INDEX IF EXISTS universal_transaction_matview_awarding_toptier_agency_name RENAME TO universal_transaction_matview_awarding_toptier_agency_name_old;
ALTER INDEX IF EXISTS universal_transaction_matview_funding_toptier_agency_name  RENAME TO universal_transaction_matview_funding_toptier_agency_name_old;
ALTER INDEX IF EXISTS universal_transaction_matview_awarding_subtier_agency_name RENAME TO universal_transaction_matview_awarding_subtier_agency_name_old;
ALTER INDEX IF EXISTS universal_transaction_matview_funding_subtier_agency_name  RENAME TO universal_transaction_matview_funding_subtier_agency_name_old;

ALTER INDEX IF EXISTS universal_transaction_matview_awarding_toptier_agency_name_trgm RENAME TO universal_transaction_matview_awarding_toptier_agency_name_trgm_old;
ALTER INDEX IF EXISTS universal_transaction_matview_funding_toptier_agency_name_trgm RENAME TO universal_transaction_matview_funding_toptier_agency_name_trgm_old;
ALTER INDEX IF EXISTS universal_transaction_matview_awarding_subtier_agency_name_trgm RENAME TO universal_transaction_matview_awarding_subtier_agency_name_trgm_old;
ALTER INDEX IF EXISTS universal_transaction_matview_funding_subtier_agency_name_trgm RENAME TO universal_transaction_matview_funding_subtier_agency_name_trgm_old;

-- Rename temp matview/indexes
ALTER MATERIALIZED VIEW universal_transaction_matview_temp RENAME TO universal_transaction_matview;
ALTER INDEX utm_id                           RENAME TO universal_transaction_matview_id;
ALTER INDEX utm_date                         RENAME TO universal_transaction_matview_date;
ALTER INDEX utm_fy_id                        RENAME TO universal_transaction_matview_fy_id;
ALTER INDEX utm_type                         RENAME TO universal_transaction_matview_type;
ALTER INDEX utm_action_type                  RENAME TO universal_transaction_matview_action_type;
ALTER INDEX utm_award_id                     RENAME TO universal_transaction_matview_award_id;
ALTER INDEX utm_award_category               RENAME TO universal_transaction_matview_award_category;
ALTER INDEX utm_total_obl                    RENAME TO universal_transaction_matview_total_obl;
ALTER INDEX utm_total_obl_bin                RENAME TO universal_transaction_matview_total_obl_bin;

ALTER INDEX utm_fain                         RENAME TO universal_transaction_matview_fain;
ALTER INDEX utm_fain_gin_trgm                RENAME TO universal_transaction_matview_fain_gin_trgm;
ALTER INDEX utm_piid                         RENAME TO universal_transaction_matview_piid;
ALTER INDEX utm_piid_gin_trgm                RENAME TO universal_transaction_matview_piid_gin_trgm;
ALTER INDEX utm_uri                          RENAME TO universal_transaction_matview_uri;
ALTER INDEX utm_uri_gin_trgm                 RENAME TO universal_transaction_matview_uri_gin_trgm;

ALTER INDEX utm_pop_country_code             RENAME TO universal_transaction_matview_pop_country_code;
ALTER INDEX utm_pop_zip5                     RENAME TO universal_transaction_matview_pop_zip5;
ALTER INDEX utm_pop_county_code              RENAME TO universal_transaction_matview_pop_county_code;
ALTER INDEX utm_pop_state                    RENAME TO universal_transaction_matview_pop_state;
ALTER INDEX utm_pop_congress                 RENAME TO universal_transaction_matview_pop_congress;

ALTER INDEX utm_face_value_loan_guarantee    RENAME TO universal_transaction_matview_face_value_loan_guarantee;
ALTER INDEX utm_original_loan_subsidy_cost   RENAME TO universal_transaction_matview_original_loan_subsidy_cost;

ALTER INDEX utm_naics_code                   RENAME TO universal_transaction_matview_naics_code;
ALTER INDEX utm_naics_desc                   RENAME TO universal_transaction_matview_naics_desc;
ALTER INDEX utm_psc_code                     RENAME TO universal_transaction_matview_psc_code;
ALTER INDEX utm_psc_desc                     RENAME TO universal_transaction_matview_psc_desc;
ALTER INDEX utm_psc_code_0                   RENAME TO universal_transaction_matview_psc_code_0;
ALTER INDEX utm_psc_desc_0                   RENAME TO universal_transaction_matview_psc_desc_0;
ALTER INDEX utm_psc_desc_uppr_tmp            RENAME TO universal_transaction_matview_psc_desc_uppr_idx;
ALTER INDEX utm_naics_desc_uppr_tmp          RENAME TO universal_transaction_matview_naics_desc_uppr_idx;

ALTER INDEX utm_pulled_from                  RENAME TO universal_transaction_matview_pulled_from;
ALTER INDEX utm_type_of_contract_pricing     RENAME TO universal_transaction_matview_type_of_contract_pricing;
ALTER INDEX utm_type_set_aside               RENAME TO universal_transaction_matview_type_set_aside;
ALTER INDEX utm_extent_competed              RENAME TO universal_transaction_matview_extent_competed;
ALTER INDEX utm_cfda_multi                   RENAME TO universal_transaction_matview_cfda_multi;

ALTER INDEX utm_recipient_name_gin_trgm      RENAME TO universal_transaction_matview_recipient_name_gin_trgm;
ALTER INDEX utm_recipient                    RENAME TO universal_transaction_matview_recipient;
ALTER INDEX utm_recipient_uniq               RENAME TO universal_transaction_matview_recipient_uniq;
ALTER INDEX utm_parent_recipient_uniq        RENAME TO universal_transaction_matview_parent_recipient_uniq;

ALTER INDEX utm_rl_country_code              RENAME TO universal_transaction_matview_rl_country_code;
ALTER INDEX utm_rl_zip5                      RENAME TO universal_transaction_matview_rl_zip5;
ALTER INDEX utm_rl_state_code                RENAME TO universal_transaction_matview_rl_state_code;
ALTER INDEX utm_rl_county_code               RENAME TO universal_transaction_matview_rl_county_code;
ALTER INDEX utm_rl_congress                  RENAME TO universal_transaction_matview_rl_congress;

ALTER INDEX utm_awarding_toptier_agency_name RENAME TO universal_transaction_matview_awarding_toptier_agency_name;
ALTER INDEX utm_funding_toptier_agency_name  RENAME TO universal_transaction_matview_funding_toptier_agency_name;
ALTER INDEX utm_awarding_subtier_agency_name RENAME TO universal_transaction_matview_awarding_subtier_agency_name;
ALTER INDEX utm_funding_subtier_agency_name  RENAME TO universal_transaction_matview_funding_subtier_agency_name;

ALTER INDEX utm_awarding_toptier_agency_name_trgm RENAME TO universal_transaction_matview_awarding_toptier_agency_name_trgm;
ALTER INDEX utm_funding_toptier_agency_name_trgm  RENAME TO universal_transaction_matview_funding_toptier_agency_name_trgm;
ALTER INDEX utm_awarding_subtier_agency_name_trgm RENAME TO universal_transaction_matview_awarding_subtier_agency_name_trgm;
ALTER INDEX utm_funding_subtier_agency_name_trgm  RENAME TO universal_transaction_matview_funding_subtier_agency_name_trgm;
