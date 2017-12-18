-- Relies on functions_and_enums.sql

-- Drop the temporary materialized views if they exist
DROP MATERIALIZED VIEW IF EXISTS universal_award_matview_temp;
DROP MATERIALIZED VIEW IF EXISTS universal_award_matview_old;
-- Temp matview
CREATE MATERIALIZED VIEW universal_award_matview_temp AS SELECT
  "awards"."id" AS award_id,
  "awards"."category",
  "awards"."latest_transaction_id",
  "awards"."type",
  "awards"."type_description",
  "awards"."description",
  "awards"."piid",
  "awards"."fain",
  "awards"."uri",
  "awards"."total_obligation",
  obligation_to_enum("awards"."total_obligation") AS total_obl_bin,
  "awards"."period_of_performance_start_date",
  "awards"."period_of_performance_current_end_date",
  "awards"."date_signed",
  "awards"."base_and_all_options_value",

  "awards"."recipient_id",
  recipient."recipient_name",
  recipient."recipient_unique_id",
  recipient."parent_recipient_unique_id",
  "recipient"."business_categories",

  latest_transaction."action_date",
  latest_transaction."fiscal_year",
  -- DUPLICATED ON 12/4. REMOVE `issued_date*` BEFORE JAN 1, 2018!!!!!!!!!!!!!!!
  latest_transaction."action_date" as issued_date,
  latest_transaction."fiscal_year" as issued_date_fiscal_year,
  -- ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  assistance_data."face_value_loan_guarantee",
  assistance_data."original_loan_subsidy_cost",

  TAA."name" AS awarding_toptier_agency_name,
  TFA."name" AS funding_toptier_agency_name,
  SAA."name" AS awarding_subtier_agency_name,
  SFA."name" AS funding_subtier_agency_name,
  AAO."name" AS awarding_agency_office_name,
  FAO."name" AS funding_agency_office_name,

  recipient_location."address_line1" AS recipient_location_address_line1,
  recipient_location."address_line2" AS recipient_location_address_line2,
  recipient_location."address_line3" AS recipient_location_address_line3,
  recipient_location."country_name" AS recipient_location_country_name,
  recipient_location."location_country_code" AS recipient_location_country_code,
  recipient_location."state_code" AS recipient_location_state_code,
  recipient_location."county_name" AS recipient_location_county_name,
  recipient_location."county_code" AS recipient_location_county_code,
  recipient_location."city_name" AS recipient_location_city_name,
  recipient_location."zip5" AS recipient_location_zip5,
  recipient_location."congressional_code" AS recipient_location_congressional_code,
  recipient_location."foreign_province" AS recipient_location_foreign_province,

  place_of_performance."country_name" AS pop_country_name,
  place_of_performance."location_country_code" AS pop_country_code,
  place_of_performance."state_name" AS pop_state_name,
  place_of_performance."state_code" AS pop_state_code,
  place_of_performance."county_name" AS pop_county_name,
  place_of_performance."county_code" AS pop_county_code,
  place_of_performance."city_name" AS pop_city_name,
  place_of_performance."zip5" AS pop_zip5,
  place_of_performance."congressional_code" AS pop_congressional_code,
  place_of_performance."foreign_province" AS pop_foreign_province,

  assistance_data."cfda_number",
  contract_data."pulled_from",
  contract_data."type_of_contract_pricing",
  contract_data."parent_award_id",
  contract_data."idv_type",
  contract_data."extent_competed",
  contract_data."extent_compete_description",
  contract_data."type_set_aside",
  contract_data."type_set_aside_description",
  contract_data."product_or_service_code",
  "psc"."description" AS product_or_service_description,
  contract_data."naics" AS naics_code,
  contract_data."naics_description"

FROM
  "awards"
LEFT OUTER JOIN
  "transaction_normalized" AS latest_transaction
    ON ("awards"."latest_transaction_id" = latest_transaction."id")
LEFT OUTER JOIN
  "transaction_fabs" AS assistance_data
  ON (latest_transaction."id" = assistance_data."transaction_id")
LEFT OUTER JOIN
  "transaction_fpds" AS contract_data
  ON (latest_transaction."id" = contract_data."transaction_id")
LEFT OUTER JOIN
  "legal_entity" AS recipient
   ON ("awards"."recipient_id" = recipient."legal_entity_id")
LEFT OUTER JOIN
  "references_location" AS recipient_location
  ON (recipient."location_id" = recipient_location."location_id")
LEFT OUTER JOIN
  "references_location" AS place_of_performance
  ON ("awards"."place_of_performance_id" = place_of_performance."location_id")
LEFT OUTER JOIN
  "psc" ON (contract_data."product_or_service_code" = "psc"."code")
LEFT OUTER JOIN
  "agency" AS AA
    ON ("awards"."awarding_agency_id" = AA."id")
LEFT OUTER JOIN
  "toptier_agency" AS TAA
    ON (AA."toptier_agency_id" = TAA."toptier_agency_id")
LEFT OUTER JOIN
  "subtier_agency" AS SAA
    ON (AA."subtier_agency_id" = SAA."subtier_agency_id")
LEFT OUTER JOIN
  "office_agency" AS AAO
    ON (AA."office_agency_id" = AAO."office_agency_id")
LEFT OUTER JOIN
  "agency" AS FA ON ("awards"."funding_agency_id" = FA."id")
LEFT OUTER JOIN
  "toptier_agency" AS TFA
    ON (FA."toptier_agency_id" = TFA."toptier_agency_id")
LEFT OUTER JOIN
  "subtier_agency" AS SFA
    ON (FA."subtier_agency_id" = SFA."subtier_agency_id")
LEFT OUTER JOIN
  "office_agency" AS FAO
    ON (FA."office_agency_id" = FAO."office_agency_id")
WHERE
  "awards"."latest_transaction_id" IS NOT NULL AND
  ("awards"."category" IS NOT NULL or ("contract_data"."pulled_from"='IDV')AND
  latest_transaction."action_date" >= '2007-10-01';


-- Temp indexes
CREATE INDEX universal_award_matview_id_tmp                    ON universal_award_matview_temp("award_id");
CREATE INDEX universal_award_matview_category_tmp              ON universal_award_matview_temp("category");
CREATE INDEX universal_award_matview_latest_tmp                ON universal_award_matview_temp("latest_transaction_id");
CREATE INDEX universal_award_matview_type_tmp                  ON universal_award_matview_temp("type")                                  WHERE "type" IS NOT NULL;
CREATE INDEX universal_award_matview_type_desc_tmp             ON universal_award_matview_temp("type"                                   DESC NULLS LAST);
CREATE INDEX universal_award_matview_type_descr_desc_tmp       ON universal_award_matview_temp("type_description"                       DESC NULLS LAST);

CREATE INDEX universal_award_matview_piid_tmp                  ON universal_award_matview_temp("piid")                                  WHERE "piid" IS NOT NULL;
CREATE INDEX universal_award_matview_piid_desc_tmp             ON universal_award_matview_temp(UPPER("piid"                             DESC NULLS LAST));
CREATE INDEX universal_award_matview_piid_gin_trgm_tmp         ON universal_award_matview_temp                                          USING gin (UPPER(piid) gin_trgm_ops);
CREATE INDEX universal_award_matview_fain_tmp                  ON universal_award_matview_temp("fain")                                  WHERE "fain" IS NOT NULL;
CREATE INDEX universal_award_matview_fain_desc_tmp             ON universal_award_matview_temp(UPPER("fain"                             DESC NULLS LAST));
CREATE INDEX universal_award_matview_fain_gin_trgm_tmp         ON universal_award_matview_temp                                          USING gin (UPPER(fain) gin_trgm_ops);
CREATE INDEX universal_award_matview_uri_desc_tmp              ON universal_award_matview_temp("uri"                                    DESC NULLS LAST);
CREATE INDEX universal_award_matview_uri_gin_trgm_tmp          ON universal_award_matview_temp                                          USING gin (UPPER(uri) gin_trgm_ops);

CREATE INDEX universal_award_matview_total_obligation_tmp      ON universal_award_matview_temp("total_obligation")                      WHERE "total_obligation" IS NOT NULL;
CREATE INDEX universal_award_matview_total_obligation_desc_tmp ON universal_award_matview_temp("total_obligation"                       DESC NULLS LAST);
CREATE INDEX universal_award_matview_total_obl_bin_tmp         ON universal_award_matview_temp("total_obl_bin");
CREATE INDEX universal_award_matview_pop_start_desc_tmp        ON universal_award_matview_temp("period_of_performance_start_date"       DESC NULLS LAST);
CREATE INDEX universal_award_matview_pop_end_desc_tmp          ON universal_award_matview_temp("period_of_performance_current_end_date" DESC NULLS LAST);

CREATE INDEX universal_award_matview_recipient_name_gin_trgm_tmp ON universal_award_matview_temp                                        USING gin(UPPER(recipient_name) gin_trgm_ops);
CREATE INDEX universal_award_matview_recipient_name_tmp          ON universal_award_matview_temp("recipient_name")                      WHERE "recipient_name" IS NOT NULL;
CREATE INDEX universal_award_matview_recipient_name_desc_tmp     ON universal_award_matview_temp("recipient_name"                       DESC NULLS LAST);
CREATE INDEX universal_award_matview_recipient_unique_tmp        ON universal_award_matview_temp("recipient_unique_id")                 WHERE "recipient_unique_id" IS NOT NULL;
CREATE INDEX universal_award_matview_parent_recipient_tmp        ON universal_award_matview_temp("parent_recipient_unique_id")          WHERE "parent_recipient_unique_id" IS NOT NULL;

CREATE INDEX universal_award_matview_issued_date_tmp  ON universal_award_matview_temp("issued_date");
CREATE INDEX universal_award_matview_issued_fy_tmp    ON universal_award_matview_temp("issued_date_fiscal_year");
CREATE INDEX universal_award_matview_date_tmp         ON universal_award_matview_temp("action_date");
CREATE INDEX universal_award_matview_fy_tmp           ON universal_award_matview_temp("fiscal_year");

CREATE INDEX universal_award_matview_subtier_agency_desc_tmp ON universal_award_matview_temp("awarding_subtier_agency_name" DESC);
CREATE INDEX universal_award_matview_toptier_agency_desc_tmp ON universal_award_matview_temp("awarding_toptier_agency_name" DESC);

CREATE INDEX universal_award_matview_rl_country_code_tmp ON universal_award_matview_temp("recipient_location_country_code")       WHERE "recipient_location_country_code" IS NOT NULL;
CREATE INDEX universal_award_matview_rl_state_code_tmp   ON universal_award_matview_temp("recipient_location_state_code")         WHERE "recipient_location_state_code" IS NOT NULL;
CREATE INDEX universal_award_matview_rl_county_code_tmp  ON universal_award_matview_temp("recipient_location_county_code")        WHERE "recipient_location_county_code" IS NOT NULL;
CREATE INDEX universal_award_matview_rl_zip5_tmp         ON universal_award_matview_temp("recipient_location_zip5")               WHERE "recipient_location_zip5" IS NOT NULL;
CREATE INDEX universal_award_matview_rl_congress_tmp     ON universal_award_matview_temp("recipient_location_congressional_code") WHERE "recipient_location_congressional_code" IS NOT NULL;

CREATE INDEX universal_award_matview_pop_country_code_tmp ON universal_award_matview_temp("pop_country_code")       WHERE "pop_country_code" IS NOT NULL;
CREATE INDEX universal_award_matview_pop_state_tmp        ON universal_award_matview_temp("pop_state_code")         WHERE "pop_state_code" IS NOT NULL;
CREATE INDEX universal_award_matview_pop_county_code_tmp  ON universal_award_matview_temp("pop_county_code")        WHERE "pop_county_code" IS NOT NULL;
CREATE INDEX universal_award_matview_pop_zip5_tmp         ON universal_award_matview_temp("pop_zip5")               WHERE "pop_zip5" IS NOT NULL;
CREATE INDEX universal_award_matview_pop_congress_tmp     ON universal_award_matview_temp("pop_congressional_code") WHERE "pop_congressional_code" IS NOT NULL;

CREATE INDEX universal_award_matview_cfda_tmp                     ON universal_award_matview_temp("cfda_number")              WHERE "cfda_number" IS NOT NULL;
CREATE INDEX universal_award_matview_pulled_from_tmp              ON universal_award_matview_temp("pulled_from")              WHERE "pulled_from" IS NOT NULL;
CREATE INDEX universal_award_matview_type_of_contract_pricing_tmp ON universal_award_matview_temp("type_of_contract_pricing") WHERE "type_of_contract_pricing" IS NOT NULL;
CREATE INDEX universal_award_matview_extent_competed_tmp          ON universal_award_matview_temp("extent_competed")          WHERE "extent_competed" IS NOT NULL;
CREATE INDEX universal_award_matview_type_set_aside_tmp           ON universal_award_matview_temp("type_set_aside")           WHERE "type_set_aside" IS NOT NULL;
CREATE INDEX universal_award_matview_psc_tmp                      ON universal_award_matview_temp("product_or_service_code")  WHERE "product_or_service_code" IS NOT NULL;
CREATE INDEX universal_award_matview_psc_desc_uppr_tmp            ON universal_award_matview_temp                             USING gin(UPPER("product_or_service_description") gin_trgm_ops);
CREATE INDEX universal_award_matview_naics_tmp                    ON universal_award_matview_temp                             USING gin("naics_code"  gin_trgm_ops);
CREATE INDEX universal_award_matview_naics_desc_uppr_tmp          ON universal_award_matview_temp                             USING gin(UPPER("naics_description") gin_trgm_ops);

-- Rename old matview/indexes
ALTER MATERIALIZED VIEW IF EXISTS universal_award_matview RENAME TO universal_award_matview_old;
ALTER INDEX IF EXISTS universal_award_matview_id_idx                    RENAME TO universal_award_matview_id_old;
ALTER INDEX IF EXISTS universal_award_matview_category_idx              RENAME TO universal_award_matview_category_old;
ALTER INDEX IF EXISTS universal_award_matview_latest_idx                RENAME TO universal_award_matview_latest_old;
ALTER INDEX IF EXISTS universal_award_matview_type_idx                  RENAME TO universal_award_matview_type_old;
ALTER INDEX IF EXISTS universal_award_matview_type_desc_idx             RENAME TO universal_award_matview_type_desc_old;
ALTER INDEX IF EXISTS universal_award_matview_type_descr_desc_idx       RENAME TO universal_award_matview_type_descr_desc_old;

ALTER INDEX IF EXISTS universal_award_matview_piid_idx                  RENAME TO universal_award_matview_piid_old;
ALTER INDEX IF EXISTS universal_award_matview_piid_desc_idx             RENAME TO universal_award_matview_piid_desc_old;
ALTER INDEX IF EXISTS universal_award_matview_piid_gin_trgm_idx         RENAME TO universal_award_matview_piid_gin_trgm_old;
ALTER INDEX IF EXISTS universal_award_matview_fain_idx                  RENAME TO universal_award_matview_fain_old;
ALTER INDEX IF EXISTS universal_award_matview_fain_desc_idx             RENAME TO universal_award_matview_fain_desc_old;
ALTER INDEX IF EXISTS universal_award_matview_fain_gin_trgm_idx         RENAME TO universal_award_matview_fain_gin_trgm_old;
ALTER INDEX IF EXISTS universal_award_matview_uri_desc_idx              RENAME TO universal_award_matview_uri_desc_old;
ALTER INDEX IF EXISTS universal_award_matview_uri_gin_trgm_idx          RENAME TO universal_award_matview_uri_gin_trgm_old;

ALTER INDEX IF EXISTS universal_award_matview_total_obligation_idx      RENAME TO universal_award_matview_total_obligation_old;
ALTER INDEX IF EXISTS universal_award_matview_total_obligation_desc_idx RENAME TO universal_award_matview_total_obligation_desc_old;
ALTER INDEX IF EXISTS universal_award_matview_total_obl_bin_idx         RENAME TO universal_award_matview_total_obl_bin_old;
ALTER INDEX IF EXISTS universal_award_matview_pop_start_desc_idx        RENAME TO universal_award_matview_pop_start_desc_old;
ALTER INDEX IF EXISTS universal_award_matview_pop_end_desc_idx          RENAME TO universal_award_matview_pop_end_desc_old;

ALTER INDEX IF EXISTS universal_award_matview_recipient_name_gin_trgm_idx RENAME TO universal_award_matview_recipient_name_gin_trgm_old;
ALTER INDEX IF EXISTS universal_award_matview_recipient_name_idx          RENAME TO universal_award_matview_recipient_name_old;
ALTER INDEX IF EXISTS universal_award_matview_recipient_name_desc_idx     RENAME TO universal_award_matview_recipient_name_desc_old;
ALTER INDEX IF EXISTS universal_award_matview_recipient_unique_idx        RENAME TO universal_award_matview_recipient_unique_old;
ALTER INDEX IF EXISTS universal_award_matview_parent_recipient_idx        RENAME TO universal_award_matview_parent_recipient_old;

ALTER INDEX IF EXISTS universal_award_matview_issued_date_idx   RENAME TO universal_award_matview_issued_date_old;
ALTER INDEX IF EXISTS universal_award_matview_issued_fy_idx     RENAME TO universal_award_matview_issued_fy_old;
ALTER INDEX IF EXISTS universal_award_matview_date_idx          RENAME TO universal_award_matview_date_old;
ALTER INDEX IF EXISTS universal_award_matview_fy_idx            RENAME TO universal_award_matview_fy_old;

ALTER INDEX IF EXISTS universal_award_matview_subtier_agency_desc_idx RENAME TO universal_award_matview_subtier_agency_desc_old;
ALTER INDEX IF EXISTS universal_award_matview_toptier_agency_desc_idx RENAME TO universal_award_matview_toptier_agency_desc_old;

ALTER INDEX IF EXISTS universal_award_matview_rl_country_code RENAME TO universal_award_matview_rl_country_code_old;
ALTER INDEX IF EXISTS universal_award_matview_rl_state_code   RENAME TO universal_award_matview_rl_state_code_old;
ALTER INDEX IF EXISTS universal_award_matview_rl_county_code  RENAME TO universal_award_matview_rl_county_code_old;
ALTER INDEX IF EXISTS universal_award_matview_rl_zip5         RENAME TO universal_award_matview_rl_zip5_old;
ALTER INDEX IF EXISTS universal_award_matview_rl_congress     RENAME TO universal_award_matview_rl_congress_old;

ALTER INDEX IF EXISTS universal_award_matview_pop_country_code RENAME TO universal_award_matview_pop_country_code_old;
ALTER INDEX IF EXISTS universal_award_matview_pop_state        RENAME TO universal_award_matview_pop_state_old;
ALTER INDEX IF EXISTS universal_award_matview_pop_county_code  RENAME TO universal_award_matview_pop_county_code_old;
ALTER INDEX IF EXISTS universal_award_matview_pop_zip5         RENAME TO universal_award_matview_pop_zip5_old;
ALTER INDEX IF EXISTS universal_award_matview_pop_congress     RENAME TO universal_award_matview_pop_congress_old;

ALTER INDEX IF EXISTS universal_award_matview_cfda_idx                     RENAME TO universal_award_matview_cfda_old;
ALTER INDEX IF EXISTS universal_award_matview_pulled_from_idx              RENAME TO universal_award_matview_pulled_from_old;
ALTER INDEX IF EXISTS universal_award_matview_type_of_contract_pricing_idx RENAME TO universal_award_matview_type_of_contract_pricing_old;
ALTER INDEX IF EXISTS universal_award_matview_extent_competed_idx          RENAME TO universal_award_matview_extent_competed_old;
ALTER INDEX IF EXISTS universal_award_matview_type_set_aside_idx           RENAME TO universal_award_matview_type_set_aside_old;
ALTER INDEX IF EXISTS universal_award_matview_psc_idx                      RENAME TO universal_award_matview_psc_old;
ALTER INDEX IF EXISTS universal_award_matview_naics_idx                    RENAME TO universal_award_matview_naics_old;
ALTER INDEX IF EXISTS universal_award_matview_psc_desc_uppr_idx            RENAME TO universal_award_matview_psc_desc_uppr_old;
ALTER INDEX IF EXISTS universal_award_matview_naics_desc_uppr_idx          RENAME TO universal_award_matview_naics_desc_uppr_old;

-- Rename temp matview/indexes
ALTER MATERIALIZED VIEW universal_award_matview_temp          RENAME TO universal_award_matview;
ALTER INDEX universal_award_matview_id_tmp                    RENAME TO universal_award_matview_id_idx;
ALTER INDEX universal_award_matview_category_tmp              RENAME TO universal_award_matview_category_idx;
ALTER INDEX universal_award_matview_latest_tmp                RENAME TO universal_award_matview_latest_idx;
ALTER INDEX universal_award_matview_type_tmp                  RENAME TO universal_award_matview_type_idx;
ALTER INDEX universal_award_matview_type_desc_tmp             RENAME TO universal_award_matview_type_desc_idx;
ALTER INDEX universal_award_matview_type_descr_desc_tmp       RENAME TO universal_award_matview_type_descr_desc_idx;

ALTER INDEX universal_award_matview_piid_tmp                  RENAME TO universal_award_matview_piid_idx;
ALTER INDEX universal_award_matview_piid_desc_tmp             RENAME TO universal_award_matview_piid_desc_idx;
ALTER INDEX universal_award_matview_piid_gin_trgm_tmp         RENAME TO universal_award_matview_piid_gin_trgm_idx;
ALTER INDEX universal_award_matview_fain_tmp                  RENAME TO universal_award_matview_fain_idx;
ALTER INDEX universal_award_matview_fain_desc_tmp             RENAME TO universal_award_matview_fain_desc_idx;
ALTER INDEX universal_award_matview_fain_gin_trgm_tmp         RENAME TO universal_award_matview_fain_gin_trgm_idx;
ALTER INDEX universal_award_matview_uri_desc_tmp              RENAME TO universal_award_matview_uri_desc_idx;
ALTER INDEX universal_award_matview_uri_gin_trgm_tmp          RENAME TO universal_award_matview_uri_gin_trgm_idx;

ALTER INDEX universal_award_matview_total_obligation_tmp      RENAME TO universal_award_matview_total_obligation_idx;
ALTER INDEX universal_award_matview_total_obligation_desc_tmp RENAME TO universal_award_matview_total_obligation_desc_idx;
ALTER INDEX universal_award_matview_total_obl_bin_tmp         RENAME TO universal_award_matview_total_obl_bin_idx;
ALTER INDEX universal_award_matview_pop_start_desc_tmp        RENAME TO universal_award_matview_pop_start_desc_idx;
ALTER INDEX universal_award_matview_pop_end_desc_tmp          RENAME TO universal_award_matview_pop_end_desc_idx;

ALTER INDEX universal_award_matview_recipient_name_gin_trgm_tmp RENAME TO universal_award_matview_recipient_name_gin_trgm_idx;
ALTER INDEX universal_award_matview_recipient_name_tmp          RENAME TO universal_award_matview_recipient_name_idx;
ALTER INDEX universal_award_matview_recipient_name_desc_tmp     RENAME TO universal_award_matview_recipient_name_desc_idx;
ALTER INDEX universal_award_matview_recipient_unique_tmp        RENAME TO universal_award_matview_recipient_unique_idx;
ALTER INDEX universal_award_matview_parent_recipient_tmp        RENAME TO universal_award_matview_parent_recipient_idx;

ALTER INDEX universal_award_matview_issued_date_tmp   RENAME TO universal_award_matview_issued_date_idx;
ALTER INDEX universal_award_matview_issued_fy_tmp     RENAME TO universal_award_matview_issued_fy_idx;
ALTER INDEX universal_award_matview_date_tmp          RENAME TO universal_award_matview_date_idx;
ALTER INDEX universal_award_matview_fy_tmp            RENAME TO universal_award_matview_fy_idx;

ALTER INDEX universal_award_matview_subtier_agency_desc_tmp RENAME TO universal_award_matview_subtier_agency_desc_idx;
ALTER INDEX universal_award_matview_toptier_agency_desc_tmp RENAME TO universal_award_matview_toptier_agency_desc_idx;

ALTER INDEX universal_award_matview_rl_country_code_tmp RENAME TO universal_award_matview_rl_country_code_old;
ALTER INDEX universal_award_matview_rl_state_code_tmp   RENAME TO universal_award_matview_rl_state_code_old;
ALTER INDEX universal_award_matview_rl_county_code_tmp  RENAME TO universal_award_matview_rl_county_code_old;
ALTER INDEX universal_award_matview_rl_zip5_tmp         RENAME TO universal_award_matview_rl_zip5_old;
ALTER INDEX universal_award_matview_rl_congress_tmp     RENAME TO universal_award_matview_rl_congress_old;

ALTER INDEX universal_award_matview_pop_country_code_tmp RENAME TO universal_award_matview_pop_country_code_old;
ALTER INDEX universal_award_matview_pop_state_tmp        RENAME TO universal_award_matview_pop_state_old;
ALTER INDEX universal_award_matview_pop_county_code_tmp  RENAME TO universal_award_matview_pop_county_code_old;
ALTER INDEX universal_award_matview_pop_zip5_tmp         RENAME TO universal_award_matview_pop_zip5_old;
ALTER INDEX universal_award_matview_pop_congress_tmp     RENAME TO universal_award_matview_pop_congress_old;

ALTER INDEX universal_award_matview_cfda_tmp                     RENAME TO universal_award_matview_cfda_idx;
ALTER INDEX universal_award_matview_pulled_from_tmp              RENAME TO universal_award_matview_pulled_from_idx;
ALTER INDEX universal_award_matview_type_of_contract_pricing_tmp RENAME TO universal_award_matview_type_of_contract_pricing_idx;
ALTER INDEX universal_award_matview_extent_competed_tmp          RENAME TO universal_award_matview_extent_competed_idx;
ALTER INDEX universal_award_matview_type_set_aside_tmp           RENAME TO universal_award_matview_type_set_aside_idx;
ALTER INDEX universal_award_matview_psc_tmp                      RENAME TO universal_award_matview_psc_idx;
ALTER INDEX universal_award_matview_naics_tmp                    RENAME TO universal_award_matview_naics_idx;
ALTER INDEX universal_award_matview_psc_desc_uppr_tmp            RENAME TO universal_award_matview_psc_desc_uppr_idx;
ALTER INDEX universal_award_matview_naics_desc_uppr_tmp          RENAME TO universal_award_matview_naics_desc_uppr_idx;
