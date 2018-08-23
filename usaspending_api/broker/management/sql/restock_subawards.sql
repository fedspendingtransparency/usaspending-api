-- Create temp table to reload into the subawards table without dropping the destination table

DROP TABLE IF EXISTS public.temporary_restock_subaward;

CREATE TABLE public.temporary_restock_subaward AS (
    SELECT
        to_tsvector(CONCAT(broker_subawards.recipient_name,' ', psc.description,' ', broker_subawards.description)) AS keyword_ts_vector,
        to_tsvector(CONCAT(aw.piid, ' ', aw.fain, ' ', aw.uri, ' ', broker_subawards.subaward_number)) AS award_ts_vector,
        to_tsvector(COALESCE(broker_subawards.recipient_name, '')) AS recipient_name_ts_vector,

        aw.recipient_id,
        cfda.id AS cfda_id,

        latest_transaction.id AS latest_transaction_id,
        latest_transaction.last_modified_date,
        broker_subawards.subaward_number,
        broker_subawards.subaward_amount AS amount,

        obligation_to_enum(broker_subawards.subaward_amount) AS total_obl_bin,
        broker_subawards.description,
        fy(broker_subawards.action_date) AS fiscal_year,
        broker_subawards.action_date,
        broker_subawards.award_report_fy_month,
        broker_subawards.award_report_fy_year,

        broker_subawards.broker_award_id,
        broker_subawards.internal_id,
        broker_subawards.award_type,

        aw.type AS prime_award_type,
        aw.id AS award_id,
        aw.piid,
        aw.fain,

        COALESCE(prime_recipient.business_categories, '{}'::TEXT[]) AS business_categories,
        broker_subawards.recipient_name AS recipient_name,
        UPPER(prime_recipient.recipient_name) AS prime_recipient_name,
        broker_subawards.duns AS recipient_unique_id,
        COALESCE(broker_subawards.parent_recipient_name, (
            SELECT legal_business_name
            FROM recipient_lookup
            WHERE duns = broker_subawards.parent_duns
        )) AS parent_recipient_name,
        broker_subawards.parent_duns AS parent_recipient_unique_id,

        contract_data.pulled_from,
        contract_data.type_of_contract_pricing,
        contract_data.extent_competed,
        contract_data.type_set_aside,
        contract_data.product_or_service_code,
        psc.description AS product_or_service_description,
        assistance_data.cfda_number,
        cfda.program_title AS cfda_title,

        aw.awarding_agency_id,
        aw.funding_agency_id,
        taa.name AS awarding_toptier_agency_name,
        saa.name AS awarding_subtier_agency_name,
        tfa.name AS funding_toptier_agency_name,
        sfa.name AS funding_subtier_agency_name,
        taa.abbreviation AS awarding_toptier_agency_abbreviation,
        tfa.abbreviation AS funding_toptier_agency_abbreviation,
        saa.abbreviation AS awarding_subtier_agency_abbreviation,
        sfa.abbreviation AS funding_subtier_agency_abbreviation,

        (
            SELECT country_name
            FROM ref_country_code
            WHERE country_code = broker_subawards.recipient_location_country_code
        ) AS recipient_location_country_name,
        broker_subawards.recipient_location_country_code,
        broker_subawards.recipient_location_state_code,
        (
            SELECT county_name
            FROM ref_city_county_code
            WHERE
                state_code = broker_subawards.recipient_location_state_code
                AND
                city_name = broker_subawards.recipient_location_city_name
            LIMIT 1
        ) AS recipient_location_county_name,
        (
            SELECT county_code
            FROM ref_city_county_code
            WHERE
                state_code = broker_subawards.recipient_location_state_code
                AND
                city_name = broker_subawards.recipient_location_city_name
            LIMIT 1
        ) AS recipient_location_county_code,
        broker_subawards.recipient_location_city_name,
        (
            SELECT city_code
            FROM ref_city_county_code
            WHERE
                state_code = broker_subawards.recipient_location_state_code
                AND
                city_name = broker_subawards.recipient_location_city_name
            LIMIT 1
        ) AS recipient_location_city_code,
        broker_subawards.recipient_location_zip5,
        broker_subawards.recipient_location_congressional_code,

        (
            SELECT country_name
            FROM ref_country_code
            WHERE country_code = broker_subawards.pop_country_code
        ) AS pop_country_name,
        broker_subawards.pop_country_code,
        broker_subawards.pop_state_code,
        (
            SELECT county_name
            FROM ref_city_county_code
            WHERE
                state_code = broker_subawards.pop_state_code
                AND
                city_name = broker_subawards.pop_city_name
            LIMIT 1
        ) AS pop_county_name,
        (
            SELECT county_code
            FROM ref_city_county_code
            WHERE
                state_code = broker_subawards.pop_state_code
                AND
                city_name = broker_subawards.pop_city_name
            LIMIT 1
        ) AS pop_county_code,
        broker_subawards.pop_city_name,
        (
            SELECT city_code
            FROM ref_city_county_code
            WHERE
                state_code = broker_subawards.pop_state_code
                AND
                city_name = broker_subawards.pop_city_name
            LIMIT 1
        ) AS pop_city_code,
        broker_subawards.pop_zip5,
        broker_subawards.pop_congressional_code

    FROM
        dblink('broker_server', '
            (
            SELECT
                fp.id AS broker_award_id,
                fp.internal_id,
                fp.contract_number AS piid,
                UPPER(''CONT_AW_'' ||
                    COALESCE(fp.contract_agency_code,''-NONE-'') || ''_'' ||
                    COALESCE(fp.contract_idv_agency_code,''-NONE-'') || ''_'' ||
                    COALESCE(fp.contract_number,''-NONE-'') || ''_'' ||
                    COALESCE(fp.idv_reference_number,''-NONE-'')) AS expected_generated_unique_award_id,
                NULL AS fain,
                ''procurement'' AS award_type,
                fsc.subcontract_date AS action_date,
                fp.report_period_mon AS award_report_fy_month,
                fp.report_period_year AS award_report_fy_year,

                NULL AS cfda_number,
                UPPER(fsc.naics) AS naics_code,
                UPPER(fsc.subcontract_num) AS subaward_number,
                fsc.subcontract_amount AS subaward_amount,
                UPPER(fsc.overall_description) AS description,

                fp.contracting_office_aid AS awarding_agency_subtier_code,
                fsc.funding_agency_id AS funding_agency_code,
                UPPER(fsc.funding_agency_name) AS funding_agency_name,

                UPPER(fsc.principle_place_country) AS pop_country_code,
                UPPER(fsc.principle_place_state) AS pop_state_code,
                UPPER(fsc.principle_place_state_name) AS pop_state_name,
                UPPER(fsc.principle_place_city) AS pop_city_name,
                UPPER(fsc.principle_place_district) AS pop_congressional_code,
                UPPER(fsc.principle_place_street) AS pop_street_address,
                UPPER(fsc.principle_place_zip) AS pop_zip5,

                UPPER(fsc.duns) AS duns,
                UPPER(fsc.company_name) AS recipient_name,
                UPPER(fsc.parent_duns) AS parent_duns,
                UPPER(fsc.parent_company_name) AS parent_recipient_name,
                UPPER(fsc.company_address_country) AS recipient_location_country_code,

                UPPER(fsc.company_address_state) AS recipient_location_state_code,
                UPPER(fsc.company_address_state_name) AS recipient_location_state_name,

                UPPER(fsc.company_address_city) AS recipient_location_city_name,

                UPPER(fsc.company_address_district) AS recipient_location_congressional_code,
                UPPER(fsc.company_address_street) AS recipient_location_street_address,
                UPPER(fsc.company_address_zip) AS recipient_location_zip5
            FROM fsrs_subcontract AS fsc
                JOIN
                fsrs_procurement AS fp ON fp.id = fsc.parent_id
            )

            UNION ALL

            (
            SELECT
                fg.id AS broker_award_id,
                fg.internal_id,
                NULL AS piid,
                NULL AS expected_generated_unique_award_id,
                UPPER(fg.fain) AS fain,
                ''grant'' AS award_type,
                fsg.subaward_date AS action_date,
                fg.report_period_mon AS award_report_fy_month,
                fg.report_period_year AS award_report_fy_year,

                UPPER(fsg.cfda_numbers) AS cfda_number,
                NULL AS naics_code,
                UPPER(fsg.subaward_num) AS subaward_number,
                fsg.subaward_amount AS subaward_amount,
                UPPER(fsg.project_description) AS description,

                NULL AS awarding_agency_subtier_code,
                fsg.funding_agency_id AS funding_agency_code,
                UPPER(fsg.funding_agency_name) AS funding_agency_name,

                UPPER(fsg.principle_place_country) AS pop_country_code,
                UPPER(fsg.principle_place_state) AS pop_state_code,
                UPPER(fsg.principle_place_state_name) AS pop_state_name,
                UPPER(fsg.principle_place_city) AS pop_city_name,
                UPPER(fsg.principle_place_district) AS pop_congressional_code,
                UPPER(fsg.principle_place_street) AS pop_street_address,
                UPPER(fsg.principle_place_zip) AS pop_zip5,

                UPPER(fsg.duns) AS duns,
                UPPER(fsg.awardee_name) AS recipient_name,
                UPPER(fsg.parent_duns) AS parent_duns,
                NULL AS parent_recipient_name,
                UPPER(fsg.awardee_address_country) AS recipient_location_country_code,

                UPPER(fsg.awardee_address_state) AS recipient_location_state_code,
                UPPER(fsg.awardee_address_state_name) AS recipient_location_state_name,

                UPPER(fsg.awardee_address_city) AS recipient_location_city_name,

                UPPER(fsg.awardee_address_district) AS recipient_location_congressional_code,

                UPPER(fsg.awardee_address_street) AS recipient_location_street_address,
                UPPER(fsg.awardee_address_zip) AS recipient_location_zip5
            FROM fsrs_subgrant AS fsg
                JOIN
                fsrs_grant AS fg ON fg.id = fsg.parent_id
            )') AS broker_subawards
        (
            broker_award_id INTEGER,
            internal_id TEXT,
            piid TEXT,
            expected_generated_unique_award_id TEXT,
            fain TEXT,
            award_type TEXT,
            action_date DATE,
            award_report_fy_month INTEGER,
            award_report_fy_year INTEGER,
            cfda_number TEXT,
            naics_code TEXT,
            subaward_number TEXT,
            subaward_amount NUMERIC,
            description TEXT,

            awarding_agency_subtier_code TEXT,
            funding_agency_code TEXT,
            funding_agency_name TEXT,

            pop_country_code TEXT,
            pop_state_code TEXT,
            pop_state_name TEXT,
            pop_city_name TEXT,
            pop_congressional_code TEXT,
            pop_street_address TEXT,
            pop_zip5 TEXT,

            duns TEXT,
            recipient_name TEXT,
            parent_duns TEXT,
            parent_recipient_name TEXT,
            recipient_location_country_code TEXT,
            recipient_location_state_code TEXT,
            recipient_location_state_name TEXT,
            recipient_location_city_name TEXT,
            recipient_location_congressional_code TEXT,
            recipient_location_street_address TEXT,
            recipient_location_zip5 TEXT
        )
    INNER JOIN awards AS aw ON (
        (broker_subawards.award_type = 'procurement' AND aw.is_fpds IS TRUE AND REPLACE(aw.generated_unique_award_id, '-', '') = REPLACE(broker_subawards.expected_generated_unique_award_id, '-', ''))
        OR
        (broker_subawards.award_type = 'grant' AND aw.is_fpds IS FALSE AND REPLACE(aw.fain, '-', '') = REPLACE(broker_subawards.fain, '-', ''))
    )
    LEFT OUTER JOIN legal_entity AS prime_recipient ON aw.recipient_id = prime_recipient.legal_entity_id
    LEFT OUTER JOIN agency AS aa ON aw.awarding_agency_id = aa.id
    LEFT OUTER JOIN toptier_agency AS taa ON aa.toptier_agency_id = taa.toptier_agency_id
    LEFT OUTER JOIN subtier_agency AS saa ON aa.subtier_agency_id = saa.subtier_agency_id
    LEFT OUTER JOIN agency AS fa on aw.funding_agency_id = fa.id
    LEFT OUTER JOIN toptier_agency AS tfa ON fa.toptier_agency_id = tfa.toptier_agency_id
    LEFT OUTER JOIN subtier_agency AS sfa ON fa.subtier_agency_id = sfa.subtier_agency_id
    LEFT OUTER JOIN transaction_normalized AS latest_transaction ON aw.latest_transaction_id = latest_transaction.id
    LEFT OUTER JOIN transaction_fabs AS assistance_data ON latest_transaction.id = assistance_data.transaction_id
    LEFT OUTER JOIN transaction_fpds AS contract_data ON latest_transaction.id = contract_data.transaction_id
    LEFT OUTER JOIN psc ON contract_data.product_or_service_code = psc.code
    LEFT OUTER JOIN references_cfda AS cfda ON assistance_data.cfda_number = cfda.program_number
    WHERE broker_subawards.subaward_number IS NOT NULL
);

BEGIN;
TRUNCATE TABLE public.subaward RESTART IDENTITY;
INSERT INTO public.subaward
    (keyword_ts_vector, award_ts_vector, recipient_name_ts_vector, recipient_id, cfda_id,
     latest_transaction_id, last_modified_date, subaward_number, amount, total_obl_bin, description, fiscal_year,
     action_date, award_report_fy_month, award_report_fy_year, broker_award_id, internal_id, award_type,
     prime_award_type, award_id, piid, fain, business_categories, recipient_name, prime_recipient_name,
     recipient_unique_id, parent_recipient_unique_id, pulled_from, type_of_contract_pricing, extent_competed,
     type_set_aside, product_or_service_code, product_or_service_description, cfda_number, cfda_title,
     awarding_agency_id, funding_agency_id, awarding_toptier_agency_name, awarding_subtier_agency_name,
     funding_toptier_agency_name, funding_subtier_agency_name, awarding_toptier_agency_abbreviation,
     funding_toptier_agency_abbreviation, awarding_subtier_agency_abbreviation, funding_subtier_agency_abbreviation,
     recipient_location_country_name, recipient_location_country_code, recipient_location_state_code,
     recipient_location_county_name, recipient_location_county_code, recipient_location_zip5,
     recipient_location_congressional_code, pop_country_name, pop_country_code, pop_state_code, pop_county_name,
     pop_county_code, pop_city_code, pop_zip5, pop_congressional_code, parent_recipient_name)
    SELECT keyword_ts_vector, award_ts_vector, recipient_name_ts_vector, recipient_id, cfda_id,
           latest_transaction_id, last_modified_date, subaward_number, amount, total_obl_bin, description, fiscal_year,
           action_date, award_report_fy_month, award_report_fy_year, broker_award_id, internal_id, award_type,
           prime_award_type, award_id, piid, fain, business_categories, recipient_name, prime_recipient_name,
           recipient_unique_id, parent_recipient_unique_id, pulled_from, type_of_contract_pricing, extent_competed,
           type_set_aside, product_or_service_code, product_or_service_description, cfda_number, cfda_title,
           awarding_agency_id, funding_agency_id, awarding_toptier_agency_name, awarding_subtier_agency_name,
           funding_toptier_agency_name, funding_subtier_agency_name, awarding_toptier_agency_abbreviation,
           funding_toptier_agency_abbreviation, awarding_subtier_agency_abbreviation,
           funding_subtier_agency_abbreviation, recipient_location_country_name, recipient_location_country_code,
           recipient_location_state_code, recipient_location_county_name, recipient_location_county_code,
           recipient_location_zip5, recipient_location_congressional_code, pop_country_name, pop_country_code,
           pop_state_code, pop_county_name, pop_county_code, pop_city_code, pop_zip5, pop_congressional_code,
           parent_recipient_name
    FROM public.temporary_restock_subaward;

WITH subaward_totals AS (
    SELECT award_id, SUM(amount) AS total_subaward_amount, COUNT(*) AS subaward_count
    FROM subaward
    GROUP BY award_id
)
UPDATE awards
SET total_subaward_amount = subaward_totals.total_subaward_amount,
    subaward_count = subaward_totals.subaward_count
FROM subaward_totals
WHERE subaward_totals.award_id = id;

DROP TABLE public.temporary_restock_subaward;
COMMIT;
