-- WARNING: This script is not meant to be run directly. Please use the "load_tas" management command as there are
-- post-sql functions that run to handle federal account relationships
CREATE OR REPLACE FUNCTION generate_tas_rendering_label(
    ata TEXT,
    aid TEXT,
    typecode TEXT,
    bpoa TEXT,
    epoa TEXT,
    mac TEXT,
    sub TEXT
) RETURNS TEXT AS $$
DECLARE
    result TEXT;
BEGIN
    result := CONCAT_WS('-', ata, aid);
    IF typecode IS NOT NULL AND typecode != '' THEN
        result := CONCAT_WS('-', result, typecode);
    ELSE
        result := CONCAT_WS('-', result, CONCAT_WS('/', bpoa, epoa));
    END IF;
    result := CONCAT_WS('-', result, mac, sub);
    RETURN result;
END; $$ LANGUAGE plpgsql;

DROP TABLE IF EXISTS public.temp_restock_tas;

CREATE TABLE public.temp_restock_tas AS (
SELECT
    'USA' AS data_source,
    broker_tas.account_num AS treasury_account_identifier,
    broker_tas.account_title,
    broker_tas.agency_identifier AS agency_id,
    broker_tas.allocation_transfer_agency AS allocation_transfer_agency_id,
    broker_tas.availability_type_code,
    NULL::TEXT AS availability_type_code_description,
    NULL::INT AS awarding_toptier_agency_id,
    broker_tas.beginning_period_of_availa AS beginning_period_of_availability,
    broker_tas.budget_bureau_code,
    broker_tas.budget_bureau_name,
    broker_tas.budget_function_code,
    broker_tas.budget_function_title,
    broker_tas.budget_subfunction_code,
    broker_tas.budget_subfunction_title,
    NOW()::DATE AS create_date,
    NULL::TEXT AS drv_appropriation_account_expired_status,
    NULL::DATE AS drv_appropriation_availability_period_end_date,
    NULL::DATE AS drv_appropriation_availability_period_start_date,
    broker_tas.ending_period_of_availabil AS ending_period_of_availability,
    NULL::INT AS federal_account_id,
    broker_tas.fr_entity_description,
    broker_tas.fr_entity_type AS fr_entity_code,
    NULL::INT AS funding_toptier_agency_id,
    broker_tas.internal_end_date,
    broker_tas.internal_start_date,
    broker_tas.main_account_code,
    broker_tas.reporting_agency_aid AS reporting_agency_id,
    broker_tas.reporting_agency_name,
    broker_tas.sub_account_code,
    generate_tas_rendering_label(
        broker_tas.allocation_transfer_agency,
        broker_tas.agency_identifier,
        broker_tas.availability_type_code,
        broker_tas.beginning_period_of_availa,
        broker_tas.ending_period_of_availabil,
        broker_tas.main_account_code,
        broker_tas.sub_account_code
    ) AS tas_rendering_label,
    NOW()::DATE AS update_date
FROM
    dblink ('broker_server', '(
        SELECT
            tas_lookup.account_num::INT,
            tas_lookup.account_title,
            tas_lookup.agency_identifier,
            tas_lookup.allocation_transfer_agency,
            tas_lookup.availability_type_code,
            tas_lookup.beginning_period_of_availa,
            tas_lookup.budget_bureau_code,
            tas_lookup.budget_bureau_name,
            tas_lookup.budget_function_code,
            tas_lookup.budget_function_title,
            tas_lookup.budget_subfunction_code,
            tas_lookup.budget_subfunction_title,
            tas_lookup.ending_period_of_availabil,
            tas_lookup.financial_indicator2,
            tas_lookup.fr_entity_description,
            tas_lookup.fr_entity_type,
            tas_lookup.internal_end_date,
            tas_lookup.internal_start_date,
            tas_lookup.main_account_code,
            tas_lookup.reporting_agency_aid,
            tas_lookup.reporting_agency_name,
            tas_lookup.sub_account_code
        FROM
            tas_lookup
        WHERE
            UPPER(tas_lookup.financial_indicator2) IS DISTINCT FROM ''F'')') AS broker_tas
        (
            account_num INT,
            account_title TEXT,
            agency_identifier TEXT,
            allocation_transfer_agency TEXT,
            availability_type_code TEXT,
            beginning_period_of_availa TEXT,
            budget_bureau_code TEXT,
            budget_bureau_name TEXT,
            budget_function_code TEXT,
            budget_function_title TEXT,
            budget_subfunction_code TEXT,
            budget_subfunction_title TEXT,
            ending_period_of_availabil TEXT,
            financial_indicator2 TEXT,
            fr_entity_description TEXT,
            fr_entity_type TEXT,
            internal_end_date DATE,
            internal_start_date DATE,
            main_account_code TEXT,
            reporting_agency_aid TEXT,
            reporting_agency_name TEXT,
            sub_account_code TEXT
        )
);

INSERT INTO public.treasury_appropriation_account
(
    data_source,
    treasury_account_identifier,
    account_title,
    agency_id,
    allocation_transfer_agency_id,
    availability_type_code,
    availability_type_code_description,
    awarding_toptier_agency_id,
    beginning_period_of_availability,
    budget_bureau_code,
    budget_bureau_name,
    budget_function_code,
    budget_function_title,
    budget_subfunction_code,
    budget_subfunction_title,
    create_date,
    drv_appropriation_account_expired_status,
    drv_appropriation_availability_period_end_date,
    drv_appropriation_availability_period_start_date,
    ending_period_of_availability,
    federal_account_id,
    fr_entity_description,
    fr_entity_code,
    funding_toptier_agency_id,
    internal_end_date,
    internal_start_date,
    main_account_code,
    reporting_agency_id,
    reporting_agency_name,
    sub_account_code,
    tas_rendering_label,
    update_date
)
SELECT
    data_source,
    treasury_account_identifier,
    account_title,
    agency_id,
    allocation_transfer_agency_id,
    availability_type_code,
    availability_type_code_description,
    awarding_toptier_agency_id,
    beginning_period_of_availability,
    budget_bureau_code,
    budget_bureau_name,
    budget_function_code,
    budget_function_title,
    budget_subfunction_code,
    budget_subfunction_title,
    create_date,
    drv_appropriation_account_expired_status,
    drv_appropriation_availability_period_end_date,
    drv_appropriation_availability_period_start_date,
    ending_period_of_availability,
    federal_account_id,
    fr_entity_description,
    fr_entity_code,
    funding_toptier_agency_id,
    internal_end_date,
    internal_start_date,
    main_account_code,
    reporting_agency_id,
    reporting_agency_name,
    sub_account_code,
    tas_rendering_label,
    update_date
FROM public.temp_restock_tas ON CONFLICT (treasury_account_identifier) DO UPDATE SET
    data_source = excluded.data_source,
    account_title = excluded.account_title,
    agency_id = excluded.agency_id,
    allocation_transfer_agency_id = excluded.allocation_transfer_agency_id,
    availability_type_code = excluded.availability_type_code,
    awarding_toptier_agency_id = excluded.awarding_toptier_agency_id,
    beginning_period_of_availability = excluded.beginning_period_of_availability,
    budget_bureau_code = excluded.budget_bureau_code,
    budget_bureau_name = excluded.budget_bureau_name,
    budget_function_code = excluded.budget_function_code,
    budget_function_title = excluded.budget_function_title,
    budget_subfunction_code = excluded.budget_subfunction_code,
    budget_subfunction_title = excluded.budget_subfunction_title,
    ending_period_of_availability = excluded.ending_period_of_availability,
    federal_account_id = excluded.federal_account_id,
    fr_entity_description = excluded.fr_entity_description,
    fr_entity_code = excluded.fr_entity_code,
    funding_toptier_agency_id = excluded.funding_toptier_agency_id,
    internal_end_date = excluded.internal_end_date,
    internal_start_date = excluded.internal_start_date,
    main_account_code = excluded.main_account_code,
    reporting_agency_id = excluded.reporting_agency_id,
    reporting_agency_name = excluded.reporting_agency_name,
    sub_account_code = excluded.sub_account_code,
    tas_rendering_label = excluded.tas_rendering_label,
    update_date = excluded.update_date
;
DROP TABLE public.temp_restock_tas;
DROP FUNCTION generate_tas_rendering_label (TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT);
