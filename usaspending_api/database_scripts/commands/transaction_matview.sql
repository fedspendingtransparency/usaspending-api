drop materialized view if exists transaction_matview;

create materialized view transaction_matview as
(
select
    *,
    fy(action_date) as fiscal_year
from
    dblink ('broker_server', '(select
            -- unique ids + cols used for unique id
            detached_award_proc_unique,
            null as afa_generated_unique,
            piid,
            parent_award_id,
            null as fain,
            null as uri,
            agency_id,
            referenced_idv_agency_iden,
            award_modification_amendme,
            transaction_number,

            -- duns
            awardee_or_recipient_uniqu,
            awardee_or_recipient_legal,

            -- recipient
            legal_entity_address_line1,
            legal_entity_address_line2,
            legal_entity_address_line3,
            null as legal_entity_foreign_provi,
            legal_entity_country_code,
            legal_entity_country_name,
            legal_entity_state_code,
            legal_entity_state_descrip as legal_entity_state_name,
            null as legal_entity_county_code,
            null as legal_entity_county_name,
            legal_entity_city_name,
            null as legal_entity_zip5,
            legal_entity_congressional,

            -- place of performance
            place_of_perform_country_c as place_of_perform_country_code,
            place_of_perf_country_desc as place_of_perform_country_name,
            place_of_performance_state as place_of_perform_state_code,
            place_of_perfor_state_desc as place_of_perform_state_name,
            null as place_of_perform_county_code,
            place_of_perform_county_na as place_of_perform_county_name,
            place_of_perform_city_name,
            null as place_of_performance_zip5,
            place_of_performance_congr,

            -- other (fpds specific)
            awarding_sub_tier_agency_c,
            funding_sub_tier_agency_co,
            contract_award_type,
            contract_award_type_desc,
            referenced_idv_type,
            referenced_idv_type_desc,
            federal_action_obligation,
            action_date,
            award_description,
            period_of_performance_star,
            period_of_performance_curr,
            base_and_all_options_value,
            last_modified::date as last_modified_date,
            awarding_office_code,
            awarding_office_name,
            funding_office_code,
            funding_office_name,
            pulled_from,
            product_or_service_code,
            product_or_service_co_desc,
            extent_competed,
            extent_compete_description,
            type_of_contract_pricing,
            naics,
            naics_description,
            idv_type,
            idv_type_description,
            type_set_aside,
            type_set_aside_description,

            -- other (fabs specific)
            null as assistance_type,
            null as original_loan_subsidy_cost,
            null as record_type,
            null as business_funds_indicator,
            null as business_types,
            null as cfda_number,
            null as cfda_title
        from detached_award_procurement)

        union all

        (select
            -- unique ids + cols used for unique id
            null as detached_award_proc_unique,
            afa_generated_unique,
            null as piid,
            null as parent_award_id,
            fain,
            uri,
            null as agency_id,
            null as referenced_idv_agency_iden,
            award_modification_amendme,
            null as transaction_number,

            -- duns
            awardee_or_recipient_uniqu,
            awardee_or_recipient_legal,

            -- recipient
            legal_entity_address_line1,
            legal_entity_address_line2,
            legal_entity_address_line3,
            legal_entity_foreign_provi,
            legal_entity_country_code,
            legal_entity_country_name,
            legal_entity_state_code,
            legal_entity_state_name,
            legal_entity_county_code,
            legal_entity_county_name,
            legal_entity_city_name,
            null as legal_entity_zip5,
            legal_entity_congressional,

            -- place of performance
            place_of_perform_country_c as place_of_perform_country_code,
            place_of_perform_country_n as place_of_perform_country_name,
            null as place_of_perform_state_code,
            place_of_perform_state_nam as place_of_perform_state_name,
            place_of_perform_county_co as place_of_perform_county_code,
            place_of_perform_county_na as place_of_perform_county_name,
            place_of_performance_city as place_of_perform_city_name,
            null as place_of_performance_zip5,
            place_of_performance_congr,

            -- other (fpds specific)
            awarding_sub_tier_agency_c,
            funding_sub_tier_agency_co,
            null as contract_award_type,
            null as contract_award_type_desc,
            null as referenced_idv_type,
            null as referenced_idv_type_desc,
            federal_action_obligation,
            action_date,
            award_description,
            period_of_performance_star,
            period_of_performance_curr,
            null as base_and_all_options_value,
            modified_at::date as last_modified_date,
            awarding_office_code,
            awarding_office_name,
            funding_office_code,
            funding_office_name,
            null as pulled_from,
            null as product_or_service_code,
            null as product_or_service_co_desc,
            null as extent_competed,
            null as extent_compete_description,
            null as type_of_contract_pricing,
            null as naics,
            null as naics_description,
            null as idv_type,
            null as idv_type_description,
            null as type_set_aside,
            null as type_set_aside_description,

            -- other (fabs specific)
            assistance_type,
            original_loan_subsidy_cost,
            record_type,
            business_funds_indicator,
            business_types,
            cfda_number,
            cfda_title
        from published_award_financial_assistance
        where is_active=TRUE)') as transaction
        (
            -- unique ids + cols used for unique id
            detached_award_proc_unique text,
            afa_generated_unique text,
            piid text,
            parent_award_id text,
            fain text,
            uri text,
            agency_id text,
            referenced_idv_agency_iden text,
            award_modification_amendme text,
            transaction_number int,

            -- duns
            awardee_or_recipient_uniqu text,
            awardee_or_recipient_legal text,

            -- recipient
            legal_entity_address_line1 text,
            legal_entity_address_line2 text,
            legal_entity_address_line3 text,
            legal_entity_foreign_provi text,
            legal_entity_country_code text,
            legal_entity_country_name text,
            legal_entity_state_code text,
            legal_entity_state_name text,
            legal_entity_county_code text,
            legal_entity_county_name text,
            legal_entity_city_name text,
            legal_entity_zip5 text,
            legal_entity_congressional text,

            -- place of performance
            place_of_perform_country_code text,
            place_of_perform_country_name text,
            place_of_perform_state_code text,
            place_of_perform_state_name text,
            place_of_perform_county_code text,
            place_of_perform_county_name text,
            place_of_perform_city_name text,
            place_of_performance_zip5 text,
            place_of_performance_congr text,

            -- other (fpds specific)
            awarding_sub_tier_agency_c text,
            funding_sub_tier_agency_co text,
            contract_award_type text,
            contract_award_type_desc text,
            referenced_idv_type text,
            referenced_idv_type_desc text,
            federal_action_obligation float(2),
            action_date date,
            award_description text,
            period_of_performance_star date,
            period_of_performance_curr date,
            base_and_all_options_value float(2),
            last_modified_date date,
            awarding_office_code text,
            awarding_office_name text,
            funding_office_code text,
            funding_office_name text,
            pulled_from text,
            product_or_service_code text,
            product_or_service_co_desc text,
            extent_competed text,
            extent_compete_description text,
            type_of_contract_pricing text,
            naics text,
            naics_description text,
            idv_type text,
            idv_type_description text,
            type_set_aside text,
            type_set_aside_description text,

            -- other (fabs specific)
            assistance_type text,
            original_loan_subsidy_cost float(2),
            record_type int,
            business_funds_indicator text,
            business_types text,
            cfda_number text,
            cfda_title text
        )
);