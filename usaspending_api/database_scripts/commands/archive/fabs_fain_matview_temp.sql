-- with agency_lookup as (     
--     select
--         agency.id as agency_id,
--         subtier_agency.subtier_code as subtier_code     
--     from
--         agency         
--     inner join
--         subtier_agency on subtier_agency.subtier_agency_id=agency.subtier_agency_id
-- ),
-- exec_comp_lookup as (
--     select
--         distinct(legal_entity.recipient_unique_id) as duns,
--         exec_comp.officer_1_name as officer_1_name,
--         exec_comp.officer_1_amount as officer_1_amount,
--         exec_comp.officer_2_name as officer_2_name,
--         exec_comp.officer_2_amount as officer_2_amount,
--         exec_comp.officer_3_name as officer_3_name,
--         exec_comp.officer_3_amount as officer_3_amount,
--         exec_comp.officer_4_name as officer_4_name,
--         exec_comp.officer_4_amount as officer_4_amount,
--         exec_comp.officer_5_name as officer_5_name,
--         exec_comp.officer_5_amount as officer_5_amount
--     from
--         references_legalentityofficers as exec_comp
--         inner join
--         legal_entity on legal_entity.legal_entity_id = exec_comp.legal_entity_id
--     where
--         exec_comp.officer_1_name is not null or
--         exec_comp.officer_1_amount is not null or
--         exec_comp.officer_2_name is not null or
--         exec_comp.officer_2_amount is not null or
--         exec_comp.officer_3_name is not null or
--         exec_comp.officer_3_amount is not null or
--         exec_comp.officer_4_name is not null or
--         exec_comp.officer_4_amount is not null or
--         exec_comp.officer_5_name is not null or
--         exec_comp.officer_5_amount is not null
-- )
create materialized view fabs_fain_matview_temp2 as
(
select
    'asst_aw_' ||
        coalesce(tf.awarding_sub_tier_agency_c,'-none-') || '_' ||
        coalesce(tf.fain, '-none-') || '_' ||
        '-none-' as generated_unique_award_id,
    tf.assistance_type as type,
    case
        when tf.assistance_type = '02' then 'Block Grant'
        when tf.assistance_type = '03' then 'Formula Grant'
        when tf.assistance_type = '04' then 'Project Grant'
        when tf.assistance_type = '05' then 'Cooperative Agreement'
        when tf.assistance_type = '06' then 'Direct Payment for Specified Use'
        when tf.assistance_type = '07' then 'Direct Loan'
        when tf.assistance_type = '08' then 'Guaranteed/Insured Loan'
        when tf.assistance_type = '09' then 'Insurance'
        when tf.assistance_type = '10' then 'Direct Payment with Unrestricted Use'
        when tf.assistance_type = '11' then 'Other Financial Assistance'
    end as type_description,
    -- ac.type_name as category,
    null::text as piid,
    tf.fain as fain,
    null::text as uri,
    uniq_award.total_obligation as total_obligation,
    null::float as total_outlay,
    -- awarding_agency.agency_id as awarding_agency_id,
    awarding_agency.id as awarding_agency_id,
    tf.awarding_sub_tier_agency_c as awarding_sub_tier_agency_c,
    -- funding_agency.agency_id as funding_agency_id,
    'DBR'::text as data_source,
    uniq_award.signed_date as date_signed,
    tf.award_description as description,
    uniq_award.period_of_performance_start_date as period_of_performance_start_date,
    uniq_award.period_of_performance_current_end_date as period_of_performance_current_end_date,
    null::float as potential_total_value_of_award,
    null::float as base_and_all_options_value,
    tf.modified_at as last_modified_date,   
    uniq_award.certified_date as certified_date,
    tf.transaction_id as latest_transaction_id,
    tf.record_type as record_type,
    'asst_tx_' || tf.afa_generated_unique as latest_transaction_unique,
    0 as total_subaward_amount,
    0 as subaward_count,
    
    -- recipient data
    tf.awardee_or_recipient_uniqu as recipient_unique_id,
    tf.awardee_or_recipient_legal as recipient_name,

    -- executive compensation data
    -- exec_comp.officer_1_name as officer_1_name,
    -- exec_comp.officer_1_amount as officer_1_amount,
    -- exec_comp.officer_2_name as officer_2_name,
    -- exec_comp.officer_2_amount as officer_2_amount,
    -- exec_comp.officer_3_name as officer_3_name,
    -- exec_comp.officer_3_amount as officer_3_amount,
    -- exec_comp.officer_4_name as officer_4_name,
    -- exec_comp.officer_4_amount as officer_4_amount,
    -- exec_comp.officer_5_name as officer_5_name,
    -- exec_comp.officer_5_amount as officer_5_amount,

    -- business categories
    tf.legal_entity_address_line1 as recipient_location_address_line1,
    tf.legal_entity_address_line2 as recipient_location_address_line2,
    tf.legal_entity_address_line3 as recipient_location_address_line3,
    
    -- foreign province
    tf.legal_entity_foreign_provi as recipient_location_foreign_province,
    
    -- country
    case
        when tf.legal_entity_country_code is null then (select country_code from ref_country_code where upper(country_name)=upper(tf.legal_entity_country_name))
        when UPPER(tf.legal_entity_country_code)='UNITED STATES' then 'USA'
        else tf.legal_entity_country_code
    end as recipient_location_country_code,
    case
        when tf.legal_entity_country_name is null then (select country_name from ref_country_code where upper(country_code)=upper(tf.legal_entity_country_code))
        else tf.legal_entity_country_name
    end as recipient_location_country_name,
    
    -- state
    case
        when tf.legal_entity_state_code is null then (select code from state_lookup where upper(name)=upper(tf.legal_entity_state_name))
        else tf.legal_entity_state_code
    end as recipient_location_state_code,
    case
        when tf.legal_entity_state_name is null then (select name from state_lookup where upper(code)=upper(tf.legal_entity_state_code))
        else tf.legal_entity_state_name
    end as recipient_location_state_name,
    
    -- county
    tf.legal_entity_county_code as recipient_location_county_code,
    tf.legal_entity_county_name as recipient_location_county_name,
    
    -- city
    tf.legal_entity_city_name as recipient_location_city_name,
    
    -- zip
    tf.legal_entity_zip5 as recipient_location_zip5,
--  tf.legal_entity_zip_last4 as recipient_location_zip_last4,
    
    -- congressional disctrict
    tf.legal_entity_congressional as recipient_location_congressional_code,
    
    -- ppop data
    
    -- foreign
    null::text as pop_foreign_province,
    
    -- country
    case
        when UPPER(place_of_performance_code)='00FORGN' and place_of_perform_country_c is null then 'USA'
        when tf.place_of_perform_country_c is null then (select country_code from ref_country_code where upper(country_name)=upper(tf.place_of_perform_country_n))
        when UPPER(tf.place_of_perform_country_c)='UNITED STATES' then 'USA'
        else tf.place_of_perform_country_c
    end as pop_country_code,
    case
        when tf.place_of_perform_country_n is null then (select country_name from ref_country_code where upper(country_code)=upper(tf.place_of_perform_country_c))
        else tf.place_of_perform_country_n
    end as pop_country_name,
    
    -- state
    case
        when tf.place_of_perform_state_nam is not null then (select code from state_lookup where upper(name)=upper(tf.place_of_perform_state_nam))
        else null
    end as pop_state_code,
    tf.place_of_perform_state_nam as pop_state_name,
    
    -- county
    tf.place_of_perform_county_co as pop_county_code,
    tf.place_of_perform_county_na as pop_county_name,
    
    -- city
    tf.place_of_performance_city as pop_city_name,
    
    -- zip
    (substring(tf.place_of_performance_zip4a from '^(\d{5})\-?(\d{4})?$')) as pop_zip5,
    tf.place_of_performance_zip4a as pop_zip4,
    
    -- congressional disctrict
    tf.place_of_performance_congr as pop_congressional_code
from
    transaction_fabs as tf -- aka latest transaction
    inner join 
    (
        select
            distinct on (transaction_fabs.fain, transaction_fabs.awarding_sub_tier_agency_c)
            transaction_fabs.fain,
            transaction_fabs.awarding_sub_tier_agency_c,
            transaction_fabs.action_date,
            transaction_fabs.award_modification_amendme,
            transaction_fabs.afa_generated_unique,
            count(transaction_fabs.fain) over w as sumfain,
            max(transaction_fabs.action_date) over w as certified_date,
            min(transaction_fabs.action_date) over w as signed_date,
            min(transaction_fabs.period_of_performance_star::date) over w as period_of_performance_start_date,
            max(transaction_fabs.period_of_performance_curr::date) over w as period_of_performance_current_end_date,
            null as base_and_all_options_value,
            sum(coalesce(transaction_fabs.federal_action_obligation::double precision, 0::double precision)) over w as total_obligation
        from transaction_fabs
        where transaction_fabs.record_type = '2'
        window w as (partition by transaction_fabs.fain, transaction_fabs.awarding_sub_tier_agency_c)
        order by 
            transaction_fabs.fain, 
            transaction_fabs.awarding_sub_tier_agency_c,  
            transaction_fabs.action_date desc, 
            transaction_fabs.award_modification_amendme desc
    ) as uniq_award on uniq_award.afa_generated_unique = tf.afa_generated_unique
    -- inner join
    -- award_category as ac on ac.type_code = tf.assistance_type
    -- inner join
    -- agency_lookup as awarding_agency on awarding_agency.subtier_code = tf.awarding_sub_tier_agency_c 
    -- left outer join
    -- agency_lookup as funding_agency on funding_agency.subtier_code = tf.funding_sub_tier_agency_co
    inner join
    subtier_agency as awarding_subtier on awarding_subtier.subtier_code = tf.awarding_sub_tier_agency_c
    inner join
    agency as awarding_agency on awarding_subtier.subtier_agency_id = awarding_agency.subtier_agency_id
    -- left outer join
    -- exec_comp_lookup as exec_comp on exec_comp.duns = tf.awardee_or_recipient_uniqu

    ) 
;

create index fabs_fain_duns_idx on fabs_fain_matview_temp2 (recipient_unique_id);
create index fabs_fain_generated_unique_award_id_idx on fabs_fain_matview_temp2 (generated_unique_award_id);
create index fabs_fain_awarding_sub_tier_idx on fabs_fain_matview_temp2 (awarding_sub_tier_agency_c);
create index fabs_fain_awarding_agency_id_idx on fabs_fain_matview_temp2 (awarding_agency_id);
create index fabs_fain_duns_idx on fabs_fain_matview_temp2 (recipient_unique_id);
create index fabs_fain_recipient_name_idx on fabs_fain_matview_temp2 (recipient_name);
create index fabs_fain_fain_idx on fabs_fain_matview_temp2 (recipient_fain_id);
create index fabs_fain_date_signed_idx on fabs_fain_matview_temp2 (date_signed);
create index fabs_fain_certified_date_idx on fabs_fain_matview_temp2 (certified_date);






