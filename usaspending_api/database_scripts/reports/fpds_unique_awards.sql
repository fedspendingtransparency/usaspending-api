with agency_lookup as (     
    select
        agency.id as agency_id,
        subtier_agency.subtier_code as subtier_code     
    from
        agency         
    inner join
        subtier_agency on subtier_agency.subtier_agency_id=agency.subtier_agency_id
),
exec_comp_lookup as (
    select
        distinct(legal_entity.recipient_unique_id) as duns,
        exec_comp.officer_1_name as officer_1_name,
        exec_comp.officer_1_amount as officer_1_amount,
        exec_comp.officer_2_name as officer_2_name,
        exec_comp.officer_2_amount as officer_2_amount,
        exec_comp.officer_3_name as officer_3_name,
        exec_comp.officer_3_amount as officer_3_amount,
        exec_comp.officer_4_name as officer_4_name,
        exec_comp.officer_4_amount as officer_4_amount,
        exec_comp.officer_5_name as officer_5_name,
        exec_comp.officer_5_amount as officer_5_amount
    from
        references_legalentityofficers as exec_comp
        inner join
        legal_entity on legal_entity.legal_entity_id = exec_comp.legal_entity_id
    where
        exec_comp.officer_1_name is not null or
        exec_comp.officer_1_amount is not null or
        exec_comp.officer_2_name is not null or
        exec_comp.officer_2_amount is not null or
        exec_comp.officer_3_name is not null or
        exec_comp.officer_3_amount is not null or
        exec_comp.officer_4_name is not null or
        exec_comp.officer_4_amount is not null or
        exec_comp.officer_5_name is not null or
        exec_comp.officer_5_amount is not null
)
select
    'cont_aw_' ||
        coalesce(tf.agency_id,'-none-') || '_' ||
        coalesce(tf.referenced_idv_agency_iden,'-none-') || '_' ||
        coalesce(tf.piid,'-none-') || '_' ||
        coalesce(tf.parent_award_id,'-none-') as generated_unique_award_id,
    tf.contract_award_type as type,
    tf.contract_award_type_desc as type_description,
    ac.type_name as category,
    tf.agency_id,
    tf.referenced_idv_agency_iden,
    tf.piid as piid,
    tf.parent_award_id as parent_award_piid,
    null as fain,
    null as uri,
    uniq_award.total_obligation as total_obligation,
    null as total_outlay,
    awarding_agency.agency_id as awarding_agency_id,
    tf.awarding_sub_tier_agency_c as awarding_sub_tier_agency_c,
    funding_agency.agency_id as funding_agency_id,
    'DBR' as data_source,
    uniq_award.signed_date as date_signed,
    tf.award_description as description,
    uniq_award.period_of_performance_start_date as period_of_performance_start_date,
    uniq_award.period_of_performance_current_end_date as period_of_performance_current_end_date,
    null as potential_total_value_of_award,
    uniq_award.base_and_all_options_value as base_and_all_options_value,
    tf.last_modified as last_modified_date, 
    uniq_award.certified_date as certified_date,
    tf.transaction_id as latest_transaction_id,
    'cont_tx_' || tf.detached_award_proc_unique as latest_transaction_unique,
    0 as total_subaward_amount,
    0 as subaward_count,
    
    -- recipient data
    tf.awardee_or_recipient_uniqu as recipient_unique_id, -- DUNS
    tf.awardee_or_recipient_legal as recipient_name,

    -- executive compensation data
    exec_comp.officer_1_name as officer_1_name,
    exec_comp.officer_1_amount as officer_1_amount,
    exec_comp.officer_2_name as officer_2_name,
    exec_comp.officer_2_amount as officer_2_amount,
    exec_comp.officer_3_name as officer_3_name,
    exec_comp.officer_3_amount as officer_3_amount,
    exec_comp.officer_4_name as officer_4_name,
    exec_comp.officer_4_amount as officer_4_amount,
    exec_comp.officer_5_name as officer_5_name,
    exec_comp.officer_5_amount as officer_5_amount,

    -- business categories
    tf.legal_entity_address_line1 as recipient_location_address_line1,
    tf.legal_entity_address_line2 as recipient_location_address_line2,
    tf.legal_entity_address_line3 as recipient_location_address_line3,
    
    -- foreign province
    null as recipient_location_foreign_province,
    
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
        when tf.legal_entity_state_code is null then (select code from state_lookup where upper(name)=upper(tf.legal_entity_state_descrip))
        else tf.legal_entity_state_code
    end as recipient_location_state_code,
    case
        when tf.legal_entity_state_descrip is null then (select name from state_lookup where upper(code)=upper(tf.legal_entity_state_code))
        else tf.legal_entity_state_descrip
    end as recipient_location_state_name,
    
    -- county (NONE FOR FPDS)
    null as recipient_location_county_code,
    null as recipient_location_county_name,
    
    -- city
    tf.legal_entity_city_name as recipient_location_city_name,
    
    -- zip
    (substring(tf.legal_entity_zip4 from '^(\d{5})\-?(\d{4})?$')) as recipient_location_zip5,
--  tf.legal_entity_zip4 as recipient_location_zip4,
    
    -- congressional disctrict
    tf.legal_entity_congressional as recipient_location_congressional_code,
    
    -- ppop data
    
    -- foreign
    null as pop_foreign_province,
    
    -- country
    case
        when tf.place_of_perform_country_c is null then (select country_code from ref_country_code where upper(country_name)=upper(tf.place_of_perf_country_desc))
        when UPPER(tf.place_of_perform_country_c)='UNITED STATES' then 'USA'
        else tf.place_of_perform_country_c
    end as pop_country_code,
    case
        when tf.place_of_perf_country_desc is null then (select country_name from ref_country_code where upper(country_code)=upper(tf.place_of_perform_country_c))
        else tf.place_of_perf_country_desc
    end as pop_country_name,
    
    -- state
    case
        when tf.place_of_performance_state is null then (select code from state_lookup where upper(name)=upper(tf.place_of_perfor_state_desc))
        else tf.place_of_performance_state
    end as pop_state_code,
    case
        when tf.place_of_perfor_state_desc is null then (select name from state_lookup where upper(code)=upper(tf.place_of_performance_state))
        else tf.place_of_perfor_state_desc
    end as pop_state_name,
    
    -- county
    null as pop_county_code,
    tf.place_of_perform_county_na as pop_county_name,
    
    -- city
    tf.place_of_perform_city_name as pop_city_name,
    
    -- zip
    (substring(tf.place_of_performance_zip4a from '^(\d{5})\-?(\d{4})?$')) as pop_zip5,
    tf.place_of_performance_zip4a as pop_zip4,
    
    -- congressional disctrict
    tf.place_of_performance_congr as pop_congressional_code
from
    transaction_fpds as tf -- latest transaction
    inner join
    (
        select
            distinct on (transaction_fpds.piid, transaction_fpds.parent_award_id, transaction_fpds.agency_id, transaction_fpds.referenced_idv_agency_iden)
            transaction_fpds.piid,
            transaction_fpds.parent_award_id,
            transaction_fpds.agency_id,
            transaction_fpds.referenced_idv_agency_iden,
            transaction_fpds.action_date,
            transaction_fpds.transaction_number,
            transaction_fpds.award_modification_amendme,
            transaction_fpds.detached_award_proc_unique,
            count(transaction_fpds.piid) over w as sumpiid,
            max(transaction_fpds.action_date) over w as certified_date,
            min(transaction_fpds.action_date) over w as signed_date,
            min(transaction_fpds.period_of_performance_star::date) over w as period_of_performance_start_date,
            max(transaction_fpds.period_of_performance_curr::date) over w as period_of_performance_current_end_date,
            sum(coalesce(transaction_fpds.base_and_all_options_value::double precision, 0::double precision)) over w as base_and_all_options_value,
            sum(coalesce(transaction_fpds.federal_action_obligation::double precision, 0::double precision)) over w as total_obligation
        from transaction_fpds
        window w as (partition by transaction_fpds.piid, transaction_fpds.parent_award_id, transaction_fpds.agency_id, transaction_fpds.referenced_idv_agency_iden)
        order by 
            transaction_fpds.piid, 
            transaction_fpds.parent_award_id, 
            transaction_fpds.agency_id, 
            transaction_fpds.referenced_idv_agency_iden, 
            transaction_fpds.action_date desc, 
            transaction_fpds.award_modification_amendme desc, 
            transaction_fpds.transaction_number desc
    ) as uniq_award on uniq_award.detached_award_proc_unique = tf.detached_award_proc_unique
    inner join
    award_category as ac on ac.type_code = tf.contract_award_type
    inner join
    agency_lookup as awarding_agency on awarding_agency.subtier_code = tf.awarding_sub_tier_agency_c 
    left outer join
    agency_lookup as funding_agency on funding_agency.subtier_code = tf.funding_sub_tier_agency_co
    left outer join
    exec_comp_lookup as exec_comp on exec_comp.duns = tf.awardee_or_recipient_uniqu
;