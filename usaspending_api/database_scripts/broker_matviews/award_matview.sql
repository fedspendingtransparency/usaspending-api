drop materialized view if exists award_matview;

create materialized view award_matview as (
(select
    generated_unique_award_id,
    type,
    type_description,
    fpds_uniq_awards.agency_id as agency_id,
    referenced_idv_agency_iden,
    referenced_idv_agency_desc,
    multiple_or_single_award_i,
    multiple_or_single_aw_desc,
    piid,
    parent_award_piid,
    fain,
    uri,
    total_obligation,
    total_subsidy_cost,
    total_outlay,
    awarding_agency_code,
    awarding_agency_name,
    awarding_agency.toptier_abbr as awarding_agency_abbr,
    awarding_sub_tier_agency_c,
    awarding_sub_tier_agency_n,
    awarding_agency.subtier_abbr as awarding_sub_tier_agency_abbr,
    awarding_office_code,
    awarding_office_name,
    funding_agency_code,
    funding_agency_name,
    funding_agency.toptier_abbr as funding_agency_abbr,
    funding_sub_tier_agency_co,
    funding_sub_tier_agency_na,
    funding_agency.subtier_abbr as funding_sub_tier_agency_abbr,
    funding_office_code,
    funding_office_name,
    data_source,
    action_date,
    date_signed,
    description,
    period_of_performance_start_date,
    period_of_performance_current_end_date,
    potential_total_value_of_award,
    base_and_all_options_value,
    last_modified_date,
    certified_date,
    record_type,
    latest_transaction_unique_id,
    total_subaward_amount,
    subaward_count,
    pulled_from,
    product_or_service_code,
    product_or_service_co_desc,
    extent_competed,
    extent_compete_description,
    type_of_contract_pricing,
    type_of_contract_pric_desc,
    contract_award_type_desc,
    cost_or_pricing_data,
    cost_or_pricing_data_desc,
    domestic_or_foreign_entity,
    domestic_or_foreign_e_desc,
    fair_opportunity_limited_s,
    fair_opportunity_limi_desc,
    foreign_funding,
    foreign_funding_desc,
    interagency_contracting_au,
    interagency_contract_desc,
    major_program,
    price_evaluation_adjustmen,
    program_acronym,
    subcontracting_plan,
    subcontracting_plan_desc,
    multi_year_contract,
    multi_year_contract_desc,
    purchase_card_as_payment_m,
    purchase_card_as_paym_desc,
    consolidated_contract,
    consolidated_contract_desc,
    solicitation_identifier,
    solicitation_procedures,
    solicitation_procedur_desc,
    number_of_offers_received,
    other_than_full_and_open_c,
    other_than_full_and_o_desc,
    commercial_item_acquisitio,
    commercial_item_acqui_desc,
    commercial_item_test_progr,
    commercial_item_test_desc,
    evaluated_preference,
    evaluated_preference_desc,
    fed_biz_opps,
    fed_biz_opps_description,
    small_business_competitive,
    dod_claimant_program_code,
    dod_claimant_prog_cod_desc,
    program_system_or_equipmen,
    program_system_or_equ_desc,
    information_technology_com,
    information_technolog_desc,
    sea_transportation,
    sea_transportation_desc,
    clinger_cohen_act_planning,
    clinger_cohen_act_pla_desc,
    davis_bacon_act,
    davis_bacon_act_descrip,
    service_contract_act,
    service_contract_act_desc,
    walsh_healey_act,
    walsh_healey_act_descrip,
    naics,
    naics_description,
    parent_award_id,
    idv_type,
    idv_type_description,
    type_set_aside,
    type_set_aside_description,
    assistance_type,
    business_funds_indicator,
    business_types,
    cfda_number,
    cfda_title,
    null::text as cfda_objectives,

    -- recipient data
    recipient_unique_id, -- DUNS
    recipient_name,
    parent_recipient_unique_id,

    -- executive compensation data
    officer_1_name,
    officer_1_amount,
    officer_2_name,
    officer_2_amount,
    officer_3_name,
    officer_3_amount,
    officer_4_name,
    officer_4_amount,
    officer_5_name,
    officer_5_amount,

    -- business categories
    recipient_location_address_line1,
    recipient_location_address_line2,
    recipient_location_address_line3,

    -- foreign province
    recipient_location_foreign_province,
    recipient_location_foreign_city_name,
    recipient_location_foreign_postal_code,

    -- country
    recipient_location_country_code,
    recipient_location_country_name,

    -- state
    recipient_location_state_code,
    recipient_location_state_name,

    -- county (NONE FOR FPDS)
    recipient_location_county_code,
    recipient_location_county_name,

    -- city
    recipient_location_city_code,
    recipient_location_city_name,

    -- zip
    recipient_location_zip5,
    recipient_location_zip4,

    -- congressional disctrict
    recipient_location_congressional_code,

    -- ppop data
    pop_code,

    -- foreign
    pop_foreign_province,

    -- country
    pop_country_code,
    pop_country_name,

    -- state
    pop_state_code,
    pop_state_name,

    -- county
    pop_county_code,
    pop_county_name,

    -- city
    pop_city_name,

    -- zip
    pop_zip5,
    pop_zip4,

    -- congressional disctrict
    pop_congressional_code,

    ac.type_name as category,
    awarding_agency.agency_id as awarding_agency_id,
    funding_agency.agency_id as funding_agency_id,
    fy(action_date) as fiscal_year
from
    dblink ('broker_server', 'select
        distinct on (tf.piid, tf.parent_award_id, tf.agency_id, tf.referenced_idv_agency_iden)
        ''cont_aw_'' ||
            coalesce(tf.agency_id,''-none-'') || ''_'' ||
            coalesce(tf.referenced_idv_agency_iden,''-none-'') || ''_'' ||
            coalesce(tf.piid,''-none-'') || ''_'' ||
            coalesce(tf.parent_award_id,''-none-'') as generated_unique_award_id,
        tf.contract_award_type as type,
        tf.contract_award_type_desc as type_description,
        tf.agency_id as agency_id,
        tf.referenced_idv_agency_iden as referenced_idv_agency_iden,
        tf.referenced_idv_agency_desc as referenced_idv_agency_desc,
        tf.multiple_or_single_award_i as multiple_or_single_award_i,
        tf.multiple_or_single_aw_desc as multiple_or_single_aw_desc,
        tf.piid as piid,
        tf.parent_award_id as parent_award_piid,
        null::text as fain,
        null::text as uri,
        sum(coalesce(tf.federal_action_obligation::double precision, 0::double precision)) over w as total_obligation,
        null::float as total_subsidy_cost,
        null::float as total_outlay,
        tf.awarding_agency_code as awarding_agency_code,
        tf.awarding_agency_name as awarding_agency_name,
        tf.awarding_sub_tier_agency_c as awarding_sub_tier_agency_c,
        tf.awarding_sub_tier_agency_n as awarding_sub_tier_agency_n,
        tf.awarding_office_code as awarding_office_code,
        tf.awarding_office_name as awarding_office_name,
        tf.funding_agency_code as funding_agency_code,
        tf.funding_agency_name as funding_agency_name,
        tf.funding_sub_tier_agency_co as funding_sub_tier_agency_co,
        tf.funding_sub_tier_agency_na as funding_sub_tier_agency_na,
        tf.funding_office_code as funding_office_code,
        tf.funding_office_name as funding_office_name,
        ''DBR''::text as data_source,
        tf.action_date::date as action_date,
        min(tf.action_date) over w as date_signed,
        tf.award_description as description,
        -- TODO: Handle when period_of_performance_star/period_of_performance_curr is ''
        min(tf.period_of_performance_star::date) over w as period_of_performance_start_date,
        max(tf.period_of_performance_curr::date) over w as period_of_performance_current_end_date,
        null::float as potential_total_value_of_award,
        sum(coalesce(tf.base_and_all_options_value::double precision, 0::double precision)) over w as base_and_all_options_value,
        tf.last_modified::date as last_modified_date,
        max(tf.action_date) over w as certified_date,
        null::int as record_type,
        ''cont_tx_'' || tf.detached_award_proc_unique as latest_transaction_unique_id,
        0 as total_subaward_amount,
        0 as subaward_count,
        tf.pulled_from as pulled_from,
        tf.product_or_service_code as product_or_service_code,
        tf.product_or_service_co_desc as product_or_service_co_desc,
        tf.extent_competed as extent_competed,
        tf.extent_compete_description as extent_compete_description,
        tf.type_of_contract_pricing as type_of_contract_pricing,
        tf.type_of_contract_pric_desc as type_of_contract_pric_desc,
        tf.contract_award_type_desc as contract_award_type_desc,
        tf.cost_or_pricing_data as cost_or_pricing_data,
        tf.cost_or_pricing_data_desc as cost_or_pricing_data_desc,
        tf.domestic_or_foreign_entity as domestic_or_foreign_entity,
        tf.domestic_or_foreign_e_desc as domestic_or_foreign_e_desc,
        tf.fair_opportunity_limited_s as fair_opportunity_limited_s,
        tf.fair_opportunity_limi_desc as fair_opportunity_limi_desc,
        tf.foreign_funding as foreign_funding,
        tf.foreign_funding_desc as foreign_funding_desc,
        tf.interagency_contracting_au as interagency_contracting_au,
        tf.interagency_contract_desc as interagency_contract_desc,
        tf.major_program as major_program,
        tf.price_evaluation_adjustmen as price_evaluation_adjustmen,
        tf.program_acronym as program_acronym,
        tf.subcontracting_plan as subcontracting_plan,
        tf.subcontracting_plan_desc as subcontracting_plan_desc,
        tf.multi_year_contract as multi_year_contract,
        tf.multi_year_contract_desc as multi_year_contract_desc,
        tf.purchase_card_as_payment_m as purchase_card_as_payment_m,
        tf.purchase_card_as_paym_desc as purchase_card_as_paym_desc,
        tf.consolidated_contract as consolidated_contract,
        tf.consolidated_contract_desc as consolidated_contract_desc,
        tf.solicitation_identifier as solicitation_identifier,
        tf.solicitation_procedures as solicitation_procedures,
        tf.solicitation_procedur_desc as solicitation_procedur_desc,
        tf.number_of_offers_received as number_of_offers_received,
        tf.other_than_full_and_open_c as other_than_full_and_open_c,
        tf.other_than_full_and_o_desc as other_than_full_and_o_desc,
        tf.commercial_item_acquisitio as commercial_item_acquisitio,
        tf.commercial_item_acqui_desc as commercial_item_acqui_desc,
        tf.commercial_item_test_progr as commercial_item_test_progr,
        tf.commercial_item_test_desc as commercial_item_test_desc,
        tf.evaluated_preference as evaluated_preference,
        tf.evaluated_preference_desc as evaluated_preference_desc,
        tf.fed_biz_opps as fed_biz_opps,
        tf.fed_biz_opps_description as fed_biz_opps_description,
        tf.small_business_competitive as small_business_competitive,
        tf.dod_claimant_program_code as dod_claimant_program_code,
        tf.dod_claimant_prog_cod_desc as dod_claimant_prog_cod_desc,
        tf.program_system_or_equipmen as program_system_or_equipmen,
        tf.program_system_or_equ_desc as program_system_or_equ_desc,
        tf.information_technology_com as information_technology_com,
        tf.information_technolog_desc as information_technolog_desc,
        tf.sea_transportation as sea_transportation,
        tf.sea_transportation_desc as sea_transportation_desc,
        tf.clinger_cohen_act_planning as clinger_cohen_act_planning,
        tf.clinger_cohen_act_pla_desc as clinger_cohen_act_pla_desc,
        tf.davis_bacon_act as davis_bacon_act,
        tf.davis_bacon_act_descrip as davis_bacon_act_descrip,
        tf.service_contract_act as service_contract_act,
        tf.service_contract_act_desc as service_contract_act_desc,
        tf.walsh_healey_act as walsh_healey_act,
        tf.walsh_healey_act_descrip as walsh_healey_act_descrip,
        tf.naics as naics,
        tf.naics_description as naics_description,
        tf.parent_award_id as parent_award_id,
        tf.idv_type as idv_type,
        tf.idv_type_description as idv_type_description,
        tf.type_set_aside as type_set_aside,
        tf.type_set_aside_description as type_set_aside_description,
        null::text as assistance_type,
        null::text as business_funds_indicator,
        null::text as business_types,
        null::text as cfda_number,
        null::text as cfda_title,

        -- recipient data
        tf.awardee_or_recipient_uniqu as recipient_unique_id, -- DUNS
        tf.awardee_or_recipient_legal as recipient_name,
        tf.ultimate_parent_unique_ide as parent_recipient_unique_id,

        -- executive compensation data
        exec_comp.high_comp_officer1_full_na as officer_1_name,
        exec_comp.high_comp_officer1_amount as officer_1_amount,
        exec_comp.high_comp_officer2_full_na as officer_2_name,
        exec_comp.high_comp_officer2_amount as officer_2_amount,
        exec_comp.high_comp_officer3_full_na as officer_3_name,
        exec_comp.high_comp_officer3_amount as officer_3_amount,
        exec_comp.high_comp_officer4_full_na as officer_4_name,
        exec_comp.high_comp_officer4_amount as officer_4_amount,
        exec_comp.high_comp_officer5_full_na as officer_5_name,
        exec_comp.high_comp_officer5_amount as officer_5_amount,

        -- business categories
        tf.legal_entity_address_line1 as recipient_location_address_line1,
        tf.legal_entity_address_line2 as recipient_location_address_line2,
        tf.legal_entity_address_line3 as recipient_location_address_line3,

        -- foreign province
        null::text as recipient_location_foreign_province,
        null::text as recipient_location_foreign_city_name,
        null::text as recipient_location_foreign_city_name,

        -- country
        tf.legal_entity_country_code as recipient_location_country_code,
        tf.legal_entity_country_name as recipient_location_country_name,

        -- state
        tf.legal_entity_state_code as recipient_location_state_code,
        tf.legal_entity_state_descrip as recipient_location_state_name,

        -- county (NONE FOR FPDS)
        null::text as recipient_location_county_code,
        null::text as recipient_location_county_name,

        -- city
        null::text as recipient_location_city_code,
        tf.legal_entity_city_name as recipient_location_city_name,

        -- zip
        null::text as recipient_location_zip5,
        tf.legal_entity_zip4 as recipient_location_zip4,

        -- congressional disctrict
        tf.legal_entity_congressional as recipient_location_congressional_code,

        -- ppop data
        null::text as pop_code,

        -- foreign
        null::text as pop_foreign_province,

        -- country
        tf.place_of_perform_country_c as pop_country_code,
        tf.place_of_perf_country_desc as pop_country_name,

        -- state
        tf.place_of_performance_state as pop_state_code,
        tf.place_of_perfor_state_desc as pop_state_name,

        -- county
        null::text as pop_county_code,
        tf.place_of_perform_county_na as pop_county_name,

        -- city
        tf.place_of_perform_city_name as pop_city_name,

        -- zip
        substring(tf.place_of_performance_zip4a from 0 for 6) as pop_zip5,
        tf.place_of_performance_zip4a as pop_zip4,

        -- congressional disctrict
        tf.place_of_performance_congr as pop_congressional_code
    from
        detached_award_procurement tf -- aka latest transaction
        left outer join
        exec_comp_lookup as exec_comp on exec_comp.awardee_or_recipient_uniqu = tf.awardee_or_recipient_uniqu
    window w as (partition by tf.piid, tf.parent_award_id, tf.agency_id, tf.referenced_idv_agency_iden)
    order by
        tf.piid,
        tf.parent_award_id,
        tf.agency_id,
        tf.referenced_idv_agency_iden,
        tf.action_date desc,
        tf.award_modification_amendme desc,
        tf.transaction_number desc') as fpds_uniq_awards
    (
        generated_unique_award_id text,
        type text,
        type_description text,
        agency_id text,
        referenced_idv_agency_iden text,
        referenced_idv_agency_desc text,
        multiple_or_single_award_i text,
        multiple_or_single_aw_desc text,
        piid text,
        parent_award_piid text,
        fain text,
        uri text,
        total_obligation float(2),
        total_subsidy_cost float(2),
        total_outlay float(2),
        awarding_agency_code text,
        awarding_agency_name text,
        awarding_sub_tier_agency_c text,
        awarding_sub_tier_agency_n text,
        awarding_office_code text,
        awarding_office_name text,
        funding_agency_code text,
        funding_agency_name text,
        funding_sub_tier_agency_co text,
        funding_sub_tier_agency_na text,
        funding_office_code text,
        funding_office_name text,
        data_source text,
        action_date date,
        date_signed date,
        description text,
        period_of_performance_start_date date,
        period_of_performance_current_end_date date,
        potential_total_value_of_award float(2),
        base_and_all_options_value float(2),
        last_modified_date date,
        certified_date date,
        record_type int,
        latest_transaction_unique_id text,
        total_subaward_amount float(2),
        subaward_count int,
        pulled_from text,
        product_or_service_code text,
        product_or_service_co_desc text,
        extent_competed text,
        extent_compete_description text,
        type_of_contract_pricing text,
        type_of_contract_pric_desc text,
        contract_award_type_desc text,
        cost_or_pricing_data text,
        cost_or_pricing_data_desc text,
        domestic_or_foreign_entity text,
        domestic_or_foreign_e_desc text,
        fair_opportunity_limited_s text,
        fair_opportunity_limi_desc text,
        foreign_funding text,
        foreign_funding_desc text,
        interagency_contracting_au text,
        interagency_contract_desc text,
        major_program text,
        price_evaluation_adjustmen text,
        program_acronym text,
        subcontracting_plan text,
        subcontracting_plan_desc text,
        multi_year_contract text,
        multi_year_contract_desc text,
        purchase_card_as_payment_m text,
        purchase_card_as_paym_desc text,
        consolidated_contract text,
        consolidated_contract_desc text,
        solicitation_identifier text,
        solicitation_procedures text,
        solicitation_procedur_desc text,
        number_of_offers_received text,
        other_than_full_and_open_c text,
        other_than_full_and_o_desc text,
        commercial_item_acquisitio text,
        commercial_item_acqui_desc text,
        commercial_item_test_progr text,
        commercial_item_test_desc text,
        evaluated_preference text,
        evaluated_preference_desc text,
        fed_biz_opps text,
        fed_biz_opps_description text,
        small_business_competitive text,
        dod_claimant_program_code text,
        dod_claimant_prog_cod_desc text,
        program_system_or_equipmen text,
        program_system_or_equ_desc text,
        information_technology_com text,
        information_technolog_desc text,
        sea_transportation text,
        sea_transportation_desc text,
        clinger_cohen_act_planning text,
        clinger_cohen_act_pla_desc text,
        davis_bacon_act text,
        davis_bacon_act_descrip text,
        service_contract_act text,
        service_contract_act_desc text,
        walsh_healey_act text,
        walsh_healey_act_descrip text,
        naics text,
        naics_description text,
        parent_award_id text,
        idv_type text,
        idv_type_description text,
        type_set_aside text,
        type_set_aside_description text,
        assistance_type text,
        business_funds_indicator text,
        business_types text,
        cfda_number text,
        cfda_title text,

        -- recipient data
        recipient_unique_id text, -- DUNS
        recipient_name text,
        parent_recipient_unique_id text,

        -- executive compensation data
        officer_1_name text,
        officer_1_amount text,
        officer_2_name text,
        officer_2_amount text,
        officer_3_name text,
        officer_3_amount text,
        officer_4_name text,
        officer_4_amount text,
        officer_5_name text,
        officer_5_amount text,

        -- business categories
        recipient_location_address_line1 text,
        recipient_location_address_line2 text,
        recipient_location_address_line3 text,

        -- foreign province
        recipient_location_foreign_province text,
        recipient_location_foreign_city_name text,
        recipient_location_foreign_postal_code text,

        -- country
        recipient_location_country_code text,
        recipient_location_country_name text,

        -- state
        recipient_location_state_code text,
        recipient_location_state_name text,

        -- county (NONE FOR FPDS)
        recipient_location_county_code text,
        recipient_location_county_name text,

        -- city
        recipient_location_city_code text,
        recipient_location_city_name text,

        -- zip
        recipient_location_zip5 text,
        recipient_location_zip4 text,

        -- congressional disctrict
        recipient_location_congressional_code text,

        -- ppop data
        pop_code text,

        -- foreign
        pop_foreign_province text,

        -- country
        pop_country_code text,
        pop_country_name text,

        -- state
        pop_state_code text,
        pop_state_name text,

        -- county
        pop_county_code text,
        pop_county_name text,

        -- city
        pop_city_name text,

        -- zip
        pop_zip5 text,
        pop_zip4 text,

        -- congressional disctrict
        pop_congressional_code text
    )
    inner join
    award_category as ac on ac.type_code = type
    inner join
    agency_lookup as awarding_agency on awarding_agency.subtier_code = awarding_sub_tier_agency_c
    left outer join
    agency_lookup as funding_agency on funding_agency.subtier_code = funding_sub_tier_agency_co)

union all

(select
    generated_unique_award_id,
    type,
    type_description,
    fabs_fain_uniq_awards.agency_id as agency_id,
    referenced_idv_agency_iden,
    referenced_idv_agency_desc,
    multiple_or_single_award_i,
    multiple_or_single_aw_desc,
    piid,
    parent_award_piid,
    fain,
    uri,
    total_obligation,
    total_subsidy_cost,
    total_outlay,
    awarding_agency_code,
    awarding_agency_name,
    awarding_agency.toptier_abbr as awarding_agency_abbr,
    awarding_sub_tier_agency_c,
    awarding_sub_tier_agency_n,
    awarding_agency.subtier_abbr as awarding_sub_tier_agency_abbr,
    awarding_office_code,
    awarding_office_name,
    funding_agency_code,
    funding_agency_name,
    funding_agency.toptier_abbr as funding_agency_abbr,
    funding_sub_tier_agency_co,
    funding_sub_tier_agency_na,
    funding_agency.subtier_abbr as funding_sub_tier_agency_abbr,
    funding_office_code,
    funding_office_name,
    fabs_fain_uniq_awards.data_source,
    action_date,
    date_signed,
    description,
    period_of_performance_start_date,
    period_of_performance_current_end_date,
    potential_total_value_of_award,
    base_and_all_options_value,
    last_modified_date,
    certified_date,
    record_type,
    latest_transaction_unique_id,
    total_subaward_amount,
    subaward_count,
    pulled_from,
    product_or_service_code,
    product_or_service_co_desc,
    extent_competed,
    extent_compete_description,
    type_of_contract_pricing,
    type_of_contract_pric_desc,
    contract_award_type_desc,
    cost_or_pricing_data,
    cost_or_pricing_data_desc,
    domestic_or_foreign_entity,
    domestic_or_foreign_e_desc,
    fair_opportunity_limited_s,
    fair_opportunity_limi_desc,
    foreign_funding,
    foreign_funding_desc,
    interagency_contracting_au,
    interagency_contract_desc,
    major_program,
    price_evaluation_adjustmen,
    program_acronym,
    subcontracting_plan,
    subcontracting_plan_desc,
    multi_year_contract,
    multi_year_contract_desc,
    purchase_card_as_payment_m,
    purchase_card_as_paym_desc,
    consolidated_contract,
    consolidated_contract_desc,
    solicitation_identifier,
    solicitation_procedures,
    solicitation_procedur_desc,
    number_of_offers_received,
    other_than_full_and_open_c,
    other_than_full_and_o_desc,
    commercial_item_acquisitio,
    commercial_item_acqui_desc,
    commercial_item_test_progr,
    commercial_item_test_desc,
    evaluated_preference,
    evaluated_preference_desc,
    fed_biz_opps,
    fed_biz_opps_description,
    small_business_competitive,
    dod_claimant_program_code,
    dod_claimant_prog_cod_desc,
    program_system_or_equipmen,
    program_system_or_equ_desc,
    information_technology_com,
    information_technolog_desc,
    sea_transportation,
    sea_transportation_desc,
    clinger_cohen_act_planning,
    clinger_cohen_act_pla_desc,
    davis_bacon_act,
    davis_bacon_act_descrip,
    service_contract_act,
    service_contract_act_desc,
    walsh_healey_act,
    walsh_healey_act_descrip,
    naics,
    naics_description,
    parent_award_id,
    idv_type,
    idv_type_description,
    type_set_aside,
    type_set_aside_description,
    assistance_type,
    business_funds_indicator,
    business_types,
    cfda_number,
    cfda_title,
    cfda.objectives as cfda_objectives,

    -- recipient data
    recipient_unique_id, -- DUNS
    recipient_name,
    parent_recipient_unique_id,

    -- executive compensation data
    officer_1_name,
    officer_1_amount,
    officer_2_name,
    officer_2_amount,
    officer_3_name,
    officer_3_amount,
    officer_4_name,
    officer_4_amount,
    officer_5_name,
    officer_5_amount,

    -- business categories
    recipient_location_address_line1,
    recipient_location_address_line2,
    recipient_location_address_line3,

    -- foreign province
    recipient_location_foreign_province,
    recipient_location_foreign_city_name,
    recipient_location_foreign_postal_code,

    -- country
    recipient_location_country_code,
    recipient_location_country_name,

    -- state
    recipient_location_state_code,
    recipient_location_state_name,

    -- county (NONE FOR FPDS)
    recipient_location_county_code,
    recipient_location_county_name,

    -- city
    recipient_location_city_code,
    recipient_location_city_name,

    -- zip
    recipient_location_zip5,
    recipient_location_zip4,

    -- congressional disctrict
    recipient_location_congressional_code,

    -- ppop data
    pop_code,

    -- foreign
    pop_foreign_province,

    -- country
    pop_country_code,
    pop_country_name,

    -- state
    pop_state_code,
    pop_state_name,

    -- county
    pop_county_code,
    pop_county_name,

    -- city
    pop_city_name,

    -- zip
    pop_zip5,
    pop_zip4,

    -- congressional disctrict
    pop_congressional_code,

    ac.type_name as category,
    awarding_agency.agency_id as awarding_agency_id,
    funding_agency.agency_id as funding_agency_id,
    fy(action_date) as fiscal_year
from
    dblink ('broker_server', 'select
    distinct on (pafa.fain, pafa.awarding_sub_tier_agency_c)
    ''asst_aw_'' ||
        coalesce(pafa.awarding_sub_tier_agency_c,''-none-'') || ''_'' ||
        coalesce(pafa.fain, ''-none-'') || ''_'' ||
        ''-none-'' as generated_unique_award_id,
    pafa.assistance_type as type,
    case
        when pafa.assistance_type = ''02'' then ''Block Grant''
        when pafa.assistance_type = ''03'' then ''Formula Grant''
        when pafa.assistance_type = ''04'' then ''Project Grant''
        when pafa.assistance_type = ''05'' then ''Cooperative Agreement''
        when pafa.assistance_type = ''06'' then ''Direct Payment for Specified Use''
        when pafa.assistance_type = ''07'' then ''Direct Loan''
        when pafa.assistance_type = ''08'' then ''Guaranteed/Insured Loan''
        when pafa.assistance_type = ''09'' then ''Insurance''
        when pafa.assistance_type = ''10'' then ''Direct Payment with Unrestricted Use''
        when pafa.assistance_type = ''11'' then ''Other Financial Assistance''
    end as type_description,
    null::text as agency_id,
    null::text as referenced_idv_agency_iden,
    null::text as referenced_idv_agency_desc,
    null::text as multiple_or_single_award_i,
    null::text as multiple_or_single_aw_desc,
    null::text as piid,
    null::text as parent_award_piid,
    pafa.fain as fain,
    null::text as uri,
    sum(coalesce(pafa.federal_action_obligation::double precision, 0::double precision)) over w as total_obligation,
    sum(coalesce(pafa.original_loan_subsidy_cost::double precision, 0::double precision)) over w as total_subsidy_cost,
    null::float as total_outlay,
    pafa.awarding_agency_code as awarding_agency_code,
    pafa.awarding_agency_name as awarding_agency_name,
    pafa.awarding_sub_tier_agency_c as awarding_sub_tier_agency_c,
    pafa.awarding_sub_tier_agency_n as awarding_sub_tier_agency_n,
    pafa.awarding_office_code as awarding_office_code,
    pafa.awarding_office_name as awarding_office_name,
    pafa.funding_agency_code as funding_agency_code,
    pafa.funding_agency_name as funding_agency_name,
    pafa.funding_sub_tier_agency_co as funding_sub_tier_agency_co,
    pafa.funding_sub_tier_agency_na as funding_sub_tier_agency_na,
    pafa.funding_office_code as funding_office_code,
    pafa.funding_office_name as funding_office_name,
    ''DBR''::text as data_source,
    pafa.action_date::date as action_date,
    min(pafa.action_date) over w as date_signed,
    pafa.award_description as description,
    -- TODO: Handle when period_of_performance_star/period_of_performance_curr is ''
    min(pafa.period_of_performance_star::date) over w as period_of_performance_start_date,
    max(pafa.period_of_performance_curr::date) over w as period_of_performance_current_end_date,
    null::float as potential_total_value_of_award,
    null::float as base_and_all_options_value,
    pafa.modified_at::date as last_modified_date,
    max(pafa.action_date) over w as certified_date,
    pafa.record_type as record_type,
    ''asst_tx_'' || pafa.afa_generated_unique as latest_transaction_unique_id,
    0 as total_subaward_amount,
    0 as subaward_count,
    null::text as pulled_from,
    null::text as product_or_service_code,
    null::text as product_or_service_co_desc,
    null::text as extent_competed,
    null::text as extent_compete_description,
    null::text as type_of_contract_pricing,
    null::text as type_of_contract_pric_desc,
    null::text as contract_award_type_desc,
    null::text as cost_or_pricing_data,
    null::text as cost_or_pricing_data_desc,
    null::text as domestic_or_foreign_entity,
    null::text as domestic_or_foreign_e_desc,
    null::text as fair_opportunity_limited_s,
    null::text as fair_opportunity_limi_desc,
    null::text as foreign_funding,
    null::text as foreign_funding_desc,
    null::text as interagency_contracting_au,
    null::text as interagency_contract_desc,
    null::text as major_program,
    null::text as price_evaluation_adjustmen,
    null::text as program_acronym,
    null::text as subcontracting_plan,
    null::text as subcontracting_plan_desc,
    null::text as multi_year_contract,
    null::text as multi_year_contract_desc,
    null::text as purchase_card_as_payment_m,
    null::text as purchase_card_as_paym_desc,
    null::text as consolidated_contract,
    null::text as consolidated_contract_desc,
    null::text as solicitation_identifier,
    null::text as solicitation_procedures,
    null::text as solicitation_procedur_desc,
    null::text as number_of_offers_received,
    null::text as other_than_full_and_open_c,
    null::text as other_than_full_and_o_desc,
    null::text as commercial_item_acquisitio,
    null::text as commercial_item_acqui_desc,
    null::text as commercial_item_test_progr,
    null::text as commercial_item_test_desc,
    null::text as evaluated_preference,
    null::text as evaluated_preference_desc,
    null::text as fed_biz_opps,
    null::text as fed_biz_opps_description,
    null::text as small_business_competitive,
    null::text as dod_claimant_program_code,
    null::text as dod_claimant_prog_cod_desc,
    null::text as program_system_or_equipmen,
    null::text as program_system_or_equ_desc,
    null::text as information_technology_com,
    null::text as information_technolog_desc,
    null::text as sea_transportation,
    null::text as sea_transportation_desc,
    null::text as clinger_cohen_act_planning,
    null::text as clinger_cohen_act_pla_desc,
    null::text as davis_bacon_act,
    null::text as davis_bacon_act_descrip,
    null::text as service_contract_act,
    null::text as service_contract_act_desc,
    null::text as walsh_healey_act,
    null::text as walsh_healey_act_descrip,
    null::text as naics,
    null::text as naics_description,
    null::text as parent_award_id,
    null::text as idv_type,
    null::text as idv_type_description,
    null::text as type_set_aside,
    null::text as type_set_aside_description,
    pafa.assistance_type as assistance_type,
    pafa.business_funds_indicator as business_funds_indicator,
    pafa.business_types as business_types,
    pafa.cfda_number as cfda_number,
    pafa.cfda_title as cfda_title,

    -- recipient data
    pafa.awardee_or_recipient_uniqu as recipient_unique_id,
    pafa.awardee_or_recipient_legal as recipient_name,
    null::text as parent_recipient_unique_id,

    -- executive compensation data
    exec_comp.high_comp_officer1_full_na as officer_1_name,
    exec_comp.high_comp_officer1_amount as officer_1_amount,
    exec_comp.high_comp_officer2_full_na as officer_2_name,
    exec_comp.high_comp_officer2_amount as officer_2_amount,
    exec_comp.high_comp_officer3_full_na as officer_3_name,
    exec_comp.high_comp_officer3_amount as officer_3_amount,
    exec_comp.high_comp_officer4_full_na as officer_4_name,
    exec_comp.high_comp_officer4_amount as officer_4_amount,
    exec_comp.high_comp_officer5_full_na as officer_5_name,
    exec_comp.high_comp_officer5_amount as officer_5_amount,

    -- business categories
    pafa.legal_entity_address_line1 as recipient_location_address_line1,
    pafa.legal_entity_address_line2 as recipient_location_address_line2,
    pafa.legal_entity_address_line3 as recipient_location_address_line3,

    -- foreign province
    pafa.legal_entity_foreign_provi as recipient_location_foreign_province,
    pafa.legal_entity_foreign_city as recipient_location_foreign_city_name,
    pafa.legal_entity_foreign_posta as recipient_location_foreign_postal_code,

    -- country
    pafa.legal_entity_country_code as recipient_location_country_code,
    pafa.legal_entity_country_name as recipient_location_country_name,

    -- state
    pafa.legal_entity_state_code as recipient_location_state_code,
    pafa.legal_entity_state_name as recipient_location_state_name,

    -- county
    pafa.legal_entity_county_code as recipient_location_county_code,
    pafa.legal_entity_county_name as recipient_location_county_name,

    -- city
    pafa.legal_entity_city_code as recipient_location_city_code,
    pafa.legal_entity_city_name as recipient_location_city_name,

    -- zip
    pafa.legal_entity_zip5 as recipient_location_zip5,
    pafa.legal_entity_zip5 || coalesce(pafa.legal_entity_zip_last4, '''') as recipient_location_zip4,

    -- congressional disctrict
    pafa.legal_entity_congressional as recipient_location_congressional_code,

    -- ppop data
    pafa.place_of_performance_code as pop_code,

    -- foreign
    pafa.place_of_performance_forei as pop_foreign_province,

    -- country
    pafa.place_of_perform_country_c as pop_country_code,
    pafa.place_of_perform_country_n as pop_country_name,

    -- state
    null::text as pop_state_code,
    pafa.place_of_perform_state_nam as pop_state_name,

    -- county
    pafa.place_of_perform_county_co as pop_county_code,
    pafa.place_of_perform_county_na as pop_county_name,

    -- city
    pafa.place_of_performance_city as pop_city_name,

    -- zip
    substring(pafa.place_of_performance_zip4a from 0 for 6) as pop_zip5,
    pafa.place_of_performance_zip4a as pop_zip4,

    -- congressional disctrict
    pafa.place_of_performance_congr as pop_congressional_code

from published_award_financial_assistance as pafa
    left outer join
    exec_comp_lookup as exec_comp on exec_comp.awardee_or_recipient_uniqu = pafa.awardee_or_recipient_uniqu
where pafa.record_type = ''2'' and is_active=TRUE
window w as (partition by pafa.fain, pafa.awarding_sub_tier_agency_c)
order by
    pafa.fain,
    pafa.awarding_sub_tier_agency_c,
    pafa.action_date desc,
    pafa.award_modification_amendme desc
;') as fabs_fain_uniq_awards
    (
        generated_unique_award_id text,
        type text,
        type_description text,
        agency_id text,
        referenced_idv_agency_iden text,
        referenced_idv_agency_desc text,
        multiple_or_single_award_i text,
        multiple_or_single_aw_desc text,
        piid text,
        parent_award_piid text,
        fain text,
        uri text,
        total_obligation float(2),
        total_subsidy_cost float(2),
        total_outlay float(2),
        awarding_agency_code text,
        awarding_agency_name text,
        awarding_sub_tier_agency_c text,
        awarding_sub_tier_agency_n text,
        awarding_office_code text,
        awarding_office_name text,
        funding_agency_code text,
        funding_agency_name text,
        funding_sub_tier_agency_co text,
        funding_sub_tier_agency_na text,
        funding_office_code text,
        funding_office_name text,
        data_source text,
        action_date date,
        date_signed date,
        description text,
        period_of_performance_start_date date,
        period_of_performance_current_end_date date,
        potential_total_value_of_award float(2),
        base_and_all_options_value float(2),
        last_modified_date date,
        certified_date date,
        record_type int,
        latest_transaction_unique_id text,
        total_subaward_amount float(2),
        subaward_count int,
        pulled_from text,
        product_or_service_code text,
        product_or_service_co_desc text,
        extent_competed text,
        extent_compete_description text,
        type_of_contract_pricing text,
        type_of_contract_pric_desc text,
        contract_award_type_desc text,
        cost_or_pricing_data text,
        cost_or_pricing_data_desc text,
        domestic_or_foreign_entity text,
        domestic_or_foreign_e_desc text,
        fair_opportunity_limited_s text,
        fair_opportunity_limi_desc text,
        foreign_funding text,
        foreign_funding_desc text,
        interagency_contracting_au text,
        interagency_contract_desc text,
        major_program text,
        price_evaluation_adjustmen text,
        program_acronym text,
        subcontracting_plan text,
        subcontracting_plan_desc text,
        multi_year_contract text,
        multi_year_contract_desc text,
        purchase_card_as_payment_m text,
        purchase_card_as_paym_desc text,
        consolidated_contract text,
        consolidated_contract_desc text,
        solicitation_identifier text,
        solicitation_procedures text,
        solicitation_procedur_desc text,
        number_of_offers_received text,
        other_than_full_and_open_c text,
        other_than_full_and_o_desc text,
        commercial_item_acquisitio text,
        commercial_item_acqui_desc text,
        commercial_item_test_progr text,
        commercial_item_test_desc text,
        evaluated_preference text,
        evaluated_preference_desc text,
        fed_biz_opps text,
        fed_biz_opps_description text,
        small_business_competitive text,
        dod_claimant_program_code text,
        dod_claimant_prog_cod_desc text,
        program_system_or_equipmen text,
        program_system_or_equ_desc text,
        information_technology_com text,
        information_technolog_desc text,
        sea_transportation text,
        sea_transportation_desc text,
        clinger_cohen_act_planning text,
        clinger_cohen_act_pla_desc text,
        davis_bacon_act text,
        davis_bacon_act_descrip text,
        service_contract_act text,
        service_contract_act_desc text,
        walsh_healey_act text,
        walsh_healey_act_descrip text,
        naics text,
        naics_description text,
        parent_award_id text,
        idv_type text,
        idv_type_description text,
        type_set_aside text,
        type_set_aside_description text,
        assistance_type text,
        business_funds_indicator text,
        business_types text,
        cfda_number text,
        cfda_title text,

        -- recipient data
        recipient_unique_id text, -- DUNS
        recipient_name text,
        parent_recipient_unique_id text,

        -- executive compensation data
        officer_1_name text,
        officer_1_amount text,
        officer_2_name text,
        officer_2_amount text,
        officer_3_name text,
        officer_3_amount text,
        officer_4_name text,
        officer_4_amount text,
        officer_5_name text,
        officer_5_amount text,

        -- business categories
        recipient_location_address_line1 text,
        recipient_location_address_line2 text,
        recipient_location_address_line3 text,

        -- foreign province
        recipient_location_foreign_province text,
        recipient_location_foreign_city_name text,
        recipient_location_foreign_postal_code text,

        -- country
        recipient_location_country_code text,
        recipient_location_country_name text,

        -- state
        recipient_location_state_code text,
        recipient_location_state_name text,

        -- county (NONE FOR FPDS)
        recipient_location_county_code text,
        recipient_location_county_name text,

        -- city
        recipient_location_city_code text,
        recipient_location_city_name text,

        -- zip
        recipient_location_zip5 text,
        recipient_location_zip4 text,

        -- congressional disctrict
        recipient_location_congressional_code text,

        -- ppop data
        pop_code text,

        -- foreign
        pop_foreign_province text,

        -- country
        pop_country_code text,
        pop_country_name text,

        -- state
        pop_state_code text,
        pop_state_name text,

        -- county
        pop_county_code text,
        pop_county_name text,

        -- city
        pop_city_name text,

        -- zip
        pop_zip5 text,
        pop_zip4 text,

        -- congressional disctrict
        pop_congressional_code text
    )
    inner join
    references_cfda as cfda on cfda.program_number = cfda_number
    inner join
    award_category as ac on ac.type_code = type
    inner join
    agency_lookup as awarding_agency on awarding_agency.subtier_code = awarding_sub_tier_agency_c
    left outer join
    agency_lookup as funding_agency on funding_agency.subtier_code = funding_sub_tier_agency_co)

union all

(select
    generated_unique_award_id,
    type,
    type_description,
    fabs_uri_uniq_awards.agency_id as agency_id,
    referenced_idv_agency_iden,
    referenced_idv_agency_desc,
    multiple_or_single_award_i,
    multiple_or_single_aw_desc,
    piid,
    parent_award_piid,
    fain,
    uri,
    total_obligation,
    total_subsidy_cost,
    total_outlay,
    awarding_agency_code,
    awarding_agency_name,
    awarding_agency.toptier_abbr as awarding_agency_abbr,
    awarding_sub_tier_agency_c,
    awarding_sub_tier_agency_n,
    awarding_agency.subtier_abbr as awarding_sub_tier_agency_abbr,
    awarding_office_code,
    awarding_office_name,
    funding_agency_code,
    funding_agency_name,
    funding_agency.toptier_abbr as funding_agency_abbr,
    funding_sub_tier_agency_co,
    funding_sub_tier_agency_na,
    funding_agency.subtier_abbr as funding_sub_tier_agency_abbr,
    funding_office_code,
    funding_office_name,
    fabs_uri_uniq_awards.data_source,
    action_date,
    date_signed,
    description,
    period_of_performance_start_date,
    period_of_performance_current_end_date,
    potential_total_value_of_award,
    base_and_all_options_value,
    last_modified_date,
    certified_date,
    record_type,
    latest_transaction_unique_id,
    total_subaward_amount,
    subaward_count,
    pulled_from,
    product_or_service_code,
    product_or_service_co_desc,
    extent_competed,
    extent_compete_description,
    type_of_contract_pricing,
    type_of_contract_pric_desc,
    contract_award_type_desc,
    cost_or_pricing_data,
    cost_or_pricing_data_desc,
    domestic_or_foreign_entity,
    domestic_or_foreign_e_desc,
    fair_opportunity_limited_s,
    fair_opportunity_limi_desc,
    foreign_funding,
    foreign_funding_desc,
    interagency_contracting_au,
    interagency_contract_desc,
    major_program,
    price_evaluation_adjustmen,
    program_acronym,
    subcontracting_plan,
    subcontracting_plan_desc,
    multi_year_contract,
    multi_year_contract_desc,
    purchase_card_as_payment_m,
    purchase_card_as_paym_desc,
    consolidated_contract,
    consolidated_contract_desc,
    solicitation_identifier,
    solicitation_procedures,
    solicitation_procedur_desc,
    number_of_offers_received,
    other_than_full_and_open_c,
    other_than_full_and_o_desc,
    commercial_item_acquisitio,
    commercial_item_acqui_desc,
    commercial_item_test_progr,
    commercial_item_test_desc,
    evaluated_preference,
    evaluated_preference_desc,
    fed_biz_opps,
    fed_biz_opps_description,
    small_business_competitive,
    dod_claimant_program_code,
    dod_claimant_prog_cod_desc,
    program_system_or_equipmen,
    program_system_or_equ_desc,
    information_technology_com,
    information_technolog_desc,
    sea_transportation,
    sea_transportation_desc,
    clinger_cohen_act_planning,
    clinger_cohen_act_pla_desc,
    davis_bacon_act,
    davis_bacon_act_descrip,
    service_contract_act,
    service_contract_act_desc,
    walsh_healey_act,
    walsh_healey_act_descrip,
    naics,
    naics_description,
    parent_award_id,
    idv_type,
    idv_type_description,
    type_set_aside,
    type_set_aside_description,
    assistance_type,
    business_funds_indicator,
    business_types,
    cfda_number,
    cfda_title,
    cfda.objectives as cfda_objectives,

    -- recipient data
    recipient_unique_id, -- DUNS
    recipient_name,
    parent_recipient_unique_id,

    -- executive compensation data
    officer_1_name,
    officer_1_amount,
    officer_2_name,
    officer_2_amount,
    officer_3_name,
    officer_3_amount,
    officer_4_name,
    officer_4_amount,
    officer_5_name,
    officer_5_amount,

    -- business categories
    recipient_location_address_line1,
    recipient_location_address_line2,
    recipient_location_address_line3,

    -- foreign province
    recipient_location_foreign_province,
    recipient_location_foreign_city_name,
    recipient_location_foreign_postal_code,

    -- country
    recipient_location_country_code,
    recipient_location_country_name,

    -- state
    recipient_location_state_code,
    recipient_location_state_name,

    -- county (NONE FOR FPDS)
    recipient_location_county_code,
    recipient_location_county_name,

    -- city
    recipient_location_city_code,
    recipient_location_city_name,

    -- zip
    recipient_location_zip5,
    recipient_location_zip4,

    -- congressional disctrict
    recipient_location_congressional_code,

    -- ppop data
    pop_code,

    -- foreign
    pop_foreign_province,

    -- country
    pop_country_code,
    pop_country_name,

    -- state
    pop_state_code,
    pop_state_name,

    -- county
    pop_county_code,
    pop_county_name,

    -- city
    pop_city_name,

    -- zip
    pop_zip5,
    pop_zip4,

    -- congressional disctrict
    pop_congressional_code,

    ac.type_name as category,
    awarding_agency.agency_id as awarding_agency_id,
    funding_agency.agency_id as funding_agency_id,
    fy(action_date) as fiscal_year
from
    dblink ('broker_server', 'select
    distinct on (pafa.uri, pafa.awarding_sub_tier_agency_c)
    ''asst_aw_'' ||
        coalesce(pafa.awarding_sub_tier_agency_c,''-none-'') || ''_'' ||
        ''-none-'' || ''_'' ||
        coalesce(pafa.uri, ''-none-'') as generated_unique_award_id,
    pafa.assistance_type as type,
    case
        when pafa.assistance_type = ''02'' then ''Block Grant''
        when pafa.assistance_type = ''03'' then ''Formula Grant''
        when pafa.assistance_type = ''04'' then ''Project Grant''
        when pafa.assistance_type = ''05'' then ''Cooperative Agreement''
        when pafa.assistance_type = ''06'' then ''Direct Payment for Specified Use''
        when pafa.assistance_type = ''07'' then ''Direct Loan''
        when pafa.assistance_type = ''08'' then ''Guaranteed/Insured Loan''
        when pafa.assistance_type = ''09'' then ''Insurance''
        when pafa.assistance_type = ''10'' then ''Direct Payment with Unrestricted Use''
        when pafa.assistance_type = ''11'' then ''Other Financial Assistance''
    end as type_description,
    null::text as agency_id,
    null::text as referenced_idv_agency_iden,
    null::text as referenced_idv_agency_desc,
    null::text as multiple_or_single_award_i,
    null::text as multiple_or_single_aw_desc,
    null::text as piid,
    null::text as parent_award_piid,
    null::text as fain,
    pafa.uri as uri,
    sum(coalesce(pafa.federal_action_obligation::double precision, 0::double precision)) over w as total_obligation,
    sum(coalesce(pafa.original_loan_subsidy_cost::double precision, 0::double precision)) over w as total_subsidy_cost,
    null::float as total_outlay,
    pafa.awarding_agency_code as awarding_agency_code,
    pafa.awarding_agency_name as awarding_agency_name,
    pafa.awarding_sub_tier_agency_c as awarding_sub_tier_agency_c,
    pafa.awarding_sub_tier_agency_n as awarding_sub_tier_agency_n,
    pafa.awarding_office_code as awarding_office_code,
    pafa.awarding_office_name as awarding_office_name,
    pafa.funding_agency_code as funding_agency_code,
    pafa.funding_agency_name as funding_agency_name,
    pafa.funding_sub_tier_agency_co as funding_sub_tier_agency_co,
    pafa.funding_sub_tier_agency_na as funding_sub_tier_agency_na,
    pafa.funding_office_code as funding_office_code,
    pafa.funding_office_name as funding_office_name,
    ''DBR''::text as data_source,
    pafa.action_date::date as action_date,
    min(pafa.action_date) over w as date_signed,
    pafa.award_description as description,
    -- TODO: Handle when period_of_performance_star/period_of_performance_curr is ''
    min(pafa.period_of_performance_star::date) over w as period_of_performance_start_date,
    max(pafa.period_of_performance_curr::date) over w as period_of_performance_current_end_date,
    null::float as potential_total_value_of_award,
    null::float as base_and_all_options_value,
    pafa.modified_at::date as last_modified_date,
    max(pafa.action_date) over w as certified_date,
    pafa.record_type as record_type,
    ''asst_tx_'' || pafa.afa_generated_unique as latest_transaction_unique_id,
    0 as total_subaward_amount,
    0 as subaward_count,
    null::text as pulled_from,
    null::text as product_or_service_code,
    null::text as product_or_service_co_desc,
    null::text as extent_competed,
    null::text as extent_compete_description,
    null::text as type_of_contract_pricing,
    null::text as type_of_contract_pric_desc,
    null::text as contract_award_type_desc,
    null::text as cost_or_pricing_data,
    null::text as cost_or_pricing_data_desc,
    null::text as domestic_or_foreign_entity,
    null::text as domestic_or_foreign_e_desc,
    null::text as fair_opportunity_limited_s,
    null::text as fair_opportunity_limi_desc,
    null::text as foreign_funding,
    null::text as foreign_funding_desc,
    null::text as interagency_contracting_au,
    null::text as interagency_contract_desc,
    null::text as major_program,
    null::text as price_evaluation_adjustmen,
    null::text as program_acronym,
    null::text as subcontracting_plan,
    null::text as subcontracting_plan_desc,
    null::text as multi_year_contract,
    null::text as multi_year_contract_desc,
    null::text as purchase_card_as_payment_m,
    null::text as purchase_card_as_paym_desc,
    null::text as consolidated_contract,
    null::text as consolidated_contract_desc,
    null::text as solicitation_identifier,
    null::text as solicitation_procedures,
    null::text as solicitation_procedur_desc,
    null::text as number_of_offers_received,
    null::text as other_than_full_and_open_c,
    null::text as other_than_full_and_o_desc,
    null::text as commercial_item_acquisitio,
    null::text as commercial_item_acqui_desc,
    null::text as commercial_item_test_progr,
    null::text as commercial_item_test_desc,
    null::text as evaluated_preference,
    null::text as evaluated_preference_desc,
    null::text as fed_biz_opps,
    null::text as fed_biz_opps_description,
    null::text as small_business_competitive,
    null::text as dod_claimant_program_code,
    null::text as dod_claimant_prog_cod_desc,
    null::text as program_system_or_equipmen,
    null::text as program_system_or_equ_desc,
    null::text as information_technology_com,
    null::text as information_technolog_desc,
    null::text as sea_transportation,
    null::text as sea_transportation_desc,
    null::text as clinger_cohen_act_planning,
    null::text as clinger_cohen_act_pla_desc,
    null::text as davis_bacon_act,
    null::text as davis_bacon_act_descrip,
    null::text as service_contract_act,
    null::text as service_contract_act_desc,
    null::text as walsh_healey_act,
    null::text as walsh_healey_act_descrip,
    null::text as naics,
    null::text as naics_description,
    null::text as parent_award_id,
    null::text as idv_type,
    null::text as idv_type_description,
    null::text as type_set_aside,
    null::text as type_set_aside_description,
    pafa.assistance_type as assistance_type,
    pafa.business_funds_indicator as business_funds_indicator,
    pafa.business_types as business_types,
    pafa.cfda_number as cfda_number,
    pafa.cfda_title as cfda_title,

    -- recipient data
    pafa.awardee_or_recipient_uniqu as recipient_unique_id,
    pafa.awardee_or_recipient_legal as recipient_name,
    null::text as parent_recipient_unique_id,

    -- executive compensation data
    exec_comp.high_comp_officer1_full_na as officer_1_name,
    exec_comp.high_comp_officer1_amount as officer_1_amount,
    exec_comp.high_comp_officer2_full_na as officer_2_name,
    exec_comp.high_comp_officer2_amount as officer_2_amount,
    exec_comp.high_comp_officer3_full_na as officer_3_name,
    exec_comp.high_comp_officer3_amount as officer_3_amount,
    exec_comp.high_comp_officer4_full_na as officer_4_name,
    exec_comp.high_comp_officer4_amount as officer_4_amount,
    exec_comp.high_comp_officer5_full_na as officer_5_name,
    exec_comp.high_comp_officer5_amount as officer_5_amount,

    -- business categories
    pafa.legal_entity_address_line1 as recipient_location_address_line1,
    pafa.legal_entity_address_line2 as recipient_location_address_line2,
    pafa.legal_entity_address_line3 as recipient_location_address_line3,

    -- foreign province
    pafa.legal_entity_foreign_provi as recipient_location_foreign_province,
    pafa.legal_entity_foreign_city as recipient_location_foreign_city_name,
    pafa.legal_entity_foreign_posta as recipient_location_foreign_postal_code,

    -- country
    pafa.legal_entity_country_code as recipient_location_country_code,
    pafa.legal_entity_country_name as recipient_location_country_name,

    -- state
    pafa.legal_entity_state_code as recipient_location_state_code,
    pafa.legal_entity_state_name as recipient_location_state_name,

    -- county
    pafa.legal_entity_county_code as recipient_location_county_code,
    pafa.legal_entity_county_name as recipient_location_county_name,

    -- city
    pafa.legal_entity_city_code as recipient_location_city_code,
    pafa.legal_entity_city_name as recipient_location_city_name,

    -- zip
    pafa.legal_entity_zip5 as recipient_location_zip5,
    pafa.legal_entity_zip5 ||
        coalesce(pafa.legal_entity_zip_last4, '''') as recipient_location_zip4,

    -- congressional disctrict
    pafa.legal_entity_congressional as recipient_location_congressional_code,

    -- ppop data
    pafa.place_of_performance_code as pop_code,

    -- foreign
    place_of_performance_forei as pop_foreign_province,

    -- country
    pafa.place_of_perform_country_c as pop_country_code,
    pafa.place_of_perform_country_n as pop_country_name,

    -- state
    null::text as pop_state_code,
    pafa.place_of_perform_state_nam as pop_state_name,

    -- county
    pafa.place_of_perform_county_co as pop_county_code,
    pafa.place_of_perform_county_na as pop_county_name,

    -- city
    pafa.place_of_performance_city as pop_city_name,

    -- zip
    substring(pafa.place_of_performance_zip4a from 0 for 6) as pop_zip5,
    pafa.place_of_performance_zip4a as pop_zip4,

    -- congressional disctrict
    pafa.place_of_performance_congr as pop_congressional_code

from published_award_financial_assistance as pafa
    left outer join
    exec_comp_lookup as exec_comp on exec_comp.awardee_or_recipient_uniqu = pafa.awardee_or_recipient_uniqu
where pafa.record_type = ''1'' and is_active=TRUE
window w as (partition by pafa.uri, pafa.awarding_sub_tier_agency_c)
order by
    pafa.uri,
    pafa.awarding_sub_tier_agency_c,
    pafa.action_date desc,
    pafa.award_modification_amendme desc') as fabs_uri_uniq_awards
    (
        generated_unique_award_id text,
        type text,
        type_description text,
        agency_id text,
        referenced_idv_agency_iden text,
        referenced_idv_agency_desc text,
        multiple_or_single_award_i text,
        multiple_or_single_aw_desc text,
        piid text,
        parent_award_piid text,
        fain text,
        uri text,
        total_obligation float(2),
        total_subsidy_cost float(2),
        total_outlay float(2),
        awarding_agency_code text,
        awarding_agency_name text,
        awarding_sub_tier_agency_c text,
        awarding_sub_tier_agency_n text,
        awarding_office_code text,
        awarding_office_name text,
        funding_agency_code text,
        funding_agency_name text,
        funding_sub_tier_agency_co text,
        funding_sub_tier_agency_na text,
        funding_office_code text,
        funding_office_name text,
        data_source text,
        action_date date,
        date_signed date,
        description text,
        period_of_performance_start_date date,
        period_of_performance_current_end_date date,
        potential_total_value_of_award float(2),
        base_and_all_options_value float(2),
        last_modified_date date,
        certified_date date,
        record_type int,
        latest_transaction_unique_id text,
        total_subaward_amount float(2),
        subaward_count int,
        pulled_from text,
        product_or_service_code text,
        product_or_service_co_desc text,
        extent_competed text,
        extent_compete_description text,
        type_of_contract_pricing text,
        type_of_contract_pric_desc text,
        contract_award_type_desc text,
        cost_or_pricing_data text,
        cost_or_pricing_data_desc text,
        domestic_or_foreign_entity text,
        domestic_or_foreign_e_desc text,
        fair_opportunity_limited_s text,
        fair_opportunity_limi_desc text,
        foreign_funding text,
        foreign_funding_desc text,
        interagency_contracting_au text,
        interagency_contract_desc text,
        major_program text,
        price_evaluation_adjustmen text,
        program_acronym text,
        subcontracting_plan text,
        subcontracting_plan_desc text,
        multi_year_contract text,
        multi_year_contract_desc text,
        purchase_card_as_payment_m text,
        purchase_card_as_paym_desc text,
        consolidated_contract text,
        consolidated_contract_desc text,
        solicitation_identifier text,
        solicitation_procedures text,
        solicitation_procedur_desc text,
        number_of_offers_received text,
        other_than_full_and_open_c text,
        other_than_full_and_o_desc text,
        commercial_item_acquisitio text,
        commercial_item_acqui_desc text,
        commercial_item_test_progr text,
        commercial_item_test_desc text,
        evaluated_preference text,
        evaluated_preference_desc text,
        fed_biz_opps text,
        fed_biz_opps_description text,
        small_business_competitive text,
        dod_claimant_program_code text,
        dod_claimant_prog_cod_desc text,
        program_system_or_equipmen text,
        program_system_or_equ_desc text,
        information_technology_com text,
        information_technolog_desc text,
        sea_transportation text,
        sea_transportation_desc text,
        clinger_cohen_act_planning text,
        clinger_cohen_act_pla_desc text,
        davis_bacon_act text,
        davis_bacon_act_descrip text,
        service_contract_act text,
        service_contract_act_desc text,
        walsh_healey_act text,
        walsh_healey_act_descrip text,
        naics text,
        naics_description text,
        parent_award_id text,
        idv_type text,
        idv_type_description text,
        type_set_aside text,
        type_set_aside_description text,
        assistance_type text,
        business_funds_indicator text,
        business_types text,
        cfda_number text,
        cfda_title text,
        
        -- recipient data
        recipient_unique_id text, -- DUNS
        recipient_name text,
        parent_recipient_unique_id text,

        -- executive compensation data
        officer_1_name text,
        officer_1_amount text,
        officer_2_name text,
        officer_2_amount text,
        officer_3_name text,
        officer_3_amount text,
        officer_4_name text,
        officer_4_amount text,
        officer_5_name text,
        officer_5_amount text,

        -- business categories
        recipient_location_address_line1 text,
        recipient_location_address_line2 text,
        recipient_location_address_line3 text,
        
        -- foreign province
        recipient_location_foreign_province text,
        recipient_location_foreign_city_name text,
        recipient_location_foreign_postal_code text,
        
        -- country
        recipient_location_country_code text,
        recipient_location_country_name text,
        
        -- state
        recipient_location_state_code text,
        recipient_location_state_name text,
        
        -- county (NONE FOR FPDS)
        recipient_location_county_code text,
        recipient_location_county_name text,
        
        -- city
        recipient_location_city_code text,
        recipient_location_city_name text,

        -- zip
        recipient_location_zip5 text,
        recipient_location_zip4 text,
        
        -- congressional disctrict
        recipient_location_congressional_code text,
        
        -- ppop data
        pop_code text,
        
        -- foreign
        pop_foreign_province text,
        
        -- country
        pop_country_code text,
        pop_country_name text,
        
        -- state
        pop_state_code text,
        pop_state_name text,
        
        -- county
        pop_county_code text,
        pop_county_name text,
        
        -- city
        pop_city_name text,
        
        -- zip
        pop_zip5 text,
        pop_zip4 text,
        
        -- congressional disctrict
        pop_congressional_code text
    )
    inner join
    references_cfda as cfda on cfda.program_number = cfda_number
    inner join
    award_category as ac on ac.type_code = type
    inner join
    agency_lookup as awarding_agency on awarding_agency.subtier_code = awarding_sub_tier_agency_c
    left outer join
    agency_lookup as funding_agency on funding_agency.subtier_code = funding_sub_tier_agency_co)
);