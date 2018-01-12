CREATE MATERIALIZED VIEW award_matview_new AS (
(SELECT
    generated_unique_award_id,
    type,
    type_description,
    fpds_uniq_awards.agency_id AS agency_id,
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
    awarding_agency.toptier_abbr AS awarding_agency_abbr,
    awarding_sub_tier_agency_c,
    awarding_sub_tier_agency_n,
    awarding_agency.subtier_abbr AS awarding_sub_tier_agency_abbr,
    awarding_office_code,
    awarding_office_name,
    funding_agency_code,
    funding_agency_name,
    funding_agency.toptier_abbr AS funding_agency_abbr,
    funding_sub_tier_agency_co,
    funding_sub_tier_agency_na,
    funding_agency.subtier_abbr AS funding_sub_tier_agency_abbr,
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
    business_categories,
    cfda_number,
    cfda_title,
    NULL::text AS cfda_objectives,

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

    ac.type_name AS category,
    awarding_agency.agency_id AS awarding_agency_id,
    funding_agency.agency_id AS funding_agency_id,
    fy(action_date) AS fiscal_year
FROM
    dblink ('broker_server', 'SELECT
        DISTINCT ON (tf.piid, tf.parent_award_id, tf.agency_id, tf.referenced_idv_agency_iden)
        ''cont_aw_'' ||
            coalesce(tf.agency_id,''-none-'') || ''_'' ||
            coalesce(tf.referenced_idv_agency_iden,''-none-'') || ''_'' ||
            coalesce(tf.piid,''-none-'') || ''_'' ||
            coalesce(tf.parent_award_id,''-none-'') AS generated_unique_award_id,
        tf.contract_award_type AS type,
        tf.contract_award_type_desc AS type_description,
        tf.agency_id AS agency_id,
        tf.referenced_idv_agency_iden AS referenced_idv_agency_iden,
        tf.referenced_idv_agency_desc AS referenced_idv_agency_desc,
        tf.multiple_or_single_award_i AS multiple_or_single_award_i,
        tf.multiple_or_single_aw_desc AS multiple_or_single_aw_desc,
        tf.piid AS piid,
        tf.parent_award_id AS parent_award_piid,
        NULL::text AS fain,
        NULL::text AS uri,
        sum(coalesce(tf.federal_action_obligation::double precision, 0::double precision)) over w AS total_obligation,
        NULL::float AS total_subsidy_cost,
        NULL::float AS total_outlay,
        tf.awarding_agency_code AS awarding_agency_code,
        tf.awarding_agency_name AS awarding_agency_name,
        tf.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
        tf.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
        tf.awarding_office_code AS awarding_office_code,
        tf.awarding_office_name AS awarding_office_name,
        tf.funding_agency_code AS funding_agency_code,
        tf.funding_agency_name AS funding_agency_name,
        tf.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
        tf.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
        tf.funding_office_code AS funding_office_code,
        tf.funding_office_name AS funding_office_name,
        ''DBR''::text AS data_source,
        tf.action_date::date AS action_date,
        MIN(tf.action_date) over w AS date_signed,
        tf.award_description AS description,
        -- TODO: Handle when period_of_performance_star/period_of_performance_curr is ''
        MIN(tf.period_of_performance_star::date) over w AS period_of_performance_start_date,
        MAX(tf.period_of_performance_curr::date) over w AS period_of_performance_current_end_date,
        NULL::float AS potential_total_value_of_award,
        sum(coalesce(tf.base_and_all_options_value::double precision, 0::double precision)) over w AS base_and_all_options_value,
        tf.last_modified::date AS last_modified_date,
        MAX(tf.action_date) over w AS certified_date,
        NULL::int AS record_type,
        ''cont_tx_'' || tf.detached_award_proc_unique AS latest_transaction_unique_id,
        0 AS total_subaward_amount,
        0 AS subaward_count,
        tf.pulled_from AS pulled_from,
        tf.product_or_service_code AS product_or_service_code,
        tf.product_or_service_co_desc AS product_or_service_co_desc,
        tf.extent_competed AS extent_competed,
        tf.extent_compete_description AS extent_compete_description,
        tf.type_of_contract_pricing AS type_of_contract_pricing,
        tf.type_of_contract_pric_desc AS type_of_contract_pric_desc,
        tf.contract_award_type_desc AS contract_award_type_desc,
        tf.cost_or_pricing_data AS cost_or_pricing_data,
        tf.cost_or_pricing_data_desc AS cost_or_pricing_data_desc,
        tf.domestic_or_foreign_entity AS domestic_or_foreign_entity,
        tf.domestic_or_foreign_e_desc AS domestic_or_foreign_e_desc,
        tf.fair_opportunity_limited_s AS fair_opportunity_limited_s,
        tf.fair_opportunity_limi_desc AS fair_opportunity_limi_desc,
        tf.foreign_funding AS foreign_funding,
        tf.foreign_funding_desc AS foreign_funding_desc,
        tf.interagency_contracting_au AS interagency_contracting_au,
        tf.interagency_contract_desc AS interagency_contract_desc,
        tf.major_program AS major_program,
        tf.price_evaluation_adjustmen AS price_evaluation_adjustmen,
        tf.program_acronym AS program_acronym,
        tf.subcontracting_plan AS subcontracting_plan,
        tf.subcontracting_plan_desc AS subcontracting_plan_desc,
        tf.multi_year_contract AS multi_year_contract,
        tf.multi_year_contract_desc AS multi_year_contract_desc,
        tf.purchase_card_as_payment_m AS purchase_card_as_payment_m,
        tf.purchase_card_as_paym_desc AS purchase_card_as_paym_desc,
        tf.consolidated_contract AS consolidated_contract,
        tf.consolidated_contract_desc AS consolidated_contract_desc,
        tf.solicitation_identifier AS solicitation_identifier,
        tf.solicitation_procedures AS solicitation_procedures,
        tf.solicitation_procedur_desc AS solicitation_procedur_desc,
        tf.number_of_offers_received AS number_of_offers_received,
        tf.other_than_full_and_open_c AS other_than_full_and_open_c,
        tf.other_than_full_and_o_desc AS other_than_full_and_o_desc,
        tf.commercial_item_acquisitio AS commercial_item_acquisitio,
        tf.commercial_item_acqui_desc AS commercial_item_acqui_desc,
        tf.commercial_item_test_progr AS commercial_item_test_progr,
        tf.commercial_item_test_desc AS commercial_item_test_desc,
        tf.evaluated_preference AS evaluated_preference,
        tf.evaluated_preference_desc AS evaluated_preference_desc,
        tf.fed_biz_opps AS fed_biz_opps,
        tf.fed_biz_opps_description AS fed_biz_opps_description,
        tf.small_business_competitive AS small_business_competitive,
        tf.dod_claimant_program_code AS dod_claimant_program_code,
        tf.dod_claimant_prog_cod_desc AS dod_claimant_prog_cod_desc,
        tf.program_system_or_equipmen AS program_system_or_equipmen,
        tf.program_system_or_equ_desc AS program_system_or_equ_desc,
        tf.information_technology_com AS information_technology_com,
        tf.information_technolog_desc AS information_technolog_desc,
        tf.sea_transportation AS sea_transportation,
        tf.sea_transportation_desc AS sea_transportation_desc,
        tf.clinger_cohen_act_planning AS clinger_cohen_act_planning,
        tf.clinger_cohen_act_pla_desc AS clinger_cohen_act_pla_desc,
        tf.davis_bacon_act AS davis_bacon_act,
        tf.davis_bacon_act_descrip AS davis_bacon_act_descrip,
        tf.service_contract_act AS service_contract_act,
        tf.service_contract_act_desc AS service_contract_act_desc,
        tf.walsh_healey_act AS walsh_healey_act,
        tf.walsh_healey_act_descrip AS walsh_healey_act_descrip,
        tf.naics AS naics,
        tf.naics_description AS naics_description,
        tf.parent_award_id AS parent_award_id,
        tf.idv_type AS idv_type,
        tf.idv_type_description AS idv_type_description,
        tf.type_set_aside AS type_set_aside,
        tf.type_set_aside_description AS type_set_aside_description,
        NULL::text AS assistance_type,
        NULL::text AS business_funds_indicator,
        NULL::text AS business_types,
        compile_fpds_business_categories(tf.small_business_competitive, tf.for_profit_organization, tf.alaskan_native_owned_corpo, tf.american_indian_owned_busi, tf.asian_pacific_american_own, tf.black_american_owned_busin, tf.hispanic_american_owned_bu, tf.native_american_owned_busi, tf.native_hawaiian_owned_busi, tf.subcontinent_asian_asian_i, tf.tribally_owned_business, tf.other_minority_owned_busin, tf.minority_owned_business, tf.women_owned_small_business, tf.economically_disadvantaged, tf.joint_venture_women_owned, tf.joint_venture_economically, tf.woman_owned_business, tf.service_disabled_veteran_o, tf.veteran_owned_business, tf.c8a_program_participant, tf.the_ability_one_program, tf.dot_certified_disadvantage, tf.emerging_small_business, tf.federally_funded_research, tf.historically_underutilized, tf.labor_surplus_area_firm, tf.sba_certified_8_a_joint_ve, tf.self_certified_small_disad, tf.small_agricultural_coopera, tf.small_disadvantaged_busine, tf.community_developed_corpor, tf.domestic_or_foreign_entity, tf.foreign_owned_and_located, tf.foreign_government, tf.international_organization, tf.foundation, tf.community_development_corp, tf.nonprofit_organization, tf.other_not_for_profit_organ, tf.state_controlled_instituti, tf.c1862_land_grant_college, tf.c1890_land_grant_college, tf.c1994_land_grant_college, tf.private_university_or_coll, tf.minority_institution, tf.historically_black_college, tf.tribal_college, tf.alaskan_native_servicing_i, tf.native_hawaiian_servicing, tf.hispanic_servicing_institu, tf.us_federal_government, tf.federal_agency, tf.us_government_entity, tf.interstate_entity, tf.us_state_government, tf.council_of_governments, tf.city_local_government, tf.county_local_government, tf.inter_municipal_local_gove, tf.municipality_local_governm, tf.township_local_government, tf.us_local_government, tf.local_government_owned, tf.school_district_local_gove, tf.us_tribal_government, tf.indian_tribe_federally_rec, tf.housing_authorities_public, tf.airport_authority, tf.port_authority, tf.transit_authority, tf.planning_commission) AS business_categories,
        NULL::text AS cfda_number,
        NULL::text AS cfda_title,

        -- recipient data
        tf.awardee_or_recipient_uniqu AS recipient_unique_id, -- DUNS
        tf.awardee_or_recipient_legal AS recipient_name,
        tf.ultimate_parent_unique_ide AS parent_recipient_unique_id,

        -- executive compensation data
        exec_comp.high_comp_officer1_full_na AS officer_1_name,
        exec_comp.high_comp_officer1_amount AS officer_1_amount,
        exec_comp.high_comp_officer2_full_na AS officer_2_name,
        exec_comp.high_comp_officer2_amount AS officer_2_amount,
        exec_comp.high_comp_officer3_full_na AS officer_3_name,
        exec_comp.high_comp_officer3_amount AS officer_3_amount,
        exec_comp.high_comp_officer4_full_na AS officer_4_name,
        exec_comp.high_comp_officer4_amount AS officer_4_amount,
        exec_comp.high_comp_officer5_full_na AS officer_5_name,
        exec_comp.high_comp_officer5_amount AS officer_5_amount,

        -- business categories
        tf.legal_entity_address_line1 AS recipient_location_address_line1,
        tf.legal_entity_address_line2 AS recipient_location_address_line2,
        tf.legal_entity_address_line3 AS recipient_location_address_line3,

        -- foreign province
        NULL::text AS recipient_location_foreign_province,
        NULL::text AS recipient_location_foreign_city_name,
        NULL::text AS recipient_location_foreign_city_name,

        -- country
        tf.legal_entity_country_code AS recipient_location_country_code,
        tf.legal_entity_country_name AS recipient_location_country_name,

        -- state
        tf.legal_entity_state_code AS recipient_location_state_code,
        tf.legal_entity_state_descrip AS recipient_location_state_name,

        -- county (NONE FOR FPDS)
        NULL::text AS recipient_location_county_code,
        NULL::text AS recipient_location_county_name,

        -- city
        NULL::text AS recipient_location_city_code,
        tf.legal_entity_city_name AS recipient_location_city_name,

        -- zip
        NULL::text AS recipient_location_zip5,
        tf.legal_entity_zip4 AS recipient_location_zip4,

        -- congressional disctrict
        tf.legal_entity_congressional AS recipient_location_congressional_code,

        -- ppop data
        NULL::text AS pop_code,

        -- foreign
        NULL::text AS pop_foreign_province,

        -- country
        tf.place_of_perform_country_c AS pop_country_code,
        tf.place_of_perf_country_desc AS pop_country_name,

        -- state
        tf.place_of_performance_state AS pop_state_code,
        tf.place_of_perfor_state_desc AS pop_state_name,

        -- county
        NULL::text AS pop_county_code,
        tf.place_of_perform_county_na AS pop_county_name,

        -- city
        tf.place_of_perform_city_name AS pop_city_name,

        -- zip
        SUBSTRING(tf.place_of_performance_zip4a FROM 0 FOR 6) AS pop_zip5,
        tf.place_of_performance_zip4a AS pop_zip4,

        -- congressional disctrict
        tf.place_of_performance_congr AS pop_congressional_code
    FROM
        detached_award_procurement tf -- aka latest transaction
        LEFT OUTER JOIN
        exec_comp_lookup AS exec_comp ON exec_comp.awardee_or_recipient_uniqu = tf.awardee_or_recipient_uniqu
    window w AS (partition BY tf.piid, tf.parent_award_id, tf.agency_id, tf.referenced_idv_agency_iden)
    ORDER BY
        tf.piid,
        tf.parent_award_id,
        tf.agency_id,
        tf.referenced_idv_agency_iden,
        tf.action_date desc,
        tf.award_modification_amendme desc,
        tf.transaction_number desc') AS fpds_uniq_awards
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
        business_categories text[],
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
    INNER JOIN
    award_category AS ac ON ac.type_code = type
    INNER JOIN
    agency_lookup AS awarding_agency ON awarding_agency.subtier_code = awarding_sub_tier_agency_c
    LEFT OUTER JOIN
    agency_lookup AS funding_agency ON funding_agency.subtier_code = funding_sub_tier_agency_co)

UNION ALL

(SELECT
    generated_unique_award_id,
    type,
    type_description,
    fabs_fain_uniq_awards.agency_id AS agency_id,
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
    awarding_agency.toptier_abbr AS awarding_agency_abbr,
    awarding_sub_tier_agency_c,
    awarding_sub_tier_agency_n,
    awarding_agency.subtier_abbr AS awarding_sub_tier_agency_abbr,
    awarding_office_code,
    awarding_office_name,
    funding_agency_code,
    funding_agency_name,
    funding_agency.toptier_abbr AS funding_agency_abbr,
    funding_sub_tier_agency_co,
    funding_sub_tier_agency_na,
    funding_agency.subtier_abbr AS funding_sub_tier_agency_abbr,
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
    business_categories,
    cfda_number,
    cfda_title,
    cfda.objectives AS cfda_objectives,

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

    ac.type_name AS category,
    awarding_agency.agency_id AS awarding_agency_id,
    funding_agency.agency_id AS funding_agency_id,
    fy(action_date) AS fiscal_year
FROM
    dblink ('broker_server', 'SELECT
    DISTINCT ON (pafa.fain, pafa.awarding_sub_tier_agency_c)
    ''asst_aw_'' ||
        coalesce(pafa.awarding_sub_tier_agency_c,''-none-'') || ''_'' ||
        coalesce(pafa.fain, ''-none-'') || ''_'' ||
        ''-none-'' AS generated_unique_award_id,
    pafa.assistance_type AS type,
    CASE
        WHEN pafa.assistance_type = ''02'' THEN ''Block Grant''
        WHEN pafa.assistance_type = ''03'' THEN ''Formula Grant''
        WHEN pafa.assistance_type = ''04'' THEN ''Project Grant''
        WHEN pafa.assistance_type = ''05'' THEN ''Cooperative Agreement''
        WHEN pafa.assistance_type = ''06'' THEN ''Direct Payment for Specified Use''
        WHEN pafa.assistance_type = ''07'' THEN ''Direct Loan''
        WHEN pafa.assistance_type = ''08'' THEN ''Guaranteed/Insured Loan''
        WHEN pafa.assistance_type = ''09'' THEN ''Insurance''
        WHEN pafa.assistance_type = ''10'' THEN ''Direct Payment with Unrestricted Use''
        WHEN pafa.assistance_type = ''11'' THEN ''Other Financial Assistance''
    END AS type_description,
    NULL::text AS agency_id,
    NULL::text AS referenced_idv_agency_iden,
    NULL::text AS referenced_idv_agency_desc,
    NULL::text AS multiple_or_single_award_i,
    NULL::text AS multiple_or_single_aw_desc,
    NULL::text AS piid,
    NULL::text AS parent_award_piid,
    pafa.fain AS fain,
    NULL::text AS uri,
    sum(coalesce(pafa.federal_action_obligation::double precision, 0::double precision)) over w AS total_obligation,
    sum(coalesce(pafa.original_loan_subsidy_cost::double precision, 0::double precision)) over w AS total_subsidy_cost,
    NULL::float AS total_outlay,
    pafa.awarding_agency_code AS awarding_agency_code,
    pafa.awarding_agency_name AS awarding_agency_name,
    pafa.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
    pafa.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
    pafa.awarding_office_code AS awarding_office_code,
    pafa.awarding_office_name AS awarding_office_name,
    pafa.funding_agency_code AS funding_agency_code,
    pafa.funding_agency_name AS funding_agency_name,
    pafa.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
    pafa.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
    pafa.funding_office_code AS funding_office_code,
    pafa.funding_office_name AS funding_office_name,
    ''DBR''::text AS data_source,
    pafa.action_date::date AS action_date,
    MIN(pafa.action_date) over w AS date_signed,
    pafa.award_description AS description,
    -- TODO: Handle when period_of_performance_star/period_of_performance_curr is ''
    MIN(pafa.period_of_performance_star::date) over w AS period_of_performance_start_date,
    MAX(pafa.period_of_performance_curr::date) over w AS period_of_performance_current_end_date,
    NULL::float AS potential_total_value_of_award,
    NULL::float AS base_and_all_options_value,
    pafa.modified_at::date AS last_modified_date,
    MAX(pafa.action_date) over w AS certified_date,
    pafa.record_type AS record_type,
    ''asst_tx_'' || pafa.afa_generated_unique AS latest_transaction_unique_id,
    0 AS total_subaward_amount,
    0 AS subaward_count,
    NULL::text AS pulled_from,
    NULL::text AS product_or_service_code,
    NULL::text AS product_or_service_co_desc,
    NULL::text AS extent_competed,
    NULL::text AS extent_compete_description,
    NULL::text AS type_of_contract_pricing,
    NULL::text AS type_of_contract_pric_desc,
    NULL::text AS contract_award_type_desc,
    NULL::text AS cost_or_pricing_data,
    NULL::text AS cost_or_pricing_data_desc,
    NULL::text AS domestic_or_foreign_entity,
    NULL::text AS domestic_or_foreign_e_desc,
    NULL::text AS fair_opportunity_limited_s,
    NULL::text AS fair_opportunity_limi_desc,
    NULL::text AS foreign_funding,
    NULL::text AS foreign_funding_desc,
    NULL::text AS interagency_contracting_au,
    NULL::text AS interagency_contract_desc,
    NULL::text AS major_program,
    NULL::text AS price_evaluation_adjustmen,
    NULL::text AS program_acronym,
    NULL::text AS subcontracting_plan,
    NULL::text AS subcontracting_plan_desc,
    NULL::text AS multi_year_contract,
    NULL::text AS multi_year_contract_desc,
    NULL::text AS purchase_card_as_payment_m,
    NULL::text AS purchase_card_as_paym_desc,
    NULL::text AS consolidated_contract,
    NULL::text AS consolidated_contract_desc,
    NULL::text AS solicitation_identifier,
    NULL::text AS solicitation_procedures,
    NULL::text AS solicitation_procedur_desc,
    NULL::text AS number_of_offers_received,
    NULL::text AS other_than_full_and_open_c,
    NULL::text AS other_than_full_and_o_desc,
    NULL::text AS commercial_item_acquisitio,
    NULL::text AS commercial_item_acqui_desc,
    NULL::text AS commercial_item_test_progr,
    NULL::text AS commercial_item_test_desc,
    NULL::text AS evaluated_preference,
    NULL::text AS evaluated_preference_desc,
    NULL::text AS fed_biz_opps,
    NULL::text AS fed_biz_opps_description,
    NULL::text AS small_business_competitive,
    NULL::text AS dod_claimant_program_code,
    NULL::text AS dod_claimant_prog_cod_desc,
    NULL::text AS program_system_or_equipmen,
    NULL::text AS program_system_or_equ_desc,
    NULL::text AS information_technology_com,
    NULL::text AS information_technolog_desc,
    NULL::text AS sea_transportation,
    NULL::text AS sea_transportation_desc,
    NULL::text AS clinger_cohen_act_planning,
    NULL::text AS clinger_cohen_act_pla_desc,
    NULL::text AS davis_bacon_act,
    NULL::text AS davis_bacon_act_descrip,
    NULL::text AS service_contract_act,
    NULL::text AS service_contract_act_desc,
    NULL::text AS walsh_healey_act,
    NULL::text AS walsh_healey_act_descrip,
    NULL::text AS naics,
    NULL::text AS naics_description,
    NULL::text AS parent_award_id,
    NULL::text AS idv_type,
    NULL::text AS idv_type_description,
    NULL::text AS type_set_aside,
    NULL::text AS type_set_aside_description,
    pafa.assistance_type AS assistance_type,
    pafa.business_funds_indicator AS business_funds_indicator,
    pafa.business_types AS business_types,
    compile_fabs_business_categories(pafa.business_types) AS business_categories,
    pafa.cfda_number AS cfda_number,
    pafa.cfda_title AS cfda_title,

    -- recipient data
    pafa.awardee_or_recipient_uniqu AS recipient_unique_id,
    pafa.awardee_or_recipient_legal AS recipient_name,
    NULL::text AS parent_recipient_unique_id,

    -- executive compensation data
    exec_comp.high_comp_officer1_full_na AS officer_1_name,
    exec_comp.high_comp_officer1_amount AS officer_1_amount,
    exec_comp.high_comp_officer2_full_na AS officer_2_name,
    exec_comp.high_comp_officer2_amount AS officer_2_amount,
    exec_comp.high_comp_officer3_full_na AS officer_3_name,
    exec_comp.high_comp_officer3_amount AS officer_3_amount,
    exec_comp.high_comp_officer4_full_na AS officer_4_name,
    exec_comp.high_comp_officer4_amount AS officer_4_amount,
    exec_comp.high_comp_officer5_full_na AS officer_5_name,
    exec_comp.high_comp_officer5_amount AS officer_5_amount,

    -- business categories
    pafa.legal_entity_address_line1 AS recipient_location_address_line1,
    pafa.legal_entity_address_line2 AS recipient_location_address_line2,
    pafa.legal_entity_address_line3 AS recipient_location_address_line3,

    -- foreign province
    pafa.legal_entity_foreign_provi AS recipient_location_foreign_province,
    pafa.legal_entity_foreign_city AS recipient_location_foreign_city_name,
    pafa.legal_entity_foreign_posta AS recipient_location_foreign_postal_code,

    -- country
    pafa.legal_entity_country_code AS recipient_location_country_code,
    pafa.legal_entity_country_name AS recipient_location_country_name,

    -- state
    pafa.legal_entity_state_code AS recipient_location_state_code,
    pafa.legal_entity_state_name AS recipient_location_state_name,

    -- county
    pafa.legal_entity_county_code AS recipient_location_county_code,
    pafa.legal_entity_county_name AS recipient_location_county_name,

    -- city
    pafa.legal_entity_city_code AS recipient_location_city_code,
    pafa.legal_entity_city_name AS recipient_location_city_name,

    -- zip
    pafa.legal_entity_zip5 AS recipient_location_zip5,
    pafa.legal_entity_zip5 || coalesce(pafa.legal_entity_zip_last4, '''') AS recipient_location_zip4,

    -- congressional disctrict
    pafa.legal_entity_congressional AS recipient_location_congressional_code,

    -- ppop data
    pafa.place_of_performance_code AS pop_code,

    -- foreign
    pafa.place_of_performance_forei AS pop_foreign_province,

    -- country
    pafa.place_of_perform_country_c AS pop_country_code,
    pafa.place_of_perform_country_n AS pop_country_name,

    -- state
    NULL::text AS pop_state_code,
    pafa.place_of_perform_state_nam AS pop_state_name,

    -- county
    pafa.place_of_perform_county_co AS pop_county_code,
    pafa.place_of_perform_county_na AS pop_county_name,

    -- city
    pafa.place_of_performance_city AS pop_city_name,

    -- zip
    SUBSTRING(pafa.place_of_performance_zip4a FROM 0 FOR 6) AS pop_zip5,
    pafa.place_of_performance_zip4a AS pop_zip4,

    -- congressional disctrict
    pafa.place_of_performance_congr AS pop_congressional_code

FROM published_award_financial_assistance AS pafa
    LEFT OUTER JOIN
    exec_comp_lookup AS exec_comp ON exec_comp.awardee_or_recipient_uniqu = pafa.awardee_or_recipient_uniqu
WHERE pafa.record_type = ''2'' AND is_active=TRUE
window w AS (partition BY pafa.fain, pafa.awarding_sub_tier_agency_c)
ORDER BY
    pafa.fain,
    pafa.awarding_sub_tier_agency_c,
    pafa.action_date desc,
    pafa.award_modification_amendme desc
;') AS fabs_fain_uniq_awards
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
        business_categories text[],
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
    INNER JOIN
    references_cfda AS cfda ON cfda.program_number = cfda_number
    INNER JOIN
    award_category AS ac ON ac.type_code = type
    INNER JOIN
    agency_lookup AS awarding_agency ON awarding_agency.subtier_code = awarding_sub_tier_agency_c
    LEFT OUTER JOIN
    agency_lookup AS funding_agency ON funding_agency.subtier_code = funding_sub_tier_agency_co)

UNION ALL

(SELECT
    generated_unique_award_id,
    type,
    type_description,
    fabs_uri_uniq_awards.agency_id AS agency_id,
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
    awarding_agency.toptier_abbr AS awarding_agency_abbr,
    awarding_sub_tier_agency_c,
    awarding_sub_tier_agency_n,
    awarding_agency.subtier_abbr AS awarding_sub_tier_agency_abbr,
    awarding_office_code,
    awarding_office_name,
    funding_agency_code,
    funding_agency_name,
    funding_agency.toptier_abbr AS funding_agency_abbr,
    funding_sub_tier_agency_co,
    funding_sub_tier_agency_na,
    funding_agency.subtier_abbr AS funding_sub_tier_agency_abbr,
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
    business_categories,
    cfda_number,
    cfda_title,
    cfda.objectives AS cfda_objectives,

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

    ac.type_name AS category,
    awarding_agency.agency_id AS awarding_agency_id,
    funding_agency.agency_id AS funding_agency_id,
    fy(action_date) AS fiscal_year
FROM
    dblink ('broker_server', 'SELECT
    DISTINCT ON (pafa.uri, pafa.awarding_sub_tier_agency_c)
    ''asst_aw_'' ||
        coalesce(pafa.awarding_sub_tier_agency_c,''-none-'') || ''_'' ||
        ''-none-'' || ''_'' ||
        coalesce(pafa.uri, ''-none-'') AS generated_unique_award_id,
    pafa.assistance_type AS type,
    CASE
        WHEN pafa.assistance_type = ''02'' THEN ''Block Grant''
        WHEN pafa.assistance_type = ''03'' THEN ''Formula Grant''
        WHEN pafa.assistance_type = ''04'' THEN ''Project Grant''
        WHEN pafa.assistance_type = ''05'' THEN ''Cooperative Agreement''
        WHEN pafa.assistance_type = ''06'' THEN ''Direct Payment for Specified Use''
        WHEN pafa.assistance_type = ''07'' THEN ''Direct Loan''
        WHEN pafa.assistance_type = ''08'' THEN ''Guaranteed/Insured Loan''
        WHEN pafa.assistance_type = ''09'' THEN ''Insurance''
        WHEN pafa.assistance_type = ''10'' THEN ''Direct Payment with Unrestricted Use''
        WHEN pafa.assistance_type = ''11'' THEN ''Other Financial Assistance''
    END AS type_description,
    NULL::text AS agency_id,
    NULL::text AS referenced_idv_agency_iden,
    NULL::text AS referenced_idv_agency_desc,
    NULL::text AS multiple_or_single_award_i,
    NULL::text AS multiple_or_single_aw_desc,
    NULL::text AS piid,
    NULL::text AS parent_award_piid,
    NULL::text AS fain,
    pafa.uri AS uri,
    sum(coalesce(pafa.federal_action_obligation::double precision, 0::double precision)) over w AS total_obligation,
    sum(coalesce(pafa.original_loan_subsidy_cost::double precision, 0::double precision)) over w AS total_subsidy_cost,
    NULL::float AS total_outlay,
    pafa.awarding_agency_code AS awarding_agency_code,
    pafa.awarding_agency_name AS awarding_agency_name,
    pafa.awarding_sub_tier_agency_c AS awarding_sub_tier_agency_c,
    pafa.awarding_sub_tier_agency_n AS awarding_sub_tier_agency_n,
    pafa.awarding_office_code AS awarding_office_code,
    pafa.awarding_office_name AS awarding_office_name,
    pafa.funding_agency_code AS funding_agency_code,
    pafa.funding_agency_name AS funding_agency_name,
    pafa.funding_sub_tier_agency_co AS funding_sub_tier_agency_co,
    pafa.funding_sub_tier_agency_na AS funding_sub_tier_agency_na,
    pafa.funding_office_code AS funding_office_code,
    pafa.funding_office_name AS funding_office_name,
    ''DBR''::text AS data_source,
    pafa.action_date::date AS action_date,
    MIN(pafa.action_date) over w AS date_signed,
    pafa.award_description AS description,
    -- TODO: Handle when period_of_performance_star/period_of_performance_curr is ''
    MIN(pafa.period_of_performance_star::date) over w AS period_of_performance_start_date,
    MAX(pafa.period_of_performance_curr::date) over w AS period_of_performance_current_end_date,
    NULL::float AS potential_total_value_of_award,
    NULL::float AS base_and_all_options_value,
    pafa.modified_at::date AS last_modified_date,
    MAX(pafa.action_date) over w AS certified_date,
    pafa.record_type AS record_type,
    ''asst_tx_'' || pafa.afa_generated_unique AS latest_transaction_unique_id,
    0 AS total_subaward_amount,
    0 AS subaward_count,
    NULL::text AS pulled_from,
    NULL::text AS product_or_service_code,
    NULL::text AS product_or_service_co_desc,
    NULL::text AS extent_competed,
    NULL::text AS extent_compete_description,
    NULL::text AS type_of_contract_pricing,
    NULL::text AS type_of_contract_pric_desc,
    NULL::text AS contract_award_type_desc,
    NULL::text AS cost_or_pricing_data,
    NULL::text AS cost_or_pricing_data_desc,
    NULL::text AS domestic_or_foreign_entity,
    NULL::text AS domestic_or_foreign_e_desc,
    NULL::text AS fair_opportunity_limited_s,
    NULL::text AS fair_opportunity_limi_desc,
    NULL::text AS foreign_funding,
    NULL::text AS foreign_funding_desc,
    NULL::text AS interagency_contracting_au,
    NULL::text AS interagency_contract_desc,
    NULL::text AS major_program,
    NULL::text AS price_evaluation_adjustmen,
    NULL::text AS program_acronym,
    NULL::text AS subcontracting_plan,
    NULL::text AS subcontracting_plan_desc,
    NULL::text AS multi_year_contract,
    NULL::text AS multi_year_contract_desc,
    NULL::text AS purchase_card_as_payment_m,
    NULL::text AS purchase_card_as_paym_desc,
    NULL::text AS consolidated_contract,
    NULL::text AS consolidated_contract_desc,
    NULL::text AS solicitation_identifier,
    NULL::text AS solicitation_procedures,
    NULL::text AS solicitation_procedur_desc,
    NULL::text AS number_of_offers_received,
    NULL::text AS other_than_full_and_open_c,
    NULL::text AS other_than_full_and_o_desc,
    NULL::text AS commercial_item_acquisitio,
    NULL::text AS commercial_item_acqui_desc,
    NULL::text AS commercial_item_test_progr,
    NULL::text AS commercial_item_test_desc,
    NULL::text AS evaluated_preference,
    NULL::text AS evaluated_preference_desc,
    NULL::text AS fed_biz_opps,
    NULL::text AS fed_biz_opps_description,
    NULL::text AS small_business_competitive,
    NULL::text AS dod_claimant_program_code,
    NULL::text AS dod_claimant_prog_cod_desc,
    NULL::text AS program_system_or_equipmen,
    NULL::text AS program_system_or_equ_desc,
    NULL::text AS information_technology_com,
    NULL::text AS information_technolog_desc,
    NULL::text AS sea_transportation,
    NULL::text AS sea_transportation_desc,
    NULL::text AS clinger_cohen_act_planning,
    NULL::text AS clinger_cohen_act_pla_desc,
    NULL::text AS davis_bacon_act,
    NULL::text AS davis_bacon_act_descrip,
    NULL::text AS service_contract_act,
    NULL::text AS service_contract_act_desc,
    NULL::text AS walsh_healey_act,
    NULL::text AS walsh_healey_act_descrip,
    NULL::text AS naics,
    NULL::text AS naics_description,
    NULL::text AS parent_award_id,
    NULL::text AS idv_type,
    NULL::text AS idv_type_description,
    NULL::text AS type_set_aside,
    NULL::text AS type_set_aside_description,
    pafa.assistance_type AS assistance_type,
    pafa.business_funds_indicator AS business_funds_indicator,
    pafa.business_types AS business_types,
    compile_fabs_business_categories(pafa.business_types) AS business_categories,
    pafa.cfda_number AS cfda_number,
    pafa.cfda_title AS cfda_title,

    -- recipient data
    pafa.awardee_or_recipient_uniqu AS recipient_unique_id,
    pafa.awardee_or_recipient_legal AS recipient_name,
    NULL::text AS parent_recipient_unique_id,

    -- executive compensation data
    exec_comp.high_comp_officer1_full_na AS officer_1_name,
    exec_comp.high_comp_officer1_amount AS officer_1_amount,
    exec_comp.high_comp_officer2_full_na AS officer_2_name,
    exec_comp.high_comp_officer2_amount AS officer_2_amount,
    exec_comp.high_comp_officer3_full_na AS officer_3_name,
    exec_comp.high_comp_officer3_amount AS officer_3_amount,
    exec_comp.high_comp_officer4_full_na AS officer_4_name,
    exec_comp.high_comp_officer4_amount AS officer_4_amount,
    exec_comp.high_comp_officer5_full_na AS officer_5_name,
    exec_comp.high_comp_officer5_amount AS officer_5_amount,

    -- business categories
    pafa.legal_entity_address_line1 AS recipient_location_address_line1,
    pafa.legal_entity_address_line2 AS recipient_location_address_line2,
    pafa.legal_entity_address_line3 AS recipient_location_address_line3,

    -- foreign province
    pafa.legal_entity_foreign_provi AS recipient_location_foreign_province,
    pafa.legal_entity_foreign_city AS recipient_location_foreign_city_name,
    pafa.legal_entity_foreign_posta AS recipient_location_foreign_postal_code,

    -- country
    pafa.legal_entity_country_code AS recipient_location_country_code,
    pafa.legal_entity_country_name AS recipient_location_country_name,

    -- state
    pafa.legal_entity_state_code AS recipient_location_state_code,
    pafa.legal_entity_state_name AS recipient_location_state_name,

    -- county
    pafa.legal_entity_county_code AS recipient_location_county_code,
    pafa.legal_entity_county_name AS recipient_location_county_name,

    -- city
    pafa.legal_entity_city_code AS recipient_location_city_code,
    pafa.legal_entity_city_name AS recipient_location_city_name,

    -- zip
    pafa.legal_entity_zip5 AS recipient_location_zip5,
    pafa.legal_entity_zip5 ||
        coalesce(pafa.legal_entity_zip_last4, '''') AS recipient_location_zip4,

    -- congressional disctrict
    pafa.legal_entity_congressional AS recipient_location_congressional_code,

    -- ppop data
    pafa.place_of_performance_code AS pop_code,

    -- foreign
    place_of_performance_forei AS pop_foreign_province,

    -- country
    pafa.place_of_perform_country_c AS pop_country_code,
    pafa.place_of_perform_country_n AS pop_country_name,

    -- state
    NULL::text AS pop_state_code,
    pafa.place_of_perform_state_nam AS pop_state_name,

    -- county
    pafa.place_of_perform_county_co AS pop_county_code,
    pafa.place_of_perform_county_na AS pop_county_name,

    -- city
    pafa.place_of_performance_city AS pop_city_name,

    -- zip
    SUBSTRING(pafa.place_of_performance_zip4a FROM 0 FOR 6) AS pop_zip5,
    pafa.place_of_performance_zip4a AS pop_zip4,

    -- congressional disctrict
    pafa.place_of_performance_congr AS pop_congressional_code

FROM published_award_financial_assistance AS pafa
    LEFT OUTER JOIN
    exec_comp_lookup AS exec_comp ON exec_comp.awardee_or_recipient_uniqu = pafa.awardee_or_recipient_uniqu
WHERE pafa.record_type = ''1'' AND is_active=TRUE
window w AS (partition BY pafa.uri, pafa.awarding_sub_tier_agency_c)
ORDER BY
    pafa.uri,
    pafa.awarding_sub_tier_agency_c,
    pafa.action_date desc,
    pafa.award_modification_amendme desc') AS fabs_uri_uniq_awards
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
        business_categories text[],
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
    INNER JOIN
    references_cfda AS cfda ON cfda.program_number = cfda_number
    INNER JOIN
    award_category AS ac ON ac.type_code = type
    INNER JOIN
    agency_lookup AS awarding_agency ON awarding_agency.subtier_code = awarding_sub_tier_agency_c
    LEFT OUTER JOIN
    agency_lookup AS funding_agency ON funding_agency.subtier_code = funding_sub_tier_agency_co)
);

ALTER MATERIALIZED VIEW award_matview RENAME TO award_matview_old;
ALTER MATERIALIZED VIEW award_matview_new RENAME TO award_matview;
DROP MATERIALIZED VIEW award_matview_old;