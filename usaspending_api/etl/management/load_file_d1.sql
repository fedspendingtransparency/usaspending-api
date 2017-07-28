-- recipient locations from contracts
INSERT INTO references_location (
        data_source,
        country_name,
        state_code,
        state_name,
        state_description,
        city_name,
        city_code,
        county_name,
        county_code,
        address_line1,
        address_line2,
        address_line3,
        foreign_location_description,
        zip4,
        zip_4a,
        congressional_code,
        performance_code,
        zip_last4,
        zip5,
        foreign_postal_code,
        foreign_province,
        foreign_city_name,
        reporting_period_start,
        reporting_period_end,
        last_modified_date,
        certified_date,
        create_date,
        update_date,
        place_of_performance_flag,
        recipient_flag,
        location_country_code,
        award_procurement_ids
      )
SELECT
        'DBR', -- ==> data_source
        ref_country_code.country_name, -- ==> country_name
        REPLACE(p.legal_entity_state_code, '.', ''), -- ==> state_code
        NULL, -- ==> state_name  TODO: read from the mapping usaspending_api.references.abbreviations import code_to_state, state_to_code
        NULL, -- ==> state_description
        p.legal_entity_city_name, -- ==> city_name
        NULL, -- ==> city_code
        NULL, -- ==> county_name
        NULL, -- ==> county_code
        p.legal_entity_address_line1, -- ==> address_line1
        p.legal_entity_address_line2, -- ==> address_line2
        p.legal_entity_address_line3, -- ==> address_line3
        NULL, -- ==> foreign_location_description
        p.legal_entity_zip4, -- ==> zip4
        NULL, -- ==> zip_4a
        p.legal_entity_congressional, -- ==> congressional_code
        NULL, -- ==> performance_code
        NULL, -- ==> zip_last4
        NULL, -- ==> zip5
        NULL, -- ==> foreign_postal_code
        NULL, -- ==> foreign_province
        NULL, -- ==> foreign_city_name
        NULL::DATE, -- ==> reporting_period_start
        NULL::DATE, -- ==> reporting_period_end
        CURRENT_DATE, -- ==> last_modified_date
        NULL::DATE, -- ==> certified_date
        CURRENT_TIMESTAMP, -- ==> create_date
        CURRENT_TIMESTAMP, -- ==> update_date
        false, -- ==> place_of_performance_flag
        true, -- ==> recipient_flag
        p.legal_entity_country_code, -- ==> location_country_code
        ARRAY_AGG(p.award_procurement_id) -- ==> award_procurement_ids
FROM    local_broker.award_procurement p
JOIN    ref_country_code ON (ref_country_code.country_code = p.legal_entity_country_code)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
         21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
ON CONFLICT
(
  coalesce(location_country_code, ''),
  coalesce(country_name, ''),
  coalesce(state_code, ''),
  coalesce(state_name, ''),
  coalesce(state_description, ''),
  coalesce(city_name, ''),
  coalesce(city_code, ''),
  coalesce(county_name, ''),
  coalesce(county_code, ''),
  coalesce(address_line1, ''),
  coalesce(address_line2, ''),
  coalesce(address_line3, ''),
  coalesce(foreign_location_description, ''),
  coalesce(zip4, ''),
  coalesce(congressional_code, ''),
  coalesce(performance_code, ''),
  coalesce(zip_last4, ''),
  coalesce(zip5, ''),
  coalesce(foreign_postal_code, ''),
  coalesce(foreign_province, ''),
  coalesce(foreign_city_name, ''),
  coalesce(reporting_period_start, '1900-01-01'::DATE),
  coalesce(reporting_period_end, '1900-01-01'::DATE),
  recipient_flag,
  place_of_performance_flag
)
DO UPDATE
SET award_procurement_ids = EXCLUDED.award_procurement_ids;


-- place of performance locations from contracts
INSERT INTO references_location (
        data_source,
        country_name,
        state_code,
        state_name,
        state_description,
        city_name,
        city_code,
        county_name,
        county_code,
        address_line1,
        address_line2,
        address_line3,
        foreign_location_description,
        zip4,
        zip_4a,
        congressional_code,
        performance_code,
        zip_last4,
        zip5,
        foreign_postal_code,
        foreign_province,
        foreign_city_name,
        reporting_period_start,
        reporting_period_end,
        last_modified_date,
        certified_date,
        create_date,
        update_date,
        place_of_performance_flag,
        recipient_flag,
        location_country_code,
        place_of_performance_award_procurement_ids
      )
SELECT
        'DBR', -- ==> data_source
        ref_country_code.country_name, -- ==> country_name
        REPLACE(p.place_of_performance_state, '.', ''), -- ==> state_code
        NULL, -- ==> state_name  TODO: read from the mapping - perhaps during the save
        NULL, -- ==> state_description
        p.place_of_performance_locat, -- ==> city_name
        NULL, -- ==> city_code
        NULL, -- ==> county_name
        NULL, -- ==> county_code
        NULL, -- ==> address_line1
        NULL, -- ==> address_line2
        NULL, -- ==> address_line3
        NULL, -- ==> foreign_location_description
        p.place_of_performance_zip4a, -- ==> zip4
        NULL, -- ==> zip_4a  TODO: even though there's also a 4a?
        p.place_of_performance_congr, -- ==> congressional_code
        NULL, -- ==> performance_code
        NULL, -- ==> zip_last4
        NULL, -- ==> zip5
        NULL, -- ==> foreign_postal_code
        NULL, -- ==> foreign_province
        NULL, -- ==> foreign_city_name
        NULL::DATE, -- ==> reporting_period_start
        NULL::DATE, -- ==> reporting_period_end
        NULL::DATE, -- ==> last_modified_date
        NULL::DATE, -- ==> certified_date
        CURRENT_TIMESTAMP, -- ==> create_date
        CURRENT_TIMESTAMP, -- ==> update_date
        true, -- ==> place_of_performance_flag
        false, -- ==> recipient_flag
        p.place_of_perform_country_c, -- ==> location_country_code
        ARRAY_AGG(p.award_procurement_id) -- ==> place_of_performance_award_procurement_ids
FROM    local_broker.award_procurement p
JOIN    ref_country_code ON (ref_country_code.country_code = p.place_of_perform_country_c)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10,
         11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
         21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
ON CONFLICT
(
  coalesce(location_country_code, ''),
  coalesce(country_name, ''),
  coalesce(state_code, ''),
  coalesce(state_name, ''),
  coalesce(state_description, ''),
  coalesce(city_name, ''),
  coalesce(city_code, ''),
  coalesce(county_name, ''),
  coalesce(county_code, ''),
  coalesce(address_line1, ''),
  coalesce(address_line2, ''),
  coalesce(address_line3, ''),
  coalesce(foreign_location_description, ''),
  coalesce(zip4, ''),
  coalesce(congressional_code, ''),
  coalesce(performance_code, ''),
  coalesce(zip_last4, ''),
  coalesce(zip5, ''),
  coalesce(foreign_postal_code, ''),
  coalesce(foreign_province, ''),
  coalesce(foreign_city_name, ''),
  coalesce(reporting_period_start, '1900-01-01'::DATE),
  coalesce(reporting_period_end, '1900-01-01'::DATE),
  recipient_flag,
  place_of_performance_flag
)
  DO UPDATE
  SET place_of_performance_award_procurement_ids = EXCLUDED.place_of_performance_award_procurement_ids;


-- legal entities from contracts
INSERT INTO public.legal_entity (
    data_source,
    parent_recipient_unique_id,
    recipient_name,
    vendor_doing_as_business_name,
    vendor_phone_number,
    vendor_fax_number,
    business_types,
    business_types_description,
    recipient_unique_id,
    limited_liability_corporation,
    sole_proprietorship,
    partnership_or_limited_liability_partnership,
    subchapter_scorporation,
    foundation,
    for_profit_organization,
    nonprofit_organization,
    corporate_entity_tax_exempt,
    corporate_entity_not_tax_exempt,
    other_not_for_profit_organization,
    sam_exception,
    city_local_government,
    county_local_government,
    inter_municipal_local_government,
    local_government_owned,
    municipality_local_government,
    school_district_local_government,
    township_local_government,
    us_state_government,
    us_federal_government,
    federal_agency,
    federally_funded_research_and_development_corp,
    us_tribal_government,
    foreign_government,
    community_developed_corporation_owned_firm,
    labor_surplus_area_firm,
    small_agricultural_cooperative,
    international_organization,
    us_government_entity,
    emerging_small_business,
    "8a_program_participant",
    sba_certified_8a_joint_venture,
    dot_certified_disadvantage,
    self_certified_small_disadvantaged_business,
    historically_underutilized_business_zone,
    small_disadvantaged_business,
    the_ability_one_program,
    historically_black_college,
    "1862_land_grant_college",
    "1890_land_grant_college",
    "1994_land_grant_college",
    minority_institution,
    private_university_or_college,
    school_of_forestry,
    state_controlled_institution_of_higher_learning,
    tribal_college,
    veterinary_college,
    educational_institution,
    alaskan_native_servicing_institution,
    community_development_corporation,
    native_hawaiian_servicing_institution,
    domestic_shelter,
    manufacturer_of_goods,
    hospital_flag,
    veterinary_hospital,
    hispanic_servicing_institution,
    woman_owned_business,
    minority_owned_business,
    women_owned_small_business,
    economically_disadvantaged_women_owned_small_business,
    joint_venture_women_owned_small_business,
    joint_venture_economic_disadvantaged_women_owned_small_bus,
    veteran_owned_business,
    service_disabled_veteran_owned_business,
    contracts,
    grants,
    receives_contracts_and_grants,
    airport_authority,
    council_of_governments,
    housing_authorities_public_tribal,
    interstate_entity,
    planning_commission,
    port_authority,
    transit_authority,
    foreign_owned_and_located,
    american_indian_owned_business,
    alaskan_native_owned_corporation_or_firm,
    indian_tribe_federally_recognized,
    native_hawaiian_owned_business,
    tribally_owned_business,
    asian_pacific_american_owned_business,
    black_american_owned_business,
    hispanic_american_owned_business,
    native_american_owned_business,
    subcontinent_asian_asian_indian_american_owned_business,
    other_minority_owned_business,
    us_local_government,
    undefinitized_action,
    domestic_or_foreign_entity,
    domestic_or_foreign_entity_description,
    division_name,
    division_number,
    last_modified_date,
    certified_date,
    reporting_period_start,
    reporting_period_end,
    create_date,
    update_date,
    city_township_government,
    special_district_government,
    small_business,
    small_business_description,
    individual,
    location_id,
    business_categories,
    award_procurement_ids )
SELECT
    'DBR', -- ==> data_source
    p.ultimate_parent_unique_ide, -- ==> parent_recipient_unique_id
    COALESCE(p.awardee_or_recipient_legal, ''), -- ==> recipient_name -- TODO: is this blank ok
    p.vendor_doing_as_business_n, -- ==> vendor_doing_as_business_name
    p.vendor_phone_number, -- ==> vendor_phone_number
    p.vendor_fax_number, -- ==> vendor_fax_number
    NULL, -- ==> business_types
    NULL, -- ==> business_types_description
    p.awardee_or_recipient_uniqu, -- ==> recipient_unique_id
    MAX(p.limited_liability_corporat), -- ==> limited_liability_corporation
    MAX(p.sole_proprietorship), -- ==> sole_proprietorship
    MAX(p.partnership_or_limited_lia), -- ==> partnership_or_limited_liability_partnership
    NULL, -- ==> subchapter_scorporation
    MAX(p.foundation), -- ==> foundation
    MAX(p.for_profit_organization), -- ==> for_profit_organization
    MAX(p.nonprofit_organization), -- ==> nonprofit_organization
    MAX(p.corporate_entity_tax_exemp), -- ==> corporate_entity_tax_exempt
    MAX(p.corporate_entity_not_tax_e), -- ==> corporate_entity_not_tax_exempt
    MAX(p.other_not_for_profit_organ), -- ==> other_not_for_profit_organization
    MAX(p.sam_exception), -- ==> sam_exception
    MAX(p.city_local_government), -- ==> city_local_government
    MAX(p.county_local_government), -- ==> county_local_government
    MAX(p.inter_municipal_local_gove), -- ==> inter_municipal_local_government
    MAX(p.local_government_owned), -- ==> local_government_owned
    MAX(p.municipality_local_governm), -- ==> municipality_local_government
    MAX(p.school_district_local_gove), -- ==> school_district_local_government
    MAX(p.township_local_government), -- ==> township_local_government
    MAX(p.us_state_government), -- ==> us_state_government
    MAX(p.us_federal_government), -- ==> us_federal_government
    MAX(p.federal_agency), -- ==> federal_agency
    MAX(p.federally_funded_research), -- ==> federally_funded_research_and_development_corp
    MAX(p.us_tribal_government), -- ==> us_tribal_government
    MAX(p.foreign_government), -- ==> foreign_government
    MAX(p.community_developed_corpor), -- ==> community_developed_corporation_owned_firm
    MAX(p.labor_surplus_area_firm), -- ==> labor_surplus_area_firm
    MAX(p.small_agricultural_coopera), -- ==> small_agricultural_cooperative
    MAX(p.international_organization), -- ==> international_organization
    MAX(p.us_government_entity), -- ==> us_government_entity
    MAX(p.emerging_small_business), -- ==> emerging_small_business
    NULL, -- ==> 8a_program_participant
    NULL, -- ==> sba_certified_8a_joint_venture
    MAX(p.dot_certified_disadvantage), -- ==> dot_certified_disadvantage
    MAX(p.self_certified_small_disad), -- ==> self_certified_small_disadvantaged_business
    NULL, -- ==> historically_underutilized_business_zone
    MAX(p.small_disadvantaged_busine), -- ==> small_disadvantaged_business
    MAX(p.the_ability_one_program), -- ==> the_ability_one_program
    MAX(p.historically_black_college), -- ==> historically_black_college
    NULL, -- ==> 1862_land_grant_college
    NULL, -- ==> 1890_land_grant_college
    NULL, -- ==> 1994_land_grant_college
    MAX(p.minority_institution), -- ==> minority_institution
    MAX(p.private_university_or_coll), -- ==> private_university_or_college
    MAX(p.school_of_forestry), -- ==> school_of_forestry
    MAX(p.state_controlled_instituti), -- ==> state_controlled_institution_of_higher_learning
    MAX(p.tribal_college), -- ==> tribal_college
    MAX(p.veterinary_college), -- ==> veterinary_college
    MAX(p.educational_institution), -- ==> educational_institution
    MAX(p.alaskan_native_servicing_i), -- ==> alaskan_native_servicing_institution
    MAX(p.community_development_corp), -- ==> community_development_corporation
    MAX(p.native_hawaiian_servicing), -- ==> native_hawaiian_servicing_institution
    MAX(p.domestic_shelter), -- ==> domestic_shelter
    MAX(p.manufacturer_of_goods), -- ==> manufacturer_of_goods
    MAX(p.hospital_flag), -- ==> hospital_flag
    MAX(p.veterinary_hospital), -- ==> veterinary_hospital
    MAX(p.hispanic_servicing_institu), -- ==> hispanic_servicing_institution
    MAX(p.woman_owned_business), -- ==> woman_owned_business
    MAX(p.minority_owned_business), -- ==> minority_owned_business
    MAX(p.women_owned_small_business), -- ==> women_owned_small_business
    MAX(p.economically_disadvantaged), -- ==> economically_disadvantaged_women_owned_small_business
    MAX(p.joint_venture_women_owned), -- ==> joint_venture_women_owned_small_business
    MAX(p.joint_venture_economically), -- ==> joint_venture_economic_disadvantaged_women_owned_small_bus
    MAX(p.veteran_owned_business), -- ==> veteran_owned_business
    MAX(p.service_disabled_veteran_o), -- ==> service_disabled_veteran_owned_business
    MAX(p.contracts), -- ==> contracts
    MAX(p.grants), -- ==> grants
    MAX(p.receives_contracts_and_gra), -- ==> receives_contracts_and_grants
    MAX(p.airport_authority), -- ==> airport_authority
    MAX(p.council_of_governments), -- ==> council_of_governments
    MAX(p.housing_authorities_public), -- ==> housing_authorities_public_tribal
    MAX(p.interstate_entity), -- ==> interstate_entity
    MAX(p.planning_commission), -- ==> planning_commission
    MAX(p.port_authority), -- ==> port_authority
    MAX(p.transit_authority), -- ==> transit_authority
    MAX(p.foreign_owned_and_located), -- ==> foreign_owned_and_located
    MAX(p.american_indian_owned_busi), -- ==> american_indian_owned_business
    MAX(p.alaskan_native_owned_corpo), -- ==> alaskan_native_owned_corporation_or_firm
    MAX(p.indian_tribe_federally_rec), -- ==> indian_tribe_federally_recognized
    MAX(p.native_hawaiian_owned_busi), -- ==> native_hawaiian_owned_business
    MAX(p.tribally_owned_business), -- ==> tribally_owned_business
    MAX(p.asian_pacific_american_own), -- ==> asian_pacific_american_owned_business
    MAX(p.black_american_owned_busin), -- ==> black_american_owned_business
    MAX(p.hispanic_american_owned_bu), -- ==> hispanic_american_owned_business
    MAX(p.native_american_owned_busi), -- ==> native_american_owned_business
    MAX(p.subcontinent_asian_asian_i), -- ==> subcontinent_asian_asian_indian_american_owned_business
    MAX(p.other_minority_owned_busin), -- ==> other_minority_owned_business
    MAX(p.us_local_government), -- ==> us_local_government
    MAX(p.undefinitized_action), -- ==> undefinitized_action
    MAX(p.domestic_or_foreign_entity), -- ==> domestic_or_foreign_entity
    NULL, -- ==> domestic_or_foreign_entity_description
    NULL, -- ==> division_name
    NULL, -- ==> division_number
    NULL::DATE, -- ==> last_modified_date
    NULL::DATE, -- ==> certified_date
    NULL::DATE, -- ==> reporting_period_start
    NULL::DATE, -- ==> reporting_period_end
    CURRENT_TIMESTAMP, -- ==> create_date
    CURRENT_TIMESTAMP, -- ==> update_date
    NULL, -- ==> city_township_government
    NULL, -- ==> special_district_government
    NULL, -- ==> small_business
    NULL, -- ==> small_business_description
    NULL, -- ==> individual
    l.location_id, -- ==> location_id
    ARRAY[]::TEXT[], -- ==> business_categories   TODO: will need to run legal_entity.update_business_type_categories - it's in legalentity.save()
    ARRAY_AGG(p.award_procurement_id) -- ==> award_procurement_ids
FROM    local_broker.award_procurement p
JOIN    references_location l ON (p.award_procurement_id = ANY(l.award_procurement_ids))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9,
    99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, location_id
ON CONFLICT (
  COALESCE(data_source, ''),
  COALESCE(parent_recipient_unique_id, ''),
  COALESCE(recipient_name, ''),
  COALESCE(vendor_doing_as_business_name, ''),
  COALESCE(vendor_phone_number, ''),
  COALESCE(vendor_fax_number, ''),
  COALESCE(business_types, ''),
  COALESCE(business_types_description, ''),
  COALESCE(recipient_unique_id, ''),
  COALESCE(domestic_or_foreign_entity_description, ''),
  COALESCE(division_name, ''),
  COALESCE(division_number, ''),
  COALESCE(city_township_government, ''),
  COALESCE(special_district_government, ''),
  COALESCE(small_business, ''),
  COALESCE(small_business_description, ''),
  COALESCE(individual, ''),
  COALESCE(location_id, -1)
)
DO UPDATE
SET award_procurement_ids = EXCLUDED.award_procurement_ids;


-- awards from contracts
INSERT INTO awards (
    data_source,
    type,
    type_description,
    piid,
    fain,
    uri,
    total_obligation,
    total_outlay,
    date_signed,
    description,
    period_of_performance_start_date,
    period_of_performance_current_end_date,
    potential_total_value_of_award,
    last_modified_date,
    certified_date,
    create_date,
    update_date,
    total_subaward_amount,
    subaward_count,
    awarding_agency_id,
    funding_agency_id,
    latest_transaction_id,
    parent_award_id,
    place_of_performance_id,
    recipient_id,
    category,
    award_procurement_ids )
SELECT
    'DBR', -- ==> data_source
    NULL, -- ==> type
    NULL, -- ==> type_description
    p.piid, -- ==> piid
    NULL, -- ==> fain
    NULL, -- ==> uri
    NULL::NUMERIC, -- ==> total_obligation
    NULL::NUMERIC, -- ==> total_outlay
    NULL::DATE, -- ==> date_signed
    NULL, -- ==> description
    p.period_of_performance_star::DATE, -- ==> period_of_performance_start_date   todo - connect this to the transactions  - so far I don't see any logic here, seems as if automatically created awards leave it blank?
    p.period_of_performance_curr::DATE, -- ==> period_of_performance_current_end_date
    p.potential_total_value_awar::NUMERIC(20,2), -- ==> potential_total_value_of_award
    NULL::DATE, -- ==> last_modified_date
    NULL::DATE, -- ==> certified_date
    CURRENT_TIMESTAMP, -- ==> create_date
    CURRENT_TIMESTAMP, -- ==> update_date
    NULL::NUMERIC, -- ==> total_subaward_amount
    0, -- ==> subaward_count  TODO: update_award_subawards , runs during load_subawards, which is its own command
    awarding_agency.id, -- ==> awarding_agency_id
    funding_agency.id, -- ==> funding_agency_id
    NULL::NUMERIC, -- ==> latest_transaction_id  TODO: raw sql in award_helpers.py.update_awards(), in post_load_cleanup in command, so still runs
    parent_award.id, -- ==> parent_award_id
    l.location_id, -- ==> place_of_performance_id
    le.legal_entity_id, -- ==> recipient_id
    NULL, -- ==> category
    ARRAY_AGG(p.award_procurement_id) -- ==> award_procurement_ids
FROM local_broker.award_procurement p
JOIN agency_by_subtier_and_optionally_toptier awarding_agency ON
    (    p.awarding_agency_code = awarding_agency.cgac_code
     AND COALESCE(p.awarding_sub_tier_agency_c, '') = COALESCE(awarding_agency.subtier_code, ''))
JOIN agency_by_subtier_and_optionally_toptier funding_agency ON
    (    p.funding_agency_code = funding_agency.cgac_code
     AND COALESCE(p.funding_sub_tier_agency_co, '') = COALESCE(funding_agency.subtier_code, ''))
LEFT OUTER JOIN awards parent_award ON (p.parent_award_id = parent_award.piid)
JOIN legal_entity le ON (p.award_procurement_id = ANY(le.award_procurement_ids))
JOIN references_location l ON (p.award_procurement_id = ANY(l.place_of_performance_award_procurement_ids))
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25
ON CONFLICT (
  COALESCE(data_source, ''),
  COALESCE(type, ''),
  COALESCE(type_description, ''),
  COALESCE(piid, ''),
  COALESCE(fain, ''),
  COALESCE(uri, ''),
  COALESCE(total_obligation, -1),
  COALESCE(total_outlay, -1),
  COALESCE(date_signed, '1900-01-01'::DATE),
  COALESCE(description, ''),
  COALESCE(period_of_performance_start_date, '1900-01-01'::DATE),
  COALESCE(period_of_performance_current_end_date, '1900-01-01'::DATE),
  COALESCE(potential_total_value_of_award, -1),
  COALESCE(last_modified_date, '1900-01-01'::DATE),
  COALESCE(certified_date, '1900-01-01'::DATE),
  -- COALESCE(create_date, '1900-01-01'::DATE),
  -- COALESCE(update_date, '1900-01-01'::DATE),
  COALESCE(total_subaward_amount, -1),
  COALESCE(subaward_count, -1),
  COALESCE(awarding_agency_id, -1),
  COALESCE(funding_agency_id, -1),
  COALESCE(latest_transaction_id, -1),
  COALESCE(parent_award_id, -1),
  COALESCE(place_of_performance_id, -1),
  COALESCE(recipient_id, -1),
  COALESCE(category, '')
)
DO UPDATE
SET award_procurement_ids = EXCLUDED.award_procurement_ids;


        -- From here on, it does not need to be idempotent,
        -- because submission load starts by deleting records from that submission
        --
        -- transactions from financial assistance
        WITH transaction_ins AS (
            INSERT INTO transaction (
                data_source,
                usaspending_unique_transaction_id,
                type,
                type_description,
                period_of_performance_start_date,
                period_of_performance_current_end_date,
                action_date,
                action_type,
                action_type_description,
                federal_action_obligation,
                modification_number,
                description,
                drv_award_transaction_usaspend,
                drv_current_total_award_value_amount_adjustment,
                drv_potential_total_award_value_amount_adjustment,
                last_modified_date,
                certified_date,
                create_date,
                update_date,
                award_id,
                awarding_agency_id,
                funding_agency_id,
                place_of_performance_id,
                recipient_id,
                submission_id,
                fiscal_year,
                award_procurement_id
              )
            SELECT
                'DBR', -- ==> data_source
                NULL, -- ==> usaspending_unique_transaction_id
                p.contract_award_type, -- ==> type
                NULL, -- ==> type_description
                p.period_of_performance_star::DATE, -- ==> period_of_performance_start_date
                p.period_of_performance_curr::DATE, -- ==> period_of_performance_current_end_date
                p.action_date::DATE, -- ==> action_date
                p.action_type, -- ==> action_type
                NULL, -- ==> action_type_description
                p.federal_action_obligation::NUMERIC(20,2), -- ==> federal_action_obligation
                award_modification_amendme, -- ==> modification_number
                p.award_description, -- ==> description
                NULL::NUMERIC(20,2), -- ==> drv_award_transaction_usaspend
                NULL::NUMERIC(20,2), -- ==> drv_current_total_award_value_amount_adjustment
                NULL::NUMERIC(20,2), -- ==> drv_potential_total_award_value_amount_adjustment
                NULL::DATE, -- ==> last_modified_date
                NULL::DATE, -- ==> certified_date
                CURRENT_TIMESTAMP, -- ==> create_date
                CURRENT_TIMESTAMP, -- ==> update_date
                a.id, -- ==> award_id
                a.awarding_agency_id, -- ==> awarding_agency_id
                a.funding_agency_id, -- ==> funding_agency_id  TODO: includes the assumption that txn awarding/funding agency always matches award-level
                a.place_of_performance_id, -- ==> place_of_performance_id
                a.recipient_id, -- ==> recipient_id
                s.submission_id, -- ==> submission_id
                NULL::INTEGER, -- ==> fiscal_year  TODO
                p.award_procurement_id -- ==> award_procurement_id
            FROM    local_broker.award_procurement p
            JOIN    awards a ON (p.award_procurement_id = ANY(a.award_procurement_ids))
            JOIN    submission_attributes s ON (s.broker_submission_id = p.submission_id)
            ON CONFLICT DO NOTHING
            RETURNING *
      )
      INSERT INTO public.transaction_contract (
          data_source,
          transaction_id,
          piid,
          parent_award_id,
          cost_or_pricing_data,
          cost_or_pricing_data_description,
          type_of_contract_pricing,
          type_of_contract_pricing_description,
          naics,
          naics_description,
          period_of_performance_potential_end_date,
          ordering_period_end_date,
          current_total_value_award,
          potential_total_value_of_award,
          referenced_idv_agency_identifier,
          idv_type,
          idv_type_description,
          multiple_or_single_award_idv,
          multiple_or_single_award_idv_description,
          type_of_idc,
          type_of_idc_description,
          a76_fair_act_action,
          dod_claimant_program_code,
          clinger_cohen_act_planning,
          commercial_item_acquisition_procedures,
          commercial_item_acquisition_procedures_description,
          commercial_item_test_program,
          consolidated_contract,
          contingency_humanitarian_or_peacekeeping_operation,
          contingency_humanitarian_or_peacekeeping_operation_description,
          contract_bundling,
          contract_bundling_description,
          contract_financing,
          contract_financing_description,
          contracting_officers_determination_of_business_size,
          cost_accounting_standards,
          cost_accounting_standards_description,
          country_of_product_or_service_origin,
          davis_bacon_act,
          davis_bacon_act_description,
          evaluated_preference,
          evaluated_preference_description,
          extent_competed,
          extent_competed_description,
          fed_biz_opps,
          fed_biz_opps_description,
          foreign_funding,
          foreign_funding_description,
          gfe_gfp,
          information_technology_commercial_item_category,
          information_technology_commercial_item_category_description,
          interagency_contracting_authority,
          interagency_contracting_authority_description,
          local_area_set_aside,
          major_program,
          purchase_card_as_payment_method,
          multi_year_contract,
          national_interest_action,
          national_interest_action_description,
          number_of_actions,
          number_of_offers_received,
          other_statutory_authority,
          performance_based_service_acquisition,
          performance_based_service_acquisition_description,
          place_of_manufacture,
          place_of_manufacture_description,
          price_evaluation_adjustment_preference_percent_difference,
          product_or_service_code,
          program_acronym,
          other_than_full_and_open_competition,
          recovered_materials_sustainability,
          recovered_materials_sustainability_description,
          research,
          research_description,
          sea_transportation,
          sea_transportation_description,
          service_contract_act,
          service_contract_act_description,
          small_business_competitiveness_demonstration_program,
          solicitation_identifier,
          solicitation_procedures,
          solicitation_procedures_description,
          fair_opportunity_limited_sources,
          fair_opportunity_limited_sources_description,
          subcontracting_plan,
          subcontracting_plan_description,
          program_system_or_equipment_code,
          type_set_aside,
          type_set_aside_description,
          epa_designated_product,
          epa_designated_product_description,
          walsh_healey_act,
          transaction_number,
          referenced_idv_modification_number,
          rec_flag,
          drv_parent_award_awarding_agency_code,
          drv_current_aggregated_total_value_of_award,
          drv_current_total_value_of_award,
          drv_potential_award_idv_amount_total_estimate,
          drv_potential_aggregated_award_idv_amount_total_estimate,
          drv_potential_aggregated_total_value_of_award,
          drv_potential_total_value_of_award,
          create_date,
          update_date,
          last_modified_date,
          certified_date,
          reporting_period_start,
          reporting_period_end,
          submission_id
        )
      SELECT
          'DBR', -- ==> data_source
          transaction_ins.id, -- ==> transaction_id
          p.piid, -- ==> piid
          a.parent_award_id, -- ==> parent_award_id
          p.cost_or_pricing_data, -- ==> cost_or_pricing_data
          NULL, -- ==> cost_or_pricing_data_description
          p.type_of_contract_pricing, -- ==> type_of_contract_pricing
          NULL, -- ==> type_of_contract_pricing_description
          p.naics, -- ==> naics
          p.naics_description, -- ==> naics_description
          p.period_of_perf_potential_e::DATE, -- ==> period_of_performance_potential_end_date
          p.ordering_period_end_date::DATE, -- ==> ordering_period_end_date
          p.current_total_value_award::NUMERIC(20,2), -- ==> current_total_value_award
          potential_total_value_awar::NUMERIC(20,2), -- ==> potential_total_value_of_award
          referenced_idv_agency_iden, -- ==> referenced_idv_agency_identifier
          p.idv_type, -- ==> idv_type
          NULL, -- ==> idv_type_description
          p.multiple_or_single_award_i, -- ==> multiple_or_single_award_idv
          NULL, -- ==> multiple_or_single_award_idv_description
          p.type_of_idc, -- ==> type_of_idc
          NULL, -- ==> type_of_idc_description
          p.a_76_fair_act_action, -- ==> a76_fair_act_action
          p.dod_claimant_program_code, -- ==> dod_claimant_program_code
          p.clinger_cohen_act_planning, -- ==> clinger_cohen_act_planning
          commercial_item_acquisitio, -- ==> commercial_item_acquisition_procedures
          NULL, -- ==> commercial_item_acquisition_procedures_description
          p.commercial_item_test_progr, -- ==> commercial_item_test_program
          p.consolidated_contract, -- ==> consolidated_contract
          p.contingency_humanitarian_o, -- ==> contingency_humanitarian_or_peacekeeping_operation
          NULL, -- ==> contingency_humanitarian_or_peacekeeping_operation_description
          p.contract_bundling, -- ==> contract_bundling
          NULL, -- ==> contract_bundling_description
          p.contract_financing, -- ==> contract_financing
          NULL, -- ==> contract_financing_description
          p.contracting_officers_deter, -- ==> contracting_officers_determination_of_business_size
          p.cost_accounting_standards, -- ==> cost_accounting_standards
          NULL, -- ==> cost_accounting_standards_description
          p.country_of_product_or_serv, -- ==> country_of_product_or_service_origin
          p.davis_bacon_act, -- ==> davis_bacon_act
          NULL, -- ==> davis_bacon_act_description
          p.evaluated_preference, -- ==> evaluated_preference
          NULL, -- ==> evaluated_preference_description
          p.extent_competed, -- ==> extent_competed
          NULL, -- ==> extent_competed_description
          p.fed_biz_opps, -- ==> fed_biz_opps
          NULL, -- ==> fed_biz_opps_description
          p.foreign_funding, -- ==> foreign_funding
          NULL, -- ==> foreign_funding_description
          NULL, -- ==> gfe_gfp  - recently disappeared from award_procurement
          p.information_technology_com, -- ==> information_technology_commercial_item_category
          NULL, -- ==> information_technology_commercial_item_category_description
          p.interagency_contracting_au, -- ==> interagency_contracting_authority
          NULL, -- ==> interagency_contracting_authority_description
          p.local_area_set_aside, -- ==> local_area_set_aside
          p.major_program, -- ==> major_program
          p.purchase_card_as_payment_m, -- ==> purchase_card_as_payment_method
          p.multi_year_contract, -- ==> multi_year_contract
          p.national_interest_action, -- ==> national_interest_action
          NULL, -- ==> national_interest_action_description
          p.number_of_actions, -- ==> number_of_actions
          p.number_of_offers_received, -- ==> number_of_offers_received
          p.other_statutory_authority, -- ==> other_statutory_authority
          p.performance_based_service, -- ==> performance_based_service_acquisition
          NULL, -- ==> performance_based_service_acquisition_description
          p.place_of_manufacture, -- ==> place_of_manufacture
          NULL, -- ==> place_of_manufacture_description
          p.price_evaluation_adjustmen::NUMERIC(4, 2), -- ==> price_evaluation_adjustment_preference_percent_difference
          p.product_or_service_code, -- ==> product_or_service_code
          p.program_acronym, -- ==> program_acronym
          p.other_than_full_and_open_c, -- ==> other_than_full_and_open_competition
          p.recovered_materials_sustai, -- ==> recovered_materials_sustainability
          NULL, -- ==> recovered_materials_sustainability_description
          p.research, -- ==> research
          NULL, -- ==> research_description
          p.sea_transportation, -- ==> sea_transportation
          NULL, -- ==> sea_transportation_description
          p.service_contract_act, -- ==> service_contract_act
          NULL, -- ==> service_contract_act_description
          NULL, -- ==> small_business_competitiveness_demonstration_program
          p.solicitation_identifier, -- ==> solicitation_identifier
          p.solicitation_procedures, -- ==> solicitation_procedures
          NULL, -- ==> solicitation_procedures_description
          p.fair_opportunity_limited_s, -- ==> fair_opportunity_limited_sources
          NULL, -- ==> fair_opportunity_limited_sources_description
          p.subcontracting_plan, -- ==> subcontracting_plan
          NULL, -- ==> subcontracting_plan_description
          p.program_system_or_equipmen, -- ==> program_system_or_equipment_code
          p.type_set_aside, -- ==> type_set_aside
          NULL, -- ==> type_set_aside_description
          p.epa_designated_product, -- ==> epa_designated_product
          NULL, -- ==> epa_designated_product_description
          p.walsh_healey_act, -- ==> walsh_healey_act
          p.transaction_number, -- ==> transaction_number
          p.referenced_idv_modificatio, -- ==> referenced_idv_modification_number
          NULL, -- ==> rec_flag
          NULL, -- ==> drv_parent_award_awarding_agency_code
          NULL::NUMERIC(20,2), -- ==> drv_current_aggregated_total_value_of_award
          NULL::NUMERIC(20,2), -- ==> drv_current_total_value_of_award
          NULL::NUMERIC(20,2), -- ==> drv_potential_award_idv_amount_total_estimate
          NULL::NUMERIC(20,2), -- ==> drv_potential_aggregated_award_idv_amount_total_estimate
          NULL::NUMERIC(20,2), -- ==> drv_potential_aggregated_total_value_of_award
          NULL::NUMERIC(20,2), -- ==> drv_potential_total_value_of_award
          CURRENT_TIMESTAMP, -- ==> create_date
          CURRENT_TIMESTAMP, -- ==> update_date
          NULL::DATE, -- ==> last_modified_date  TODO
          s.certified_date, -- ==> certified_date
          s.reporting_period_start, -- ==> reporting_period_start
          s.reporting_period_end, -- ==> reporting_period_end
          s.submission_id -- ==> submission_id
      FROM    local_broker.award_procurement p
      JOIN    transaction_ins ON (p.award_procurement_id = transaction_ins.award_procurement_id)
      JOIN    awards a ON (transaction_ins.award_id = a.id)
      JOIN    submission_attributes s ON (s.broker_submission_id = p.submission_id)
