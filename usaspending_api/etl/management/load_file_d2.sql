

DROP SCHEMA IF EXISTS local_broker CASCADE


CREATE SCHEMA local_broker


CREATE TABLE local_broker.award_financial_assistance AS
  SELECT * FROM broker.award_financial_assistance
  WHERE submission_id = %(broker_submission_id)s


CREATE UNIQUE INDEX references_location_uniq_nullable_idx ON references_location
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
);




ALTER TABLE references_location ADD COLUMN award_financial_assistance_ids INTEGER[];


-- recipient locations from financial assistance
INSERT INTO public.references_location (
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
    award_financial_assistance_ids
    )
SELECT
    'DBR', -- ==> data_source
    ref_country_code.country_name, -- ==> country_name
    REPLACE(fa.legal_entity_state_code, '.', ''), -- ==> state_code
    legal_entity_state_name, -- ==> state_name
    NULL::text, -- ==> state_description
    legal_entity_city_name, -- ==> city_name
    legal_entity_city_code, -- ==> city_code
    legal_entity_county_name, -- ==> county_name
    legal_entity_county_code, -- ==> county_code
    legal_entity_address_line1, -- ==> address_line1
    legal_entity_address_line2, -- ==> address_line2
    legal_entity_address_line3, -- ==> address_line3
    NULL::text, -- ==> foreign_location_description
    NULL::text, -- ==> zip4
    NULL::text, -- ==> zip_4a
    legal_entity_congressional, -- ==> congressional_code
    NULL::text, -- ==> performance_code
    legal_entity_zip_last4, -- ==> zip_last4
    legal_entity_zip5, -- ==> zip5
    legal_entity_foreign_posta, -- ==> foreign_postal_code
    legal_entity_foreign_provi, -- ==> foreign_province
    legal_entity_foreign_city, -- ==> foreign_city_name
    NULL::date, -- ==> reporting_period_start
    NULL::date, -- ==> reporting_period_end
    NULL::date, -- ==> last_modified_date  # TODO:
    NULL::date, -- ==> certified_date
    NULL::timestamp with time zone, -- ==> create_date
    NULL::timestamp with time zone, -- ==> update_date
    false, -- ==> place_of_performance_flag
    true, -- ==> recipient_flag
    legal_entity_country_code, -- ==> location_country_code
    ARRAY_AGG(fa.award_financial_assistance_id) -- ==> location_award_procurement_ids
FROM     local_broker.award_financial_assistance fa
JOIN     ref_country_code ON (ref_country_code.country_code = fa.legal_entity_country_code)
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31
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
SET award_financial_assistance_ids = EXCLUDED.award_financial_assistance_ids;


ALTER TABLE references_location ADD COLUMN place_of_performance_award_financial_assistance_ids INTEGER[];


-- place of performance locations from financial assistance
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
        place_of_performance_award_financial_assistance_ids
      )
SELECT
        'DBR', -- ==> data_source
        place_of_perform_county_na, -- ==> country_name
        NULL::text, -- ==> state_code  TODO: fetch from reference table by name?
        place_of_perform_state_nam, -- ==> state_name
        NULL::text, -- ==> state_description
        place_of_performance_city, -- ==> city_name
        NULL::text, -- ==> city_code
        NULL::text, -- ==> county_name
        NULL::text, -- ==> county_code
        NULL::text, -- ==> address_line1
        NULL::text, -- ==> address_line2
        NULL::text, -- ==> address_line3
        place_of_performance_forei, -- ==> foreign_location_description
        place_of_performance_zip4a, -- ==> zip4
        NULL::text, -- ==> zip_4a
        place_of_performance_congr, -- ==> congressional_code
        place_of_performance_code, -- ==> performance_code
        NULL::text, -- ==> zip_last4
        NULL::text, -- ==> zip5
        NULL::text, -- ==> foreign_postal_code
        NULL::text, -- ==> foreign_province
        NULL::text, -- ==> foreign_city_name
        NULL::date, -- ==> reporting_period_start
        NULL::date, -- ==> reporting_period_end
        NULL::date, -- ==> last_modified_date
        NULL::date, -- ==> certified_date
        NULL::timestamp with time zone, -- ==> create_date
        NULL::timestamp with time zone, -- ==> update_date
        true, -- ==> place_of_performance_flag
        false, -- ==> recipient_flag
        place_of_perform_country_c, -- ==> location_country_code
        ARRAY_AGG(fa.award_financial_assistance_id) -- ==> place_of_performance_award_financial_assistance_ids
FROM    local_broker.award_financial_assistance fa
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
  SET place_of_performance_award_financial_assistance_ids = EXCLUDED.place_of_performance_award_financial_assistance_ids;



drop index if exists legal_entity_unique_nullable;


create unique index if not exists legal_entity_unique_nullable on legal_entity (
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
    );



ALTER TABLE legal_entity ADD COLUMN award_financial_assistance_ids INTEGER[];


-- legal entities from financial assistance
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
    award_financial_assistance_ids )
SELECT
    'DBR', -- ==> data_source
    NULL::text, -- ==> parent_recipient_unique_id
    COALESCE(fa.awardee_or_recipient_legal, ''), -- ==> recipient_name -- TODO: is this blank ok
    NULL::text, -- ==> vendor_doing_as_business_name
    NULL::text, -- ==> vendor_phone_number
    NULL::text, -- ==> vendor_fax_number
    fa.business_types, -- ==> business_types
    NULL, -- ==> business_types_description
    fa.awardee_or_recipient_uniqu, -- ==> recipient_unique_id
    NULL::text, -- ==> limited_liability_corporation
    NULL::text, -- ==> sole_proprietorship
    NULL::text, -- ==> partnership_or_limited_liability_partnership
    NULL::text, -- ==> subchapter_scorporation
    NULL::text, -- ==> foundation
    NULL::text, -- ==> for_profit_organization
    NULL::text, -- ==> nonprofit_organization
    NULL::text, -- ==> corporate_entity_tax_exempt
    NULL::text, -- ==> corporate_entity_not_tax_exempt
    NULL::text, -- ==> other_not_for_profit_organization
    NULL::text, -- ==> sam_exception
    NULL::text, -- ==> city_local_government
    NULL::text, -- ==> county_local_government
    NULL::text, -- ==> inter_municipal_local_government
    NULL::text, -- ==> local_government_owned
    NULL::text, -- ==> municipality_local_government
    NULL::text, -- ==> school_district_local_government
    NULL::text, -- ==> township_local_government
    NULL::text, -- ==> us_state_government
    NULL::text, -- ==> us_federal_government
    NULL::text, -- ==> federal_agency
    NULL::text, -- ==> federally_funded_research_and_development_corp
    NULL::text, -- ==> us_tribal_government
    NULL::text, -- ==> foreign_government
    NULL::text, -- ==> community_developed_corporation_owned_firm
    NULL::text, -- ==> labor_surplus_area_firm
    NULL::text, -- ==> small_agricultural_cooperative
    NULL::text, -- ==> international_organization
    NULL::text, -- ==> us_government_entity
    NULL::text, -- ==> emerging_small_business
    NULL::text, -- ==> 8a_program_participant
    NULL::text, -- ==> sba_certified_8a_joint_venture
    NULL::text, -- ==> dot_certified_disadvantage
    NULL::text, -- ==> self_certified_small_disadvantaged_business
    NULL::text, -- ==> historically_underutilized_business_zone
    NULL::text, -- ==> small_disadvantaged_business
    NULL::text, -- ==> the_ability_one_program
    NULL::text, -- ==> historically_black_college
    NULL::text, -- ==> 1862_land_grant_college
    NULL::text, -- ==> 1890_land_grant_college
    NULL::text, -- ==> 1994_land_grant_college
    NULL::text, -- ==> minority_institution
    NULL::text, -- ==> private_university_or_college
    NULL::text, -- ==> school_of_forestry
    NULL::text, -- ==> state_controlled_institution_of_higher_learning
    NULL::text, -- ==> tribal_college
    NULL::text, -- ==> veterinary_college
    NULL::text, -- ==> educational_institution
    NULL::text, -- ==> alaskan_native_servicing_institution
    NULL::text, -- ==> community_development_corporation
    NULL::text, -- ==> native_hawaiian_servicing_institution
    NULL::text, -- ==> domestic_shelter
    NULL::text, -- ==> manufacturer_of_goods
    NULL::text, -- ==> hospital_flag
    NULL::text, -- ==> veterinary_hospital
    NULL::text, -- ==> hispanic_servicing_institution
    NULL::text, -- ==> woman_owned_business
    NULL::text, -- ==> minority_owned_business
    NULL::text, -- ==> women_owned_small_business
    NULL::text, -- ==> economically_disadvantaged_women_owned_small_business
    NULL::text, -- ==> joint_venture_women_owned_small_business
    NULL::text, -- ==> joint_venture_economic_disadvantaged_women_owned_small_bus
    NULL::text, -- ==> veteran_owned_business
    NULL::text, -- ==> service_disabled_veteran_owned_business
    NULL::text, -- ==> contracts
    NULL::text, -- ==> grants
    NULL::text, -- ==> receives_contracts_and_grants
    NULL::text, -- ==> airport_authority
    NULL::text, -- ==> council_of_governments
    NULL::text, -- ==> housing_authorities_public_tribal
    NULL::text, -- ==> interstate_entity
    NULL::text, -- ==> planning_commission
    NULL::text, -- ==> port_authority
    NULL::text, -- ==> transit_authority
    NULL::text, -- ==> foreign_owned_and_located
    NULL::text, -- ==> american_indian_owned_business
    NULL::text, -- ==> alaskan_native_owned_corporation_or_firm
    NULL::text, -- ==> indian_tribe_federally_recognized
    NULL::text, -- ==> native_hawaiian_owned_business
    NULL::text, -- ==> tribally_owned_business
    NULL::text, -- ==> asian_pacific_american_owned_business
    NULL::text, -- ==> black_american_owned_business
    NULL::text, -- ==> hispanic_american_owned_business
    NULL::text, -- ==> native_american_owned_business
    NULL::text, -- ==> subcontinent_asian_asian_indian_american_owned_business
    NULL::text, -- ==> other_minority_owned_business
    NULL::text, -- ==> us_local_government
    NULL::text, -- ==> undefinitized_action
    NULL::text, -- ==> domestic_or_foreign_entity
    NULL::text, -- ==> domestic_or_foreign_entity_description
    NULL::text, -- ==> division_name
    NULL::text, -- ==> division_number
    NULL::date, -- ==> last_modified_date
    NULL::date, -- ==> certified_date
    NULL::date, -- ==> reporting_period_start
    NULL::date, -- ==> reporting_period_end
    CURRENT_TIMESTAMP, -- ==> create_date
    CURRENT_TIMESTAMP, -- ==> update_date
    NULL, -- ==> city_township_government
    NULL, -- ==> special_district_government
    NULL, -- ==> small_business
    NULL, -- ==> small_business_description
    NULL, -- ==> individual
    l.location_id, -- ==> location_id
    ARRAY[]::TEXT[], -- ==> business_categories   TODO: will need to run legal_entity.update_business_type_categories - it's in legalentity.save()
    ARRAY_AGG(fa.award_financial_assistance_id) -- ==> award_financial_assistance_ids
FROM    local_broker.award_financial_assistance fa
JOIN    references_location l ON (fa.award_financial_assistance_id = ANY(l.award_financial_assistance_ids))
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
SET award_financial_assistance_ids = EXCLUDED.award_financial_assistance_ids;


drop index if exists awards_unique_nullable;


create unique index if not exists awards_unique_nullable on awards (
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
);




ALTER TABLE awards ADD COLUMN award_financial_assistance_ids INTEGER[];



-- awards from financial assistance
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
    award_financial_assistance_ids )
SELECT
    'DBR', -- ==> data_source
    NULL, -- ==> type
    NULL, -- ==> type_description
    NULL, -- ==> piid
    fa.fain, -- ==> fain
    fa.uri, -- ==> uri
    NULL::NUMERIC, -- ==> total_obligation
    NULL::NUMERIC, -- ==> total_outlay
    NULL::DATE, -- ==> date_signed
    NULL, -- ==> description
    fa.period_of_performance_star::DATE, -- ==> period_of_performance_start_date   todo - connect this to the transactions  - so far I don't see any logic here, seems as if automatically created awards leave it blank?
    fa.period_of_performance_curr::DATE, -- ==> period_of_performance_current_end_date
    NULL::numeric, -- ==> potential_total_value_of_award
    NULL::DATE, -- ==> last_modified_date
    NULL::DATE, -- ==> certified_date
    CURRENT_TIMESTAMP, -- ==> create_date
    CURRENT_TIMESTAMP, -- ==> update_date
    NULL::NUMERIC, -- ==> total_subaward_amount
    0, -- ==> subaward_count  TODO: update_award_subawards , runs during load_subawards, which is its own command
    awarding_agency.id, -- ==> awarding_agency_id
    funding_agency.id, -- ==> funding_agency_id
    NULL::NUMERIC, -- ==> latest_transaction_id  TODO: raw sql in award_helpers.py.update_awards(), in post_load_cleanup in command, so still runs
    NULL::NUMERIC, -- ==> parent_award_id  TODO: true - no parents for FA?
    l.location_id, -- ==> place_of_performance_id
    le.legal_entity_id, -- ==> recipient_id
    NULL, -- ==> category
    ARRAY_AGG(fa.award_financial_assistance_id) -- ==> award_financial_assistance_ids
FROM local_broker.award_financial_assistance fa
JOIN agency_by_subtier_and_optionally_toptier awarding_agency ON
    (    fa.awarding_agency_code = awarding_agency.cgac_code
     AND COALESCE(fa.awarding_sub_tier_agency_c, '') = COALESCE(awarding_agency.subtier_code, ''))
JOIN agency_by_subtier_and_optionally_toptier funding_agency ON
    (    fa.funding_agency_code = funding_agency.cgac_code
     AND COALESCE(fa.funding_sub_tier_agency_co, '') = COALESCE(funding_agency.subtier_code, ''))
JOIN legal_entity le ON (fa.award_financial_assistance_id = ANY(le.award_financial_assistance_ids))
JOIN references_location l ON (fa.award_financial_assistance_id = ANY(l.place_of_performance_award_financial_assistance_ids))
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
SET award_financial_assistance_ids = EXCLUDED.award_financial_assistance_ids;


ALTER TABLE transaction ADD COLUMN award_financial_assistance_id INTEGER;


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
                award_financial_assistance_id
              )
            SELECT
                DISTINCT
                'DBR', -- ==> data_source
                NULL::text, -- ==> usaspending_unique_transaction_id
                NULL::text, -- ==> type
                NULL::text, -- ==> type_description
                period_of_performance_star::date, -- ==> period_of_performance_start_date
                period_of_performance_curr::date, -- ==> period_of_performance_current_end_date
                action_date::date, -- ==> action_date
                action_type, -- ==> action_type
                NULL::text, -- ==> action_type_description
                federal_action_obligation, -- ==> federal_action_obligation
                award_modification_amendme, -- ==> modification_number
                NULL::text, -- ==> description
                NULL::numeric, -- ==> drv_award_transaction_usaspend
                NULL::numeric, -- ==> drv_current_total_award_value_amount_adjustment
                NULL::numeric, -- ==> drv_potential_total_award_value_amount_adjustment
                NULL::date, -- ==> last_modified_date
                NULL::date, -- ==> certified_date
                CURRENT_TIMESTAMP, -- ==> create_date
                CURRENT_TIMESTAMP, -- ==> update_date
                a.id, -- ==> award_id
                a.awarding_agency_id, -- ==> awarding_agency_id
                a.funding_agency_id, -- ==> funding_agency_id  TODO: includes the assumption that txn awarding/funding agency always matches award-level
                a.place_of_performance_id, -- ==> place_of_performance_id
                a.recipient_id, -- ==> recipient_id
                s.submission_id, -- ==> submission_id
                NULL::INTEGER, -- ==> fiscal_year  TODO
                fa.award_financial_assistance_id -- ==> award_financial_assistance_id
            FROM    local_broker.award_financial_assistance fa
            JOIN    awards a ON (fa.award_financial_assistance_id = ANY(a.award_financial_assistance_ids))
            JOIN    submission_attributes s ON (s.broker_submission_id = fa.submission_id)
            ON CONFLICT DO NOTHING
            RETURNING *
        )
        INSERT INTO public.transaction_assistance (
            data_source,
            transaction_id,
            fain,
            uri,
            business_funds_indicator,
            business_funds_indicator_description,
            non_federal_funding_amount,
            total_funding_amount,
            face_value_loan_guarantee,
            original_loan_subsidy_cost,
            record_type,
            record_type_description,
            correction_late_delete_indicator,
            correction_late_delete_indicator_description,
            fiscal_year_and_quarter_correction,
            sai_number,
            drv_federal_funding_amount,
            drv_award_finance_assistance_type_label,
            reporting_period_start,
            reporting_period_end,
            last_modified_date,
            submitted_type,
            certified_date,
            create_date,
            update_date,
            period_of_performance_start_date,
            period_of_performance_current_end_date,
            submission_id,
            cfda_id
            )
        SELECT
          DISTINCT
            'DBR', -- ==> data_source
            transaction_ins.id, -- ==> transaction_id
            fa.fain, -- ==> fain
            fa.uri, -- ==> uri
            business_funds_indicator, -- ==> business_funds_indicator
            NULL::text, -- ==> business_funds_indicator_description
            non_federal_funding_amount::numeric, -- ==> non_federal_funding_amount
            total_funding_amount::numeric, -- ==> total_funding_amount
            face_value_loan_guarantee::numeric, -- ==> face_value_loan_guarantee
            original_loan_subsidy_cost::numeric, -- ==> original_loan_subsidy_cost
            record_type::integer, -- ==> record_type
            NULL::text, -- ==> record_type_description
            correction_late_delete_ind, -- ==> correction_late_delete_indicator
            NULL::text, -- ==> correction_late_delete_indicator_description
            fiscal_year_and_quarter_co, -- ==> fiscal_year_and_quarter_correction
            sai_number, -- ==> sai_number
            NULL::numeric, -- ==> drv_federal_funding_amount
            NULL::text, -- ==> drv_award_finance_assistance_type_label
            NULL::date, -- ==> reporting_period_start
            NULL::date, -- ==> reporting_period_end
            NULL::date, -- ==> last_modified_date
            NULL::text, -- ==> submitted_type
            NULL::date, -- ==> certified_date
            CURRENT_TIMESTAMP, -- ==> create_date
            CURRENT_TIMESTAMP, -- ==> update_date
            period_of_performance_star::date, -- ==> period_of_performance_start_date
            period_of_performance_curr::date, -- ==> period_of_performance_current_end_date
            s.submission_id, -- ==> submission_id
            NULL::integer -- ==> cfda_id
        FROM    local_broker.award_financial_assistance fa
        JOIN    transaction_ins ON (transaction_ins.award_financial_assistance_id = fa.award_financial_assistance_id)
        JOIN    submission_attributes s ON (s.broker_submission_id = fa.submission_id)
