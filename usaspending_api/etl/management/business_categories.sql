DROP FUNCTION IF EXISTS find_business_type_categories(le legal_entity);


CREATE FUNCTION find_business_type_categories(le legal_entity) RETURNS text[] AS $$
DECLARE
    result text[] := '{}';
BEGIN
        -- # Begin Business Category
    IF    le.small_business = '1'
       OR le.business_types = 'R' THEN
      result := ARRAY_APPEND(result, 'small_business');
    END IF;

    IF le.business_types = 'Q' THEN
      result := ARRAY_APPEND(result, 'other_than_small_business');
    END IF;

    IF le.business_types = 'Q' THEN
      result := ARRAY_APPEND(result, 'other_than_small_business');
    END IF;

    IF     le.for_profit_organization = '1'
        OR 'small_business' = ANY(result)
        OR 'other_than_small_business' = ANY(result) THEN
      result := ARRAY_APPEND(result, 'category_business');
    END IF;
        -- # End Business Category

        -- # Minority Owned Business Category
    IF le.alaskan_native_owned_corporation_or_firm = '1' THEN
      result := ARRAY_APPEND(result, 'alaskan_native_owned_business');
    END IF;

    IF le.american_indian_owned_business = '1' THEN
      result := ARRAY_APPEND(result, 'american_indian_owned_business');
    END IF;

    IF le.asian_pacific_american_owned_business = '1' THEN
      result := ARRAY_APPEND(result, 'asian_pacific_american_owned_business');
    END IF;

    IF le.black_american_owned_business = '1' THEN
      result := ARRAY_APPEND(result, 'black_american_owned_business');
    END IF;

    IF le.hispanic_american_owned_business = '1' THEN
      result := ARRAY_APPEND(result, 'hispanic_american_owned_business');
    END IF;

    IF le.native_american_owned_business = '1' THEN
      result := ARRAY_APPEND(result, 'native_american_owned_business');
    END IF;

    IF le.native_hawaiian_owned_business = '1' THEN
      result := ARRAY_APPEND(result, 'native_hawaiian_owned_business');
    END IF;

    IF le.subcontinent_asian_asian_indian_american_owned_business = '1' THEN
      result := ARRAY_APPEND(result, 'subcontinent_asian_asian_indian_american_owned_business');
    END IF;

    IF le.tribally_owned_business = '1' THEN
      result := ARRAY_APPEND(result, 'tribally_owned_business');
    END IF;

    IF le.other_minority_owned_business = '1' THEN
      result := ARRAY_APPEND(result, 'other_minority_owned_business');
    END IF;

    IF    le.other_minority_owned_business = '1'
       OR 'alaskan_native_owned_business' = ANY(result)
       OR 'american_indian_owned_business' = ANY(result)
       OR 'asian_pacific_american_owned_business' = ANY(result)
       OR 'black_american_owned_business' = ANY(result)
       OR 'hispanic_american_owned_business' = ANY(result)
       OR 'native_american_owned_business' = ANY(result)
       OR 'native_hawaiian_owned_business' = ANY(result)
       OR 'subcontinent_asian_indian_american_owned_business' = ANY(result)
       OR 'tribally_owned_business' = ANY(result)
       OR 'other_minority_owned_business' = ANY(result)
    THEN
      result := ARRAY_APPEND(result, 'minority_owned_business');
    END IF;

        -- # End Minority Owned Business Category

        -- # Woman Owned Business Category
    IF le.women_owned_small_business = '1' THEN
      result := ARRAY_APPEND(result, 'women_owned_small_business');
    END IF;

    IF le.economically_disadvantaged_women_owned_small_business = '1' THEN
      result := ARRAY_APPEND(result, 'economically_disadvantaged_women_owned_small_business');
    END IF;

    IF le.joint_venture_women_owned_small_business = '1' THEN
      result := ARRAY_APPEND(result, 'joint_venture_women_owned_small_business');
    END IF;

    IF le.joint_venture_economic_disadvantaged_women_owned_small_bus = '1' THEN
      result := ARRAY_APPEND(result, 'joint_venture_economic_disadvantaged_women_owned_small_bus');
    END IF;

    IF    le.woman_owned_business = '1'
       OR 'women_owned_small_business' = ANY(result)
       OR 'economically_disadvantaged_women_owned_small_business' = ANY(result)
       OR 'joint_venture_women_owned_small_business' = ANY(result)
       OR 'joint_venture_economically_disadvantaged_women_owned_small_business' = ANY(result)
    THEN
      result := ARRAY_APPEND(result, 'woman_owned_business');
    END IF;

        -- # Veteran Owned Business Category
    IF le.service_disabled_veteran_owned_business = '1' THEN
      result := ARRAY_APPEND(result, 'service_disabled_veteran_owned_business');
    END IF;

    IF    le.veteran_owned_business = '1'
       OR 'service_disabled_veteran_owned_business' = ANY(result)
    THEN
      result := ARRAY_APPEND(result, 'veteran_owned_business');
    END IF;
        -- # End Veteran Owned Business

        -- # Special Designations Category
    IF le."8a_program_participant" = '1' THEN
      result := ARRAY_APPEND(result, '8a_program_participant');
    END IF;

    IF le.the_ability_one_program = '1' THEN
      result := ARRAY_APPEND(result, 'ability_one_program');
    END IF;

    IF le.dot_certified_disadvantage = '1' THEN
      result := ARRAY_APPEND(result, 'dot_certified_disadvantaged_business_enterprise');
    END IF;

    IF le.emerging_small_business = '1' THEN
      result := ARRAY_APPEND(result, 'emerging_small_business');
    END IF;

    IF le.federally_funded_research_and_development_corp = '1' THEN
      result := ARRAY_APPEND(result, 'federally_funded_research_and_development_corp');
    END IF;

    IF le.historically_underutilized_business_zone = '1' THEN
      result := ARRAY_APPEND(result, 'historically_underutilized_business_firm');
    END IF;

    IF le.labor_surplus_area_firm = '1' THEN
      result := ARRAY_APPEND(result, 'labor_surplus_area_firm');
    END IF;

    IF le.sba_certified_8a_joint_venture = '1' THEN
      result := ARRAY_APPEND(result, 'sba_certified_8a_joint_venture');
    END IF;

    IF le.self_certified_small_disadvantaged_business = '1' THEN
      result := ARRAY_APPEND(result, 'self_certified_small_disadvantaged_business');
    END IF;

    IF le.small_agricultural_cooperative = '1' THEN
      result := ARRAY_APPEND(result, 'small_agricultural_cooperative');
    END IF;

    IF le.small_disadvantaged_business = '1' THEN
      result := ARRAY_APPEND(result, 'small_disadvantaged_business');
    END IF;

    IF le.community_developed_corporation_owned_firm = '1' THEN
      result := ARRAY_APPEND(result, 'community_developed_corporation_owned_firm');
    END IF;

    IF le.domestic_or_foreign_entity = 'A' THEN
      result := ARRAY_APPEND(result, 'us_owned_business');
    ELSIF le.domestic_or_foreign_entity = 'C' THEN  -- Foreign-Owned Business Incorporated in the U.S.
      result := ARRAY_APPEND(result, 'foreign_owned_and_us_located_business');
    END IF;

    IF    le.domestic_or_foreign_entity = 'D'  -- Foreign-Owned Business Not Incorporated in the U.S.
       OR le.foreign_owned_and_located = '1' THEN
      result := ARRAY_APPEND(result, 'foreign_owned_and_located_business');
    END IF;

    IF le.foreign_government = '1' THEN
      result := ARRAY_APPEND(result, 'foreign_government');
    END IF;

    IF le.international_organization = '1' THEN
      result := ARRAY_APPEND(result, 'international_organization');
    END IF;

    IF    '8a_program_participant' = ANY(result)
       OR 'ability_one_program' = ANY(result)
       OR 'dot_certified_disadvantaged_business_enterprise' = ANY(result)
       OR 'emerging_small_business' = ANY(result)
       OR 'federally_funded_research_and_development_corp' = ANY(result)
       OR 'historically_underutilized_business_firm' = ANY(result)
       OR 'labor_surplus_area_firm' = ANY(result)
       OR 'sba_certified_8a_joint_venture' = ANY(result)
       OR 'self_certified_small_disadvanted_business' = ANY(result)
       OR 'small_agricultural_cooperative' = ANY(result)
       OR 'community_developed_corporation_owned_firm' = ANY(result)
       OR 'us_owned_business' = ANY(result)
       OR 'foreign_owned_and_us_located_business' = ANY(result)
       OR 'foreign_owned_and_located_business' = ANY(result)
       OR 'foreign_government' = ANY(result)
       OR 'international_organization' = ANY(result)
    THEN
      result := ARRAY_APPEND(result, 'special_designations');
    END IF;
        -- # End Special Designations

        -- # Non-profit category
    IF le.foundation = '1' THEN
      result := ARRAY_APPEND(result, 'foundation');
    END IF;

    IF le.community_developed_corporation_owned_firm = '1' THEN
      result := ARRAY_APPEND(result, 'community_development_corporations');
    END IF;

    IF    le.business_types = 'M'  -- Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)
       OR le.business_types = 'N'  -- Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)
       OR le.nonprofit_organization = '1'
       OR le.other_not_for_profit_organization = '1'
       OR 'foundation' = ANY(result)
       OR 'community_development_corporations' = ANY(result) THEN
      result := ARRAY_APPEND(result, 'nonprofit');
    END IF;
        -- # End Non-profit category

        -- # Higher Education Category
    IF    le.business_types = 'H'  -- Public/State Controlled Institution of Higher Education
       OR le.state_controlled_institution_of_higher_learning = '1'
       OR le."1862_land_grant_college" = '1'
       OR le."1890_land_grant_college" = '1'
       OR le."1994_land_grant_college" = '1' THEN
      result := ARRAY_APPEND(result, 'public_institution_of_higher_education');
    END IF;

    IF    le.business_types = 'O'  -- Private Institution of Higher Education
       OR le.private_university_or_college = '1' THEN
      result := ARRAY_APPEND(result, 'private_institution_of_higher_education');
    END IF;

    IF    le.business_types IN ('T', 'U', 'V', 'S')  -- Historically Black Colleges and Universities (HBCUs),
             -- Tribally Controlled Colleges and Universities (TCCUs), Alaska Native and Native Hawaiian Serving Institutions, Hispanic-serving Institution
       OR le.minority_institution = '1'
       OR le.historically_black_college = '1'
       OR le.tribal_college = '1'
       OR le.alaskan_native_servicing_institution = '1'
       OR le.native_hawaiian_servicing_institution = '1'
       OR le.hispanic_servicing_institution = '1' THEN
      result := ARRAY_APPEND(result, 'minority_serving_institution_of_higher_education');
    END IF;

    IF    'public_institution_of_higher_education' = ANY(result)
       OR 'private_institution_of_higher_education' = ANY(result)
       OR 'minority_serving_institution_of_higher_education' = ANY(result)
    THEN
      result := ARRAY_APPEND(result, 'higher_education');
    END IF;
        -- # End Higher Education Category

        -- # Government Category
    IF    le.us_federal_government = '1'
       OR le.federal_agency = '1'
       OR le.us_government_entity = '1'
       OR le.interstate_entity = '1' THEN
      result := ARRAY_APPEND(result, 'national_government');
    END IF;

    IF    le.business_types = 'A'  -- State government
       OR le.business_types = 'E'  -- Regional Organization
       OR le.us_state_government = '1'
       OR le.council_of_governments = '1' THEN
      result := ARRAY_APPEND(result, 'regional_and_state_government');
    END IF;

    IF    le.business_types = 'F' THEN  -- U.S. Territory or Possession
      result := ARRAY_APPEND(result, 'us_territory_or_possession');
    END IF;

    IF    le.business_types IN ('C', 'B', 'D', 'G')  -- City or Township, County, Special District Government; Independent School District
       OR le.city_local_government = '1'
       OR le.county_local_government = '1'
       OR le.inter_municipal_local_government = '1'
       OR le.municipality_local_government = '1'
       OR le.township_local_government = '1'
       OR le.us_local_government = '1'
       OR le.local_government_owned = '1'
       OR le.school_district_local_government = '1' THEN
      result := ARRAY_APPEND(result, 'local_government');
    END IF;
    IF    le.business_types IN ('I', 'J')  -- Indian/Native American Tribal Government: Federally Recognized, Other than Federally Recognized
       OR le.us_tribal_government = '1'
       OR le.indian_tribe_federally_recognized = '1' THEN
      result := ARRAY_APPEND(result, 'indian_native_american_tribal_government');
    END IF;

    IF    le.business_types = 'L'  -- Public/Indian Housing Authority
       OR le.housing_authorities_public_tribal = '1'
       OR le.airport_authority = '1'
       OR le.port_authority = '1'
       OR le.transit_authority = '1'
       OR le.planning_commission = '1' THEN
      result := ARRAY_APPEND(result, 'authorities_and_commissions');
    END IF;

    IF    'national_government' = ANY(result)
       OR 'regional_and_state_government' = ANY(result)
       OR 'us_territory_or_possession' = ANY(result)
       OR 'local_government' = ANY(result)
       OR 'indian_native_american_tribal_government' = ANY(result)
       OR 'authorities_and_commissions' = ANY(result) THEN
      result := ARRAY_APPEND(result, 'government');
    END IF;
        -- # End Government Category

        -- # Individuals Category
    IF    le.individual = '1'
       OR le.business_types = 'P' THEN  -- Individual
      result := ARRAY_APPEND(result, 'individuals');
    END IF;
        -- # End Individuals category

    RETURN result;
END;
$$ LANGUAGE plpgsql;
