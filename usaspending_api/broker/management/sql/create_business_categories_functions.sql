create or replace function compile_fpds_business_categories(small_business_competitive text, for_profit_organization text, alaskan_native_owned_corpo text, american_indian_owned_busi text, asian_pacific_american_own text, black_american_owned_busin text, hispanic_american_owned_bu text, native_american_owned_busi text, native_hawaiian_owned_busi text, subcontinent_asian_asian_i text, tribally_owned_business text, other_minority_owned_busin text, minority_owned_business text, women_owned_small_business text, economically_disadvantaged text, joint_venture_women_owned text, joint_venture_economically text, woman_owned_business text, service_disabled_veteran_o text, veteran_owned_business text, c8a_program_participant text, the_ability_one_program text, dot_certified_disadvantage text, emerging_small_business text, federally_funded_research text, historically_underutilized text, labor_surplus_area_firm text, sba_certified_8_a_joint_ve text, self_certified_small_disad text, small_agricultural_coopera text, small_disadvantaged_busine text, community_developed_corpor text, domestic_or_foreign_entity text, foreign_owned_and_located text, foreign_government text, international_organization text, foundation text, community_development_corp text, nonprofit_organization text, other_not_for_profit_organ text, state_controlled_instituti text, c1862_land_grant_college text, c1890_land_grant_college text, c1994_land_grant_college text, private_university_or_coll text, minority_institution text, historically_black_college text, tribal_college text, alaskan_native_servicing_i text, native_hawaiian_servicing text, hispanic_servicing_institu text, us_federal_government text, federal_agency text, us_government_entity text, interstate_entity text, us_state_government text, council_of_governments text, city_local_government text, county_local_government text, inter_municipal_local_gove text, municipality_local_governm text, township_local_government text, us_local_government text, local_government_owned text, school_district_local_gove text, us_tribal_government text, indian_tribe_federally_rec text, housing_authorities_public text, airport_authority text, port_authority text, transit_authority text, planning_commission text)
returns text[] as $$
declare
    bc_arr text[];
begin

    -- SMALL BUSINESS
    if small_business_competitive = '1' or for_profit_organization = '1'
    then
        bc_arr := bc_arr || array['small_business', 'category_business'];
    end if;

    -- MINORITY BUSINESS
    if alaskan_native_owned_corpo = '1'
    then
        bc_arr := bc_arr || array['alaskan_native_owned_business'];
    end if;

    if american_indian_owned_busi = '1'
    then
        bc_arr := bc_arr || array['american_indian_owned_business'];
    end if;

    if asian_pacific_american_own = '1'
    then
        bc_arr := bc_arr || array['asian_pacific_american_owned_business'];
    end if;

    if black_american_owned_busin = '1'
    then
        bc_arr := bc_arr || array['black_american_owned_business'];
    end if;

    if hispanic_american_owned_bu = '1'
    then
        bc_arr := bc_arr || array['hispanic_american_owned_business'];
    end if;

    if native_american_owned_busi = '1'
    then
        bc_arr := bc_arr || array['native_american_owned_business'];
    end if;

    if native_hawaiian_owned_busi = '1'
    then
        bc_arr := bc_arr || array['native_hawaiian_owned_business'];
    end if;

    if subcontinent_asian_asian_i = '1'
    then
        bc_arr := bc_arr || array['subcontinent_asian_indian_american_owned_business'];
    end if;

    if tribally_owned_business = '1'
    then
        bc_arr := bc_arr || array['tribally_owned_business'];
    end if;

    if other_minority_owned_busin = '1'
    then
        bc_arr := bc_arr || array['other_minority_owned_business'];
    end if;

    if minority_owned_business = '1' or (bc_arr && array['alaskan_native_owned_business', 'american_indian_owned_business', 'asian_pacific_american_owned_business', 'black_american_owned_business', 'hispanic_american_owned_business', 'native_american_owned_business', 'native_hawaiian_owned_business', 'subcontinent_asian_indian_american_owned_business', 'tribally_owned_business', 'other_minority_owned_business'])
    then
        bc_arr := bc_arr || array['minority_owned_business'];
    end if;

    -- WOMEN OWNED BUSINESS
    if women_owned_small_business = '1'
    then
        bc_arr := bc_arr || array['women_owned_small_business'];
    end if;

    if economically_disadvantaged = '1'
    then
        bc_arr := bc_arr || array['economically_disadvantaged_women_owned_small_business'];
    end if;

    if joint_venture_women_owned = '1'
    then
        bc_arr := bc_arr || array['joint_venture_women_owned_small_business'];
    end if;

    if joint_venture_economically = '1'
    then
        bc_arr := bc_arr || array['joint_venture_economically_disadvantaged_women_owned_small_business'];
    end if;

    if woman_owned_business = '1' or (bc_arr && array['women_owned_small_business', 'economically_disadvantaged_women_owned_small_business', 'joint_venture_women_owned_small_business', 'joint_venture_economically_disadvantaged_women_owned_small_business'])
    then
        bc_arr := bc_arr || array['woman_owned_business'];
    end if;

    -- VETERAN OWNED BUSINESS
    if service_disabled_veteran_o = '1'
    then
        bc_arr := bc_arr || array['service_disabled_veteran_owned_business'];
    end if;

    if veteran_owned_business = '1' or (bc_arr && array['service_disabled_veteran_owned_business'])
    then
        bc_arr := bc_arr || array['veteran_owned_business'];
    end if;

    -- SPECIAL DESIGNATIONS
    if c8a_program_participant = '1'
    then
        bc_arr := bc_arr || array['8a_program_participant'];
    end if;

    if the_ability_one_program = '1'
    then
        bc_arr := bc_arr || array['ability_one_program'];
    end if;

    if dot_certified_disadvantage = '1'
    then
        bc_arr := bc_arr || array['dot_certified_disadvantaged_business_enterprise'];
    end if;

    if emerging_small_business = '1'
    then
        bc_arr := bc_arr || array['emerging_small_business'];
    end if;

    if federally_funded_research = '1'
    then
        bc_arr := bc_arr || array['federally_funded_research_and_development_corp'];
    end if;

    if historically_underutilized = '1'
    then
        bc_arr := bc_arr || array['historically_underutilized_business_firm'];
    end if;

    if labor_surplus_area_firm = '1'
    then
        bc_arr := bc_arr || array['labor_surplus_area_firm'];
    end if;

    if sba_certified_8_a_joint_ve = '1'
    then
        bc_arr := bc_arr || array['sba_certified_8a_joint_venture'];
    end if;

    if self_certified_small_disad = '1'
    then
        bc_arr := bc_arr || array['self_certified_small_disadvanted_business'];
    end if;

    if small_agricultural_coopera = '1'
    then
        bc_arr := bc_arr || array['small_agricultural_cooperative'];
    end if;

    if small_disadvantaged_busine = '1'
    then
        bc_arr := bc_arr || array['small_disadvantaged_business'];
    end if;

    if community_developed_corpor = '1'
    then
        bc_arr := bc_arr || array['community_developed_corporation_owned_firm'];
    end if;

    -- U.S. Owned Business
    if domestic_or_foreign_entity = 'A'
    then
        bc_arr := bc_arr || array['us_owned_business'];
    end if;

    -- Foreign-Owned Business Incorporated in the U.S.
    if domestic_or_foreign_entity = 'C'
    then
        bc_arr := bc_arr || array['foreign_owned_and_us_located_business'];
    end if;

    -- Foreign-Owned Business Not Incorporated in the U.S.
    if domestic_or_foreign_entity = 'D' or foreign_owned_and_located = '1'
    then
        bc_arr := bc_arr || array['foreign_owned_and_located_business'];
    end if;

    if foreign_government = '1'
    then
        bc_arr := bc_arr || array['foreign_government'];
    end if;

    if international_organization = '1'
    then
        bc_arr := bc_arr || array['international_organization'];
    end if;

    if bc_arr && array['8a_program_participant', 'ability_one_program', 'dot_certified_disadvantaged_business_enterprise', 'emerging_small_business', 'federally_funded_research_and_development_corp', 'historically_underutilized_business_firm', 'labor_surplus_area_firm', 'sba_certified_8a_joint_venture', 'self_certified_small_disadvanted_business', 'small_agricultural_cooperative', 'small_disadvantaged_business', 'community_developed_corporation_owned_firm', 'us_owned_business', 'foreign_owned_and_us_located_business', 'foreign_owned_and_located_business', 'foreign_government', 'international_organization']
    then
        bc_arr := bc_arr || array['special_designations'];
    end if;

    -- NON-PROFIT
    if foundation = '1'
    then
        bc_arr := bc_arr || array['foundation'];
    end if;

    if community_development_corp = '1'
    then
        bc_arr := bc_arr || array['community_development_corporations'];
    end if;

    if nonprofit_organization = '1' or other_not_for_profit_organ = '1' or (bc_arr && array['foundation', 'community_development_corporations'])
    then
        bc_arr := bc_arr || array['nonprofit'];
    end if;

    -- HIGHER EDUCATION
    if state_controlled_instituti = '1' or c1862_land_grant_college = '1' or c1890_land_grant_college = '1' or c1994_land_grant_college = '1'
    then
        bc_arr := bc_arr || array['public_institution_of_higher_education'];
    end if;

    if private_university_or_coll = '1'
    then
        bc_arr := bc_arr || array['private_institution_of_higher_education'];
    end if;

    if minority_institution = '1' or historically_black_college = '1' or tribal_college = '1' or alaskan_native_servicing_i = '1' or native_hawaiian_servicing = '1' or hispanic_servicing_institu = '1'
    then
        bc_arr := bc_arr || array['minority_serving_institution_of_higher_education'];
    end if;

    if bc_arr && array['public_institution_of_higher_education', 'private_institution_of_higher_education', 'minority_serving_institution_of_higher_education']
    then
        bc_arr := bc_arr || array['higher_education'];
    end if;

    -- GOVERNMENT
    if us_federal_government = '1' or federal_agency = '1' or us_government_entity = '1' or interstate_entity = '1'
    then
        bc_arr := bc_arr || array['national_government'];
    end if;

    if us_state_government = '1' or council_of_governments = '1'
    then
        bc_arr := bc_arr || array['regional_and_state_government'];
    end if;

    if city_local_government = '1' or county_local_government = '1' or inter_municipal_local_gove = '1' or municipality_local_governm = '1' or township_local_government = '1' or us_local_government = '1' or local_government_owned = '1' or school_district_local_gove = '1'
    then
        bc_arr := bc_arr || array['local_government'];
    end if;

    if us_tribal_government = '1' or indian_tribe_federally_rec = '1'
    then
        bc_arr := bc_arr || array['indian_native_american_tribal_government'];
    end if;

    if housing_authorities_public = '1' or airport_authority = '1' or port_authority = '1' or transit_authority = '1' or planning_commission = '1'
    then
        bc_arr := bc_arr || array['authorities_and_commissions'];
    end if;

    if bc_arr && array['national_government', 'regional_and_state_government', 'us_territory_or_possession', 'local_government', 'indian_native_american_tribal_government', 'authorities_and_commissions']
    then
        bc_arr := bc_arr || array['government'];
    end if;

    return bc_arr;
end;
$$  language plpgsql;

create or replace function compile_fabs_business_categories(business_types text)
returns text[] as $$
declare
    bc_arr text[];
begin
    if business_types in ('R', '23')
    then
        bc_arr := bc_arr || array['small_business'];
    end if;

    if business_types in ('Q', '22')
    then
        bc_arr := bc_arr || array['other_than_small_business'];
    end if;

    if bc_arr && array['small_business', 'other_than_small_business']
    then
        bc_arr := bc_arr || array['category_business'];
    end if;

    if business_types in ('M', 'N', '12')
    then
        bc_arr := bc_arr || array['nonprofit'];
    end if;

    if business_types in ('H', '06')
    then
        bc_arr := bc_arr || array['public_institution_of_higher_education'];
    end if;

    if business_types in ('O', '20')
    then
        bc_arr := bc_arr || array['private_institution_of_higher_education'];
    end if;

    if business_types in ('T', 'U', 'V', 'S')
    then
        bc_arr := bc_arr || array['minority_serving_institution_of_higher_education'];
    end if;

    if bc_arr && array['public_institution_of_higher_education', 'private_institution_of_higher_education', 'minority_serving_institution_of_higher_education']
    then
        bc_arr := bc_arr || array['higher_education'];
    end if;

    if business_types in ('A', 'E', '00')
    then
        bc_arr := bc_arr || array['regional_and_state_government'];
    end if;

    if business_types in ('F')
    then
        bc_arr := bc_arr || array['us_territory_or_possession'];
    end if;

    if business_types in ('B', 'C', 'D', 'G', '01', '02', '04', '05')
    then
        bc_arr := bc_arr || array['local_government'];
    end if;

    if business_types in ('I', 'J', '11')
    then
        bc_arr := bc_arr || array['indian_native_american_tribal_government'];
    end if;

    if business_types in ('L')
    then
        bc_arr := bc_arr || array['authorities_and_commissions'];
    end if;

    if bc_arr && array['regional_and_state_government', 'us_territory_or_possession', 'local_government', 'indian_native_american_tribal_government', 'authorities_and_commissions']
    then
        bc_arr := bc_arr || array['government'];
    end if;

    if business_types in ('P', '21')
    then
        bc_arr := bc_arr || array['individuals'];
    end if;

    return bc_arr;
end;
$$  language plpgsql;