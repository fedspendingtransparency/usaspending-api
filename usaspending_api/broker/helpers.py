from distutils.util import strtobool


def build_legal_entity_booleans_dict(row):
    bool_dict = \
        {
            'small_business_competitive': bool(strtobool(row['small_business_competitive']))
            if row['small_business_competitive'] else False,
            'city_local_government': bool(strtobool(row['city_local_government']))
            if row['city_local_government'] else False,
            'county_local_government': bool(strtobool(row['county_local_government']))
            if row['county_local_government'] else False,
            'inter_municipal_local_gove': bool(strtobool(row['inter_municipal_local_gove']))
            if row['inter_municipal_local_gove'] else False,
            'local_government_owned': bool(strtobool(row['local_government_owned']))
            if row['local_government_owned'] else False,
            'municipality_local_governm': bool(strtobool(row['municipality_local_governm']))
            if row['municipality_local_governm'] else False,
            'school_district_local_gove': bool(strtobool(row['school_district_local_gove']))
            if row['school_district_local_gove'] else False,
            'township_local_government': bool(strtobool(row['township_local_government']))
            if row['township_local_government'] else False,
            'us_state_government': bool(strtobool(row['us_state_government']))
            if row['us_state_government'] else False,
            'us_federal_government': bool(strtobool(row['us_federal_government']))
            if row['us_federal_government'] else False,
            'federal_agency': bool(strtobool(row['federal_agency']))
            if row['federal_agency'] else False,
            'federally_funded_research': bool(strtobool(row['federally_funded_research']))
            if row['federally_funded_research'] else False,
            'us_tribal_government': bool(strtobool(row['us_tribal_government']))
            if row['us_tribal_government'] else False,
            'foreign_government': bool(strtobool(row['foreign_government']))
            if row['foreign_government'] else False,
            'community_developed_corpor': bool(strtobool(row['community_developed_corpor']))
            if row['community_developed_corpor'] else False,
            'labor_surplus_area_firm': bool(strtobool(row['labor_surplus_area_firm']))
            if row['labor_surplus_area_firm'] else False,
            'corporate_entity_not_tax_e': bool(strtobool(row['corporate_entity_not_tax_e']))
            if row['corporate_entity_not_tax_e'] else False,
            'corporate_entity_tax_exemp': bool(strtobool(row['corporate_entity_tax_exemp']))
            if row['corporate_entity_tax_exemp'] else False,
            'partnership_or_limited_lia': bool(strtobool(row['partnership_or_limited_lia']))
            if row['partnership_or_limited_lia'] else False,
            'sole_proprietorship': bool(strtobool(row['sole_proprietorship']))
            if row['sole_proprietorship'] else False,
            'small_agricultural_coopera': bool(strtobool(row['small_agricultural_coopera']))
            if row['small_agricultural_coopera'] else False,
            'international_organization': bool(strtobool(row['international_organization']))
            if row['international_organization'] else False,
            'us_government_entity': bool(strtobool(row['us_government_entity']))
            if row['us_government_entity'] else False,
            'emerging_small_business': bool(strtobool(row['emerging_small_business']))
            if row['emerging_small_business'] else False,
            'c8a_program_participant': bool(strtobool(row['c8a_program_participant']))
            if row['c8a_program_participant'] else False,
            'sba_certified_8_a_joint_ve': bool(strtobool(row['sba_certified_8_a_joint_ve']))
            if row['sba_certified_8_a_joint_ve'] else False,
            'dot_certified_disadvantage': bool(strtobool(row['dot_certified_disadvantage']))
            if row['dot_certified_disadvantage'] else False,
            'self_certified_small_disad': bool(strtobool(row['self_certified_small_disad']))
            if row['self_certified_small_disad'] else False,
            'historically_underutilized': bool(strtobool(row['historically_underutilized']))
            if row['historically_underutilized'] else False,
            'small_disadvantaged_busine': bool(strtobool(row['small_disadvantaged_busine']))
            if row['small_disadvantaged_busine'] else False,
            'the_ability_one_program': bool(strtobool(row['the_ability_one_program']))
            if row['the_ability_one_program'] else False,
            'historically_black_college': bool(strtobool(row['historically_black_college']))
            if row['historically_black_college'] else False,
            'c1862_land_grant_college': bool(strtobool(row['c1862_land_grant_college']))
            if row['c1862_land_grant_college'] else False,
            'c1890_land_grant_college': bool(strtobool(row['c1890_land_grant_college']))
            if row['c1890_land_grant_college'] else False,
            'c1994_land_grant_college': bool(strtobool(row['c1994_land_grant_college']))
            if row['c1994_land_grant_college'] else False,
            'minority_institution': bool(strtobool(row['minority_institution']))
            if row['minority_institution'] else False,
            'private_university_or_coll': bool(strtobool(row['private_university_or_coll']))
            if row['private_university_or_coll'] else False,
            'school_of_forestry': bool(strtobool(row['school_of_forestry']))
            if row['school_of_forestry'] else False,
            'state_controlled_instituti': bool(strtobool(row['state_controlled_instituti']))
            if row['state_controlled_instituti'] else False,
            'tribal_college': bool(strtobool(row['tribal_college']))
            if row['tribal_college'] else False,
            'veterinary_college': bool(strtobool(row['veterinary_college']))
            if row['veterinary_college'] else False,
            'educational_institution': bool(strtobool(row['educational_institution']))
            if row['educational_institution'] else False,
            'alaskan_native_servicing_i': bool(strtobool(row['alaskan_native_servicing_i']))
            if row['alaskan_native_servicing_i'] else False,
            'community_development_corp': bool(strtobool(row['community_development_corp']))
            if row['community_development_corp'] else False,
            'native_hawaiian_servicing': bool(strtobool(row['native_hawaiian_servicing']))
            if row['native_hawaiian_servicing'] else False,
            'domestic_shelter': bool(strtobool(row['domestic_shelter']))
            if row['domestic_shelter'] else False,
            'manufacturer_of_goods': bool(strtobool(row['manufacturer_of_goods']))
            if row['manufacturer_of_goods'] else False,
            'hospital_flag': bool(strtobool(row['hospital_flag']))
            if row['hospital_flag'] else False,
            'veterinary_hospital': bool(strtobool(row['veterinary_hospital']))
            if row['veterinary_hospital'] else False,
            'hispanic_servicing_institu': bool(strtobool(row['hispanic_servicing_institu']))
            if row['hispanic_servicing_institu'] else False,
            'foundation': bool(strtobool(row['foundation']))
            if row['foundation'] else False,
            'woman_owned_business': bool(strtobool(row['woman_owned_business']))
            if row['woman_owned_business'] else False,
            'minority_owned_business': bool(strtobool(row['minority_owned_business']))
            if row['minority_owned_business'] else False,
            'women_owned_small_business': bool(strtobool(row['women_owned_small_business']))
            if row['women_owned_small_business'] else False,
            'economically_disadvantaged': bool(strtobool(row['economically_disadvantaged']))
            if row['economically_disadvantaged'] else False,
            'joint_venture_women_owned': bool(strtobool(row['joint_venture_women_owned']))
            if row['joint_venture_women_owned'] else False,
            'joint_venture_economically': bool(strtobool(row['joint_venture_economically']))
            if row['joint_venture_economically'] else False,
            'veteran_owned_business': bool(strtobool(row['veteran_owned_business']))
            if row['veteran_owned_business'] else False,
            'service_disabled_veteran_o': bool(strtobool(row['service_disabled_veteran_o']))
            if row['service_disabled_veteran_o'] else False,
            'contracts': bool(strtobool(row['contracts']))
            if row['contracts'] else False,
            'grants': bool(strtobool(row['grants']))
            if row['grants'] else False,
            'receives_contracts_and_gra': bool(strtobool(row['receives_contracts_and_gra']))
            if row['receives_contracts_and_gra'] else False,
            'airport_authority': bool(strtobool(row['airport_authority']))
            if row['airport_authority'] else False,
            'council_of_governments': bool(strtobool(row['council_of_governments']))
            if row['council_of_governments'] else False,
            'housing_authorities_public': bool(strtobool(row['housing_authorities_public']))
            if row['housing_authorities_public'] else False,
            'interstate_entity': bool(strtobool(row['interstate_entity']))
            if row['interstate_entity'] else False,
            'planning_commission': bool(strtobool(row['planning_commission']))
            if row['planning_commission'] else False,
            'port_authority': bool(strtobool(row['port_authority']))
            if row['port_authority'] else False,
            'transit_authority': bool(strtobool(row['transit_authority']))
            if row['transit_authority'] else False,
            'subchapter_s_corporation': bool(strtobool(row['subchapter_s_corporation']))
            if row['subchapter_s_corporation'] else False,
            'limited_liability_corporat': bool(strtobool(row['limited_liability_corporat']))
            if row['limited_liability_corporat'] else False,
            'foreign_owned_and_located': bool(strtobool(row['foreign_owned_and_located']))
            if row['foreign_owned_and_located'] else False,
            'american_indian_owned_busi': bool(strtobool(row['american_indian_owned_busi']))
            if row['american_indian_owned_busi'] else False,
            'alaskan_native_owned_corpo': bool(strtobool(row['alaskan_native_owned_corpo']))
            if row['alaskan_native_owned_corpo'] else False,
            'indian_tribe_federally_rec': bool(strtobool(row['indian_tribe_federally_rec']))
            if row['indian_tribe_federally_rec'] else False,
            'native_hawaiian_owned_busi': bool(strtobool(row['native_hawaiian_owned_busi']))
            if row['native_hawaiian_owned_busi'] else False,
            'tribally_owned_business': bool(strtobool(row['tribally_owned_business']))
            if row['tribally_owned_business'] else False,
            'asian_pacific_american_own': bool(strtobool(row['asian_pacific_american_own']))
            if row['asian_pacific_american_own'] else False,
            'black_american_owned_busin': bool(strtobool(row['black_american_owned_busin']))
            if row['black_american_owned_busin'] else False,
            'hispanic_american_owned_bu': bool(strtobool(row['hispanic_american_owned_bu']))
            if row['hispanic_american_owned_bu'] else False,
            'native_american_owned_busi': bool(strtobool(row['native_american_owned_busi']))
            if row['native_american_owned_busi'] else False,
            'subcontinent_asian_asian_i': bool(strtobool(row['subcontinent_asian_asian_i']))
            if row['subcontinent_asian_asian_i'] else False,
            'other_minority_owned_busin': bool(strtobool(row['other_minority_owned_busin']))
            if row['other_minority_owned_busin'] else False,
            'for_profit_organization': bool(strtobool(row['for_profit_organization']))
            if row['for_profit_organization'] else False,
            'nonprofit_organization': bool(strtobool(row['nonprofit_organization']))
            if row['nonprofit_organization'] else False,
            'other_not_for_profit_organ': bool(strtobool(row['other_not_for_profit_organ']))
            if row['other_not_for_profit_organ'] else False,
            'us_local_government': bool(strtobool(row['us_local_government']))
            if row['us_local_government'] else False
        }
    return bool_dict


def get_award_category(award_type_code):
    if award_type_code in ('A', 'B', 'C', 'D'):
        return 'contract'
    if award_type_code in ('02', '03', '04', '05'):
        return 'grant'
    if award_type_code in ('06', '10'):
        return 'direct payment'
    if award_type_code in ('07', '08'):
        return 'loans'
    if award_type_code == '09':
        return 'insurance'
    if award_type_code == '11':
        return 'other'
    return None


def get_assistance_type_description(type):
    if type == '02':
        return 'BLOCK GRANT'
    if type == '03':
        return 'FORMULA GRANT'
    if type == '04':
        return 'PROJECT GRANT'
    if type == '05':
        return 'COOPERATIVE AGREEMENT'
    if type == '06':
        return 'DIRECT PAYMENT FOR SPECIFIED USE'
    if type == '07':
        return 'DIRECT LOAN'
    if type == '08':
        return 'GUARANTEED/INSURED LOAN'
    if type == '09':
        return 'INSURANCE'
    if type == '10':
        return 'DIRECT PAYMENT WITH UNRESTRICTED USE'
    if type == '11':
        return 'OTHER FINANCIAL ASSISTANCE'
    return 'UNKNOWN ASSISTANCE TYPE'


def get_business_type_description(business_type):
    if business_type in ('A', '00'):
        return 'STATE GOVERNMENT'
    if business_type in ('B', '01'):
        return 'COUNTY GOVERNMENT'
    if business_type in ('C', '02'):
        return 'CITY OR TOWNSHIP GOVERNMENT'
    if business_type in ('D', '04'):
        return 'SPECIAL DISTRICT GOVERNMENT'
    if business_type == 'E':
        return 'REGIONAL ORGANIZATION'
    if business_type == 'F':
        return 'U.S. TERRITORY OR POSSESSION'
    if business_type in ('G', '05'):
        return 'INDEPENDENT SCHOOL DISTRICT'
    if business_type in ('H', '06'):
        return 'PUBLIC/STATE CONTROLLED INSTITUTION OF HIGHER EDUCATION'
    if business_type == 'I':
        return 'INDIAN/NATIVE AMERICAN TRIBAL GOVERNMENT (FEDERALLY RECOGNIZED)'
    if business_type == 'J':
        return 'INDIAN/NATIVE AMERICAN TRIBAL GOVERNMENT (OTHER THAN FEDERALLY RECOGNIZED)'
    if business_type in ('K', '11'):
        return 'INDIAN/NATIVE AMERICAN TRIBAL DESIGNATED ORGANIZATION'
    if business_type == '12':
        return 'OTHER NON-PROFIT'
    if business_type == 'L':
        return 'PUBLIC/INDIAN HOUSING AUTHORITY'
    if business_type == 'M':
        return 'NONPROFIT WITH 501C3 IRS STATUS (OTHER THAN INSTITUTION OF HIGHER EDUCATION)'
    if business_type == 'N':
        return 'NONPROFIT WITHOUT 501C3 IRS STATUS (OTHER THAN INSTITUTION OF HIGHER EDUCATION)'
    if business_type in ('O', '20'):
        return 'PRIVATE INSTITUTION OF HIGHER EDUCATION'
    if business_type in ('P', '21'):
        return 'INDIVIDUAL'
    if business_type in ('Q', '22'):
        return 'FOR-PROFIT ORGANIZATION (OTHER THAN SMALL BUSINESS)'
    if business_type in ('R', '23'):
        return 'SMALL BUSINESS'
    if business_type == 'S':
        return 'HISPANIC-SERVING INSTITUTION'
    if business_type == 'T':
        return 'HISTORICALLY BLACK COLLEGES AND UNIVERSITIES (HBCUS)'
    if business_type == 'U':
        return 'TRIBALLY CONTROLLED COLLEGES AND UNIVERSITIES (TCCUS)'
    if business_type == 'V':
        return 'ALASKA NATIVE AND NATIVE HAWAIIAN SERVING INSTITUTIONS'
    if business_type == 'W':
        return 'NON-DOMESTIC (NON-US) ENTITY'
    if business_type in ('X', '25'):
        return 'OTHER'
    return 'UNKNOWN TYPES'


def get_business_categories(row, data_type):
    business_category_set = set()

    if data_type == 'fabs':
        # BUSINESS (FOR-PROFIT)
        business_types = row.get('business_types')
        if business_types in ('R', '23'):
            business_category_set.add('small_business')

        if business_types in ('Q', '22'):
            business_category_set.add('other_than_small_business')

        if business_category_set & {'small_business', 'other_than_small_business'}:
            business_category_set.add('category_business')

        # NON-PROFIT
        if business_types in ('M', 'N', '12'):
            business_category_set.add('nonprofit')

        # HIGHER EDUCATION
        if business_types in ('H', '06'):
            business_category_set.add('public_institution_of_higher_education')

        if business_types in ('O', '20'):
            business_category_set.add('private_institution_of_higher_education')

        if business_types in ('T', 'U', 'V', 'S'):
            business_category_set.add('minority_serving_institution_of_higher_education')

        if business_category_set & {'public_institution_of_higher_education', 'private_institution_of_higher_education',
                                    'minority_serving_institution_of_higher_education'}:
            business_category_set.add('higher_education')

        # GOVERNMENT
        if business_types in ('A', '00'):
            business_category_set.add('regional_and_state_government')

        if business_types == 'E':
            business_category_set.add('regional_organization')

        if business_types == 'F':
            business_category_set.add('us_territory_or_possession')

        if business_types in ('B', 'C', 'D', 'G', '01', '02', '04', '05'):
            business_category_set.add('local_government')

        if business_types in ('I', 'J', 'K', '11'):
            business_category_set.add('indian_native_american_tribal_government')

        if business_types == 'L':
            business_category_set.add('authorities_and_commissions')

        if business_category_set & {'regional_and_state_government', 'us_territory_or_possession', 'local_government',
                                    'indian_native_american_tribal_government', 'authorities_and_commissions',
                                    'regional_organization'}:
            business_category_set.add('government')

        # INDIVIDUALS
        if business_types in ('P', '21'):
            business_category_set.add('individuals')

        return list(business_category_set)

    elif data_type == 'fpds':
        legal_entity_bool_dict = build_legal_entity_booleans_dict(row)

        # BUSINESS (FOR-PROFIT)
        if row.get('contracting_officers_deter') == 'S' \
                or legal_entity_bool_dict['women_owned_small_business'] is True \
                or legal_entity_bool_dict['economically_disadvantaged'] is True \
                or legal_entity_bool_dict['joint_venture_women_owned'] is True \
                or legal_entity_bool_dict['emerging_small_business'] is True \
                or legal_entity_bool_dict['self_certified_small_disad'] is True \
                or legal_entity_bool_dict['small_agricultural_coopera'] is True \
                or legal_entity_bool_dict['small_disadvantaged_busine'] is True:
            business_category_set.add('small_business')

        if row.get('contracting_officers_deter') == 'O':
            business_category_set.add('other_than_small_business')

        if legal_entity_bool_dict['corporate_entity_tax_exemp'] is True:
            business_category_set.add('corporate_entity_tax_exempt')

        if legal_entity_bool_dict['corporate_entity_not_tax_e'] is True:
            business_category_set.add('corporate_entity_not_tax_exempt')

        if legal_entity_bool_dict['partnership_or_limited_lia'] is True:
            business_category_set.add('partnership_or_limited_liability_partnership')

        if legal_entity_bool_dict['sole_proprietorship'] is True:
            business_category_set.add('sole_proprietorship')

        if legal_entity_bool_dict['manufacturer_of_goods'] is True:
            business_category_set.add('manufacturer_of_goods')

        if legal_entity_bool_dict['subchapter_s_corporation'] is True:
            business_category_set.add('subchapter_s_corporation')

        if legal_entity_bool_dict['limited_liability_corporat'] is True:
            business_category_set.add('limited_liability_corporation')

        if legal_entity_bool_dict['for_profit_organization'] is True or \
                (business_category_set & {'small_business', 'other_than_small_business',
                                          'corporate_entity_tax_exempt', 'corporate_entity_not_tax_exempt',
                                          'partnership_or_limited_liability_partnership', 'sole_proprietorship',
                                          'manufacturer_of_goods', 'subchapter_s_corporation',
                                          'limited_liability_corporation'}):
            business_category_set.add('category_business')

        # MINORITY BUSINESS
        if legal_entity_bool_dict['alaskan_native_owned_corpo'] is True:
            business_category_set.add('alaskan_native_owned_business')

        if legal_entity_bool_dict['american_indian_owned_busi'] is True:
            business_category_set.add('american_indian_owned_business')

        if legal_entity_bool_dict['asian_pacific_american_own'] is True:
            business_category_set.add('asian_pacific_american_owned_business')

        if legal_entity_bool_dict['black_american_owned_busin'] is True:
            business_category_set.add('black_american_owned_business')

        if legal_entity_bool_dict['hispanic_american_owned_bu'] is True:
            business_category_set.add('hispanic_american_owned_business')

        if legal_entity_bool_dict['native_american_owned_busi'] is True:
            business_category_set.add('native_american_owned_business')

        if legal_entity_bool_dict['native_hawaiian_owned_busi'] is True:
            business_category_set.add('native_hawaiian_owned_business')

        if legal_entity_bool_dict['subcontinent_asian_asian_i'] is True:
            business_category_set.add('subcontinent_asian_indian_american_owned_business')

        if legal_entity_bool_dict['tribally_owned_business'] is True:
            business_category_set.add('tribally_owned_business')

        if legal_entity_bool_dict['other_minority_owned_busin'] is True:
            business_category_set.add('other_minority_owned_business')

        if legal_entity_bool_dict['minority_owned_business'] is True or \
                (business_category_set & {'alaskan_native_owned_business', 'american_indian_owned_business',
                                          'asian_pacific_american_owned_business', 'black_american_owned_business',
                                          'hispanic_american_owned_business', 'native_american_owned_business',
                                          'native_hawaiian_owned_business',
                                          'subcontinent_asian_indian_american_owned_business',
                                          'tribally_owned_business',
                                          'other_minority_owned_business'}):
            business_category_set.add('minority_owned_business')

        # WOMEN OWNED BUSINESS
        if legal_entity_bool_dict['women_owned_small_business'] is True:
            business_category_set.add('women_owned_small_business')

        if legal_entity_bool_dict['economically_disadvantaged'] is True:
            business_category_set.add('economically_disadvantaged_women_owned_small_business')

        if legal_entity_bool_dict['joint_venture_women_owned'] is True:
            business_category_set.add('joint_venture_women_owned_small_business')

        if legal_entity_bool_dict['joint_venture_economically'] is True:
            business_category_set.add('joint_venture_economically_disadvantaged_women_owned_small_business')

        if legal_entity_bool_dict['woman_owned_business'] is True or \
                (business_category_set & {'women_owned_small_business',
                                          'economically_disadvantaged_women_owned_small_business',
                                          'joint_venture_women_owned_small_business',
                                          'joint_venture_economically_disadvantaged_women_owned_small_business'}):
            business_category_set.add('woman_owned_business')

        # VETERAN OWNED BUSINESS
        if legal_entity_bool_dict['service_disabled_veteran_o'] is True:
            business_category_set.add('service_disabled_veteran_owned_business')

        if legal_entity_bool_dict['veteran_owned_business'] is True or (
                    business_category_set & {'service_disabled_veteran_owned_business'}):
            business_category_set.add('veteran_owned_business')

        # SPECIAL DESIGNATIONS
        if legal_entity_bool_dict['c8a_program_participant'] is True:
            business_category_set.add('8a_program_participant')

        if legal_entity_bool_dict['the_ability_one_program'] is True:
            business_category_set.add('ability_one_program')

        if legal_entity_bool_dict['dot_certified_disadvantage'] is True:
            business_category_set.add('dot_certified_disadvantaged_business_enterprise')

        if legal_entity_bool_dict['emerging_small_business'] is True:
            business_category_set.add('emerging_small_business')

        if legal_entity_bool_dict['federally_funded_research'] is True:
            business_category_set.add('federally_funded_research_and_development_corp')

        if legal_entity_bool_dict['historically_underutilized'] is True:
            business_category_set.add('historically_underutilized_business_firm')

        if legal_entity_bool_dict['labor_surplus_area_firm'] is True:
            business_category_set.add('labor_surplus_area_firm')

        if legal_entity_bool_dict['sba_certified_8_a_joint_ve'] is True:
            business_category_set.add('sba_certified_8a_joint_venture')

        if legal_entity_bool_dict['self_certified_small_disad'] is True:
            business_category_set.add('self_certified_small_disadvanted_business')

        if legal_entity_bool_dict['small_agricultural_coopera'] is True:
            business_category_set.add('small_agricultural_cooperative')

        if legal_entity_bool_dict['small_disadvantaged_busine'] is True:
            business_category_set.add('small_disadvantaged_business')

        if legal_entity_bool_dict['community_developed_corpor'] is True:
            business_category_set.add('community_developed_corporation_owned_firm')

        # U.S. Owned Business
        if row.get('domestic_or_foreign_entity') == 'A':
            business_category_set.add('us_owned_business')

        # Foreign-Owned Business Incorporated in the U.S.
        if row.get('domestic_or_foreign_entity') == 'C':
            business_category_set.add('foreign_owned_and_us_located_business')

        # Foreign-Owned Business Not Incorporated in the U.S.
        if row.get('domestic_or_foreign_entity') == 'D' or legal_entity_bool_dict['foreign_owned_and_located'] is True:
            business_category_set.add('foreign_owned_and_located_business')

        if legal_entity_bool_dict['foreign_government'] is True:
            business_category_set.add('foreign_government')

        if legal_entity_bool_dict['international_organization'] is True:
            business_category_set.add('international_organization')

        if legal_entity_bool_dict['domestic_shelter'] is True:
            business_category_set.add('domestic_shelter')

        if legal_entity_bool_dict['hospital_flag'] is True:
            business_category_set.add('hospital')

        if legal_entity_bool_dict['veterinary_hospital'] is True:
            business_category_set.add('veterinary_hospital')

        if business_category_set & {'8a_program_participant', 'ability_one_program',
                                    'dot_certified_disadvantaged_business_enterprise', 'emerging_small_business',
                                    'federally_funded_research_and_development_corp',
                                    'historically_underutilized_business_firm', 'labor_surplus_area_firm',
                                    'sba_certified_8a_joint_venture', 'self_certified_small_disadvanted_business',
                                    'small_agricultural_cooperative', 'small_disadvantaged_business',
                                    'community_developed_corporation_owned_firm', 'us_owned_business',
                                    'foreign_owned_and_us_located_business', 'foreign_owned_and_located_business',
                                    'foreign_government', 'international_organization', 'domestic_shelter',
                                    'hospital', 'veterinary_hospital'}:
            business_category_set.add('special_designations')

        # NON-PROFIT
        if legal_entity_bool_dict['foundation'] is True:
            business_category_set.add('foundation')

        if legal_entity_bool_dict['community_development_corp'] is True:
            business_category_set.add('community_development_corporations')

        if legal_entity_bool_dict['nonprofit_organization'] is True \
                or legal_entity_bool_dict['other_not_for_profit_organ'] is True or \
                (business_category_set & {'foundation', 'community_development_corporations'}):
            business_category_set.add('nonprofit')

        # HIGHER EDUCATION
        if legal_entity_bool_dict['educational_institution'] is True:
            business_category_set.add('educational_institution')

        if legal_entity_bool_dict['state_controlled_instituti'] is True \
                or legal_entity_bool_dict['c1862_land_grant_college'] is True \
                or legal_entity_bool_dict['c1890_land_grant_college'] is True \
                or legal_entity_bool_dict['c1994_land_grant_college'] is True:
            business_category_set.add('public_institution_of_higher_education')

        if legal_entity_bool_dict['private_university_or_coll'] is True:
            business_category_set.add('private_institution_of_higher_education')

        if legal_entity_bool_dict['minority_institution'] is True \
                or legal_entity_bool_dict['historically_black_college'] is True \
                or legal_entity_bool_dict['tribal_college'] is True \
                or legal_entity_bool_dict['alaskan_native_servicing_i'] is True \
                or legal_entity_bool_dict['native_hawaiian_servicing'] is True \
                or legal_entity_bool_dict['hispanic_servicing_institu'] is True:
            business_category_set.add('minority_serving_institution_of_higher_education')

        if legal_entity_bool_dict['school_of_forestry'] is True:
            business_category_set.add('school_of_forestry')

        if legal_entity_bool_dict['veterinary_college'] is True:
            business_category_set.add('veterinary_college')

        if business_category_set & {'educational_institution', 'public_institution_of_higher_education',
                                    'private_institution_of_higher_education', 'school_of_forestry',
                                    'minority_serving_institution_of_higher_education', 'veterinary_college'}:
            business_category_set.add('higher_education')

        # GOVERNMENT
        if legal_entity_bool_dict['us_federal_government'] is True \
                or legal_entity_bool_dict['federal_agency'] is True \
                or legal_entity_bool_dict['us_government_entity'] is True:
            business_category_set.add('national_government')

        if legal_entity_bool_dict['interstate_entity'] is True:
            business_category_set.add('interstate_entity')

        if legal_entity_bool_dict['us_state_government'] is True:
            business_category_set.add('regional_and_state_government')

        if legal_entity_bool_dict['council_of_governments'] is True:
            business_category_set.add('council_of_governments')

        if legal_entity_bool_dict['city_local_government'] is True \
                or legal_entity_bool_dict['county_local_government'] is True \
                or legal_entity_bool_dict['inter_municipal_local_gove'] is True \
                or legal_entity_bool_dict['municipality_local_governm'] is True \
                or legal_entity_bool_dict['township_local_government'] is True \
                or legal_entity_bool_dict['us_local_government'] is True \
                or legal_entity_bool_dict['local_government_owned'] is True \
                or legal_entity_bool_dict['school_district_local_gove'] is True:
            business_category_set.add('local_government')

        if legal_entity_bool_dict['us_tribal_government'] is True \
                or legal_entity_bool_dict['indian_tribe_federally_rec'] is True:
            business_category_set.add('indian_native_american_tribal_government')

        if legal_entity_bool_dict['housing_authorities_public'] is True \
                or legal_entity_bool_dict['airport_authority'] is True \
                or legal_entity_bool_dict['port_authority'] is True \
                or legal_entity_bool_dict['transit_authority'] is True \
                or legal_entity_bool_dict['planning_commission'] is True:
            business_category_set.add('authorities_and_commissions')

        if business_category_set & {'national_government', 'regional_and_state_government', 'local_government',
                                    'indian_native_american_tribal_government', 'authorities_and_commissions',
                                    'interstate_entity', 'council_of_governments'}:
            business_category_set.add('government')

        return list(business_category_set)
    else:
        raise ValueError("Invalid object type provided to update_business_categories. "
                         "Must be one of the following types: TransactionFPDS, TransactionFABS")


def set_legal_entity_boolean_fields(row):
    """ in place updates to specific fields to be mapped as booleans """
    legal_entity_bool_dict = build_legal_entity_booleans_dict(row)
    for key in legal_entity_bool_dict:
        row[key] = legal_entity_bool_dict[key]
