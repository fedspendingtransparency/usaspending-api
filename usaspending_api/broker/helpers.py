from distutils.util import strtobool


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
        business_types = row.get('business_types')
        if business_types in ('R', '23'):
            business_category_set.add('small_business')

        if business_types in ('Q', '22'):
            business_category_set.add('other_than_small_business')

        if business_category_set & {'small_business', 'other_than_small_business'}:
            business_category_set.add('category_business')

        if business_types in ('M', 'N', '12'):
            business_category_set.add('nonprofit')

        if business_types in ('H', '06'):
            business_category_set.add('public_institution_of_higher_education')

        if business_types in ('O', '20'):
            business_category_set.add('private_institution_of_higher_education')

        if business_types in ('T', 'U', 'V', 'S'):
            business_category_set.add('minority_serving_institution_of_higher_education')

        if business_category_set & {'public_institution_of_higher_education', 'private_institution_of_higher_education',
                                'minority_serving_institution_of_higher_education'}:
            business_category_set.add('higher_education')

        if business_types in ('A', 'E', '00'):
            business_category_set.add('regional_and_state_government')

        if business_types == 'F':
            business_category_set.add('us_territory_or_possession')

        if business_types in ('B', 'C', 'D', 'G', '01', '02', '04', '05'):
            business_category_set.add('local_government')

        if business_types in ('I', 'J', 'K', '11'):
            business_category_set.add('indian_native_american_tribal_government')

        if business_types == 'L':
            business_category_set.add('authorities_and_commissions')

        if business_category_set & {'regional_and_state_government', 'us_territory_or_possession', 'local_government',
                                'indian_native_american_tribal_government', 'authorities_and_commissions'}:
            business_category_set.add('government')

        if business_types in ('P', '21'):
            business_category_set.add('individuals')

        return list(business_category_set)

    elif data_type == 'fpds':
        # SMALL BUSINESS
        if strtobool(row.get('small_business_competitive', '0')) is True or \
                        strtobool(row.get('for_profit_organization', '0')) is True:
            business_category_set.add({'small_business', 'category_business'})

        # MINORITY BUSINESS
        if strtobool(row.get('alaskan_native_owned_corpo', '0')) is True:
            business_category_set.add({'alaskan_native_owned_business'})

        if strtobool(row.get('american_indian_owned_busi', '0')) is True:
            business_category_set.add({'american_indian_owned_business'})

        if strtobool(row.get('asian_pacific_american_own', '0')) is True:
            business_category_set.add({'asian_pacific_american_owned_business'})

        if strtobool(row.get('black_american_owned_busin', '0')) is True:
            business_category_set.add({'black_american_owned_business'})

        if strtobool(row.get('hispanic_american_owned_bu', '0')) is True:
            business_category_set.add({'hispanic_american_owned_business'})

        if strtobool(row.get('native_american_owned_busi', '0')) is True:
            business_category_set.add({'native_american_owned_business'})

        if strtobool(row.get('native_hawaiian_owned_busi', '0')) is True:
            business_category_set.add({'native_hawaiian_owned_business'})

        if strtobool(row.get('subcontinent_asian_asian_i', '0')) is True:
            business_category_set.add({'subcontinent_asian_indian_american_owned_business'})

        if strtobool(row.get('tribally_owned_business', '0')) is True:
            business_category_set.add({'tribally_owned_business'})

        if strtobool(row.get('other_minority_owned_busin', '0')) is True:
            business_category_set.add({'other_minority_owned_business'})

        if strtobool(row.get('minority_owned_business', '0')) is True or\
                (business_category_set & {'alaskan_native_owned_business', 'american_indian_owned_business',
                                          'asian_pacific_american_owned_business', 'black_american_owned_business',
                                          'hispanic_american_owned_business', 'native_american_owned_business',
                                          'native_hawaiian_owned_business',
                                          'subcontinent_asian_indian_american_owned_business',
                                          'tribally_owned_business',
                                          'other_minority_owned_business'}):
            business_category_set.add({'minority_owned_business'})

        # WOMEN OWNED BUSINESS
        if strtobool(row.get('women_owned_small_business', '0')) is True:
            business_category_set.add({'women_owned_small_business'})

        if strtobool(row.get('economically_disadvantaged', '0')) is True:
            business_category_set.add({'economically_disadvantaged_women_owned_small_business'})

        if strtobool(row.get('joint_venture_women_owned', '0')) is True:
            business_category_set.add({'joint_venture_women_owned_small_business'})

        if strtobool(row.get('joint_venture_economically', '0')) is True:
            business_category_set.add({'joint_venture_economically_disadvantaged_women_owned_small_business'})

        if strtobool(row.get('woman_owned_business', '0')) is True or \
                (business_category_set & {'women_owned_small_business',
                                          'economically_disadvantaged_women_owned_small_business',
                                          'joint_venture_women_owned_small_business',
                                          'joint_venture_economically_disadvantaged_women_owned_small_business'}):
            business_category_set.add({'woman_owned_business'})

        # VETERAN OWNED BUSINESS
        if strtobool(row.get('service_disabled_veteran_o', '0')) is True:
            business_category_set.add({'service_disabled_veteran_owned_business'})

        if strtobool(row.get('veteran_owned_business', '0')) is True or (
                    business_category_set & {'service_disabled_veteran_owned_business'}):
            business_category_set.add({'veteran_owned_business'})

        # SPECIAL DESIGNATIONS
        if strtobool(row.get('c8a_program_participant', '0')) is True:
            business_category_set.add({'8a_program_participant'})

        if strtobool(row.get('the_ability_one_program', '0')) is True:
            business_category_set.add({'ability_one_program'})

        if strtobool(row.get('dot_certified_disadvantage', '0')) is True:
            business_category_set.add({'dot_certified_disadvantaged_business_enterprise'})

        if strtobool(row.get('emerging_small_business', '0')) is True:
            business_category_set.add({'emerging_small_business'})

        if strtobool(row.get('federally_funded_research', '0')) is True:
            business_category_set.add({'federally_funded_research_and_development_corp'})

        if strtobool(row.get('historically_underutilized', '0')) is True:
            business_category_set.add({'historically_underutilized_business_firm'})

        if strtobool(row.get('labor_surplus_area_firm', '0')) is True:
            business_category_set.add({'labor_surplus_area_firm'})

        if strtobool(row.get('sba_certified_8_a_joint_ve', '0')) is True:
            business_category_set.add({'sba_certified_8a_joint_venture'})

        if strtobool(row.get('self_certified_small_disad', '0')) is True:
            business_category_set.add({'self_certified_small_disadvanted_business'})

        if strtobool(row.get('small_agricultural_coopera', '0')) is True:
            business_category_set.add({'small_agricultural_cooperative'})

        if strtobool(row.get('small_disadvantaged_busine', '0')) is True:
            business_category_set.add({'small_disadvantaged_business'})

        if strtobool(row.get('community_developed_corpor', '0')) is True:
            business_category_set.add({'community_developed_corporation_owned_firm'})

        # U.S. Owned Business
        if row.get('domestic_or_foreign_entity') == 'A':
            business_category_set.add({'us_owned_business'})

        # Foreign-Owned Business Incorporated in the U.S.
        if row.get('domestic_or_foreign_entity') == 'C':
            business_category_set.add({'foreign_owned_and_us_located_business'})

        # Foreign-Owned Business Not Incorporated in the U.S.
        if row.get('domestic_or_foreign_entity') == 'D' or strtobool(row.get('foreign_owned_and_located', '0')) is True:
            business_category_set.add({'foreign_owned_and_located_business'})

        if strtobool(row.get('foreign_government', '0')) is True:
            business_category_set.add({'foreign_government'})

        if strtobool(row.get('international_organization', '0')) is True:
            business_category_set.add({'international_organization'})

        if business_category_set & {'8a_program_participant', 'ability_one_program',
                                    'dot_certified_disadvantaged_business_enterprise', 'emerging_small_business',
                                    'federally_funded_research_and_development_corp',
                                    'historically_underutilized_business_firm', 'labor_surplus_area_firm',
                                    'sba_certified_8a_joint_venture', 'self_certified_small_disadvanted_business',
                                    'small_agricultural_cooperative', 'small_disadvantaged_business',
                                    'community_developed_corporation_owned_firm', 'us_owned_business',
                                    'foreign_owned_and_us_located_business', 'foreign_owned_and_located_business',
                                    'foreign_government', 'international_organization'}:
            business_category_set.add({'special_designations'})

        # NON-PROFIT
        if strtobool(row.get('foundation', '0')) is True:
            business_category_set.add({'foundation'})

        if strtobool(row.get('community_development_corp', '0')) is True:
            business_category_set.add({'community_development_corporations'})

        if strtobool(row.get('nonprofit_organization', '0')) is True or \
                        strtobool(row.get('other_not_for_profit_organ', '0')) is True or \
                (business_category_set & {'foundation', 'community_development_corporations'}):
            business_category_set.add({'nonprofit'})

        # HIGHER EDUCATION
        if strtobool(row.get('state_controlled_instituti', '0')) is True or \
                        strtobool(row.get('c1862_land_grant_college', '0')) is True or \
                        strtobool(row.get('c1890_land_grant_college', '0')) is True or \
                        strtobool(row.get('c1994_land_grant_college', '0')) is True:
            business_category_set.add({'public_institution_of_higher_education'})

        if strtobool(row.get('private_university_or_coll', '0')) is True:
            business_category_set.add({'private_institution_of_higher_education'})

        if strtobool(row.get('minority_institution', '0')) is True or \
                        strtobool(row.get('historically_black_college', '0')) is True or \
                        strtobool(row.get('tribal_college', '0')) is True or \
                        strtobool(row.get('alaskan_native_servicing_i', '0')) is True or \
                        strtobool(row.get('native_hawaiian_servicing', '0')) is True or \
                        strtobool(row.get('hispanic_servicing_institu', '0')) is True:
            business_category_set.add({'minority_serving_institution_of_higher_education'})

        if business_category_set & {'public_institution_of_higher_education', 'private_institution_of_higher_education',
                                    'minority_serving_institution_of_higher_education'}:
            business_category_set.add({'higher_education'})

        # GOVERNMENT
        if strtobool(row.get('us_federal_government', '0')) is True or \
                        strtobool(row.get('federal_agency', '0')) is True or \
                        strtobool(row.get('us_government_entity', '0')) is True or \
                        strtobool(row.get('interstate_entity', '0')) is True:
            business_category_set.add({'national_government'})

        if strtobool(row.get('us_state_government', '0')) is True or \
                        strtobool(row.get('council_of_governments', '0')) is True:
            business_category_set.add({'regional_and_state_government'})

        if strtobool(row.get('city_local_government', '0')) is True or \
                        strtobool(row.get('county_local_government', '0')) is True or \
                        strtobool(row.get('inter_municipal_local_gove', '0')) is True or \
                        strtobool(row.get('municipality_local_governm', '0')) is True or \
                        strtobool(row.get('township_local_government', '0')) is True or \
                        strtobool(row.get('us_local_government', '0')) is True or \
                        strtobool(row.get('local_government_owned', '0')) is True or \
                        strtobool(row.get('school_district_local_gove', '0')) is True:
            business_category_set.add({'local_government'})

        if strtobool(row.get('us_tribal_government', '0')) is True or \
                        strtobool(row.get('indian_tribe_federally_rec', '0')) is True:
            business_category_set.add({'indian_native_american_tribal_government'})

        if strtobool(row.get('housing_authorities_public', '0')) is True or \
                        strtobool(row.get('airport_authority', '0')) is True or \
                        strtobool(row.get('port_authority', '0')) is True or \
                        strtobool(row.get('transit_authority', '0')) is True or \
                        strtobool(row.get('planning_commission', '0')) is True:
            business_category_set.add({'authorities_and_commissions'})

        if business_category_set & {'national_government', 'regional_and_state_government',
                                    'us_territory_or_possession', 'local_government',
                                    'indian_native_american_tribal_government', 'authorities_and_commissions'}:
            business_category_set.add({'government'})

        return list(business_category_set)
    else:
        raise ValueError("Invalid object type provided to update_business_categories. "
                         "Must be one of the following types: TransactionFPDS, TransactionFABS")