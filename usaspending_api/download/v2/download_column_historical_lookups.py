"""
Sets up mappings from column names used in downloads to the
query paths used to get the data from django.

Not in use while we pull CSV data from the non-historical tables.
Until we switch to pulling CSV downloads from the historical
tables TransactionFPDS and TransactionFABS, import download_column_lookups.py
instad.
"""

"""
Code to generate these from spreadsheets:

tail -n +3 'usaspending_api/data/DAIMS_IDD_Resorted+DRW+KB+GGv7/D2-Award (Financial Assistance)-Table 1.csv' > d2_columns.csv


def find_column(col_name, model_classes):
    for (model_class, prefix) in model_classes:
        if hasattr(model_class, col_name):
            return '{}{}'.format(prefix, col_name)
    return None

query_paths = {'transaction': {'d1': {}, 'd2': {}}, 'award': {'d1': {}, 'd2': {}}}
human_names = {'transaction': {'d1': [], 'd2': []}, 'award': {'d1': [], 'd2': []}}

models_award_d1 = ((Award, ''), (TransactionNormalized, 'latest_transaction__'), (TransactionFPDS, 'latest_transaction__contract_data__'))
models_transaction_d1 = ((TransactionNormalized, ''), (TransactionFPDS, 'contract_data__'), (Award, 'award__'))
models_award_d2 = ((Award, ''), (TransactionNormalized, 'latest_transaction__'), (TransactionFABS, 'latest_transaction__assistance_data__'))
models_transaction_d2 = ((TransactionNormalized, ''), (TransactionFABS, 'assistance_data__'), (Award, 'award__'))

def set_if_found(dl_name, path, model, file):
    if path:
        if dl_name in ('fain', 'piid', 'uri'):
            dl_name = 'award_id_' + dl_name
        query_paths[model][file][dl_name] = path
        human_names[model][file].append(dl_name)
    else:
        print('Not found: {}: {}: {}'.format(model, file, dl_name))
                      
for row in d1:
    if len(row['Download Name']) <= 1:
        continue
    if len(row['Database Tag']) <= 1:
        continue
    if row['Award Level'] == 'Y':
        path = find_column(row['Database Tag'], models_award_d1)
        set_if_found(row['Download Name'], path, 'award', 'd1')
    if row['Transaction Level'] == 'Y':
        path = find_column(row['Database Tag'], models_transaction_d1)  
        set_if_found(row['Download Name'], path, 'transaction', 'd1')

# no database tags supplied for d2  
for row in d2:
    if len(row['Download Name']) <= 1:
        continue
    if row['Award Level?'] == 'Y':
        path = find_column(row['Download Name'], models_award_d2)
        if not path:
            # try what it was for D1
            path = query_paths['award']['d1'].get(row['Download Name'])
            if path:
                col_name = path.split('__')[-1]
                path = find_column(col_name, models_award_d2)
        set_if_found(row['Download Name'], path, 'award', 'd2')
    if row['Transaction Level?'] == 'Y':
        path = find_column(row['Download Name'], models_transaction_d2)
        if not path:
            # try what it was for D1
            path = query_paths['transaction']['d1'].get(row['Download Name'])
            if path:
                col_name = path.split('__')[-1]
                path = find_column(col_name, models_transaction_d2)        
        set_if_found(row['Download Name'], path, 'transaction', 'd2')

                


"""

query_paths = {
    'award': {
        'd1': {
            '1862_land_grant_college':
            'latest_transaction__contract_data__c1862_land_grant_college',
            '1890_land_grant_college':
            'latest_transaction__contract_data__c1890_land_grant_college',
            '1994_land_grant_college':
            'latest_transaction__contract_data__c1994_land_grant_college',
            'a76_fair_act_action_code':
            'latest_transaction__contract_data__a_76_fair_act_action',
            'airport_authority':
            'latest_transaction__contract_data__airport_authority',
            'alaskan_native_owned_corporation_or_firm':
            'latest_transaction__contract_data__alaskan_native_owned_corpo',
            'alaskan_native_servicing_institution':
            'latest_transaction__contract_data__alaskan_native_servicing_i',
            'american_indian_owned_business':
            'latest_transaction__contract_data__american_indian_owned_busi',
            'asian_pacific_american_owned_business':
            'latest_transaction__contract_data__asian_pacific_american_own',
            'award_description':
            'latest_transaction__contract_data__award_description',
            'award_id_piid': 'piid',
            'award_type_code':
            'latest_transaction__contract_data__contract_award_type',
            'awarding_agency_code':
            'latest_transaction__contract_data__awarding_agency_code',
            'awarding_agency_name':
            'latest_transaction__contract_data__awarding_agency_name',
            'awarding_office_code':
            'latest_transaction__contract_data__awarding_office_code',
            'awarding_office_name':
            'latest_transaction__contract_data__awarding_office_name',
            'awarding_sub_agency_code':
            'latest_transaction__contract_data__awarding_sub_tier_agency_c',
            'awarding_sub_agency_name':
            'latest_transaction__contract_data__awarding_sub_tier_agency_n',
            'black_american_owned_business':
            'latest_transaction__contract_data__black_american_owned_busin',
            'c8a_program_participant':
            'latest_transaction__contract_data__c8a_program_participant',
            'city_local_government':
            'latest_transaction__contract_data__city_local_government',
            'clinger_cohen_act_planning_code':
            'latest_transaction__contract_data__clinger_cohen_act_planning',
            'commercial_item_acquisition_procedures_code':
            'latest_transaction__contract_data__commercial_item_acquisitio',
            'commercial_item_test_program_code':
            'latest_transaction__contract_data__commercial_item_test_progr',
            'community_developed_corporation_owned_firm':
            'latest_transaction__contract_data__community_developed_corpor',
            'community_development_corporation':
            'latest_transaction__contract_data__community_development_corp',
            'consolidated_contract_code':
            'latest_transaction__contract_data__consolidated_contract',
            'contingency_humanitarian_or_peacekeeping_operation_code':
            'latest_transaction__contract_data__contingency_humanitarian_o',
            'contract_bundling_code':
            'latest_transaction__contract_data__contract_bundling',
            'contract_financing_code':
            'latest_transaction__contract_data__contract_financing',
            'contracting_officers_determination_of_business_size':
            'latest_transaction__contract_data__contracting_officers_deter',
            'corporate_entity_not_tax_exempt':
            'latest_transaction__contract_data__corporate_entity_not_tax_e',
            'corporate_entity_tax_exempt':
            'latest_transaction__contract_data__corporate_entity_tax_exemp',
            'cost_accounting_standards_clause_code':
            'latest_transaction__contract_data__cost_accounting_standards',
            'cost_or_pricing_data_code':
            'latest_transaction__contract_data__cost_or_pricing_data',
            'council_of_governments':
            'latest_transaction__contract_data__council_of_governments',
            'country_of_product_or_service_origin_code':
            'latest_transaction__contract_data__country_of_product_or_serv',
            'county_local_government':
            'latest_transaction__contract_data__county_local_government',
            'davis_bacon_act_code':
            'latest_transaction__contract_data__davis_bacon_act',
            'dod_acquisition_program_code':
            'latest_transaction__contract_data__program_system_or_equipmen',
            'dod_claimant_program_code':
            'latest_transaction__contract_data__dod_claimant_program_code',
            'domestic_or_foreign_entity_code':
            'latest_transaction__contract_data__domestic_or_foreign_entity',
            'domestic_shelter':
            'latest_transaction__contract_data__domestic_shelter',
            'dot_certified_disadvantage':
            'latest_transaction__contract_data__dot_certified_disadvantage',
            'economically_disadvantaged_women_owned_small_business':
            'latest_transaction__contract_data__economically_disadvantaged',
            'educational_institution':
            'latest_transaction__contract_data__educational_institution',
            'emerging_small_business':
            'latest_transaction__contract_data__emerging_small_business',
            'epa_designated_product_code':
            'latest_transaction__contract_data__epa_designated_product',
            'evaluated_preference_code':
            'latest_transaction__contract_data__evaluated_preference',
            'extent_competed_code':
            'latest_transaction__contract_data__extent_competed',
            'fair_opportunity_limited_sources_code':
            'latest_transaction__contract_data__fair_opportunity_limited_s',
            'fed_biz_opps_code':
            'latest_transaction__contract_data__fed_biz_opps',
            'federal_agency':
            'latest_transaction__contract_data__federal_agency',
            'federally_funded_research_and_development_corp':
            'latest_transaction__contract_data__federally_funded_research',
            'for_profit_organization':
            'latest_transaction__contract_data__for_profit_organization',
            'foreign_government':
            'latest_transaction__contract_data__foreign_government',
            'foreign_owned_and_located':
            'latest_transaction__contract_data__foreign_owned_and_located',
            'foundation': 'latest_transaction__contract_data__foundation',
            'funding_agency_code':
            'latest_transaction__contract_data__funding_agency_code',
            'funding_agency_name':
            'latest_transaction__contract_data__funding_agency_name',
            'funding_office_code':
            'latest_transaction__contract_data__funding_office_code',
            'funding_office_name':
            'latest_transaction__contract_data__funding_office_name',
            'funding_sub_agency_code':
            'latest_transaction__contract_data__funding_sub_tier_agency_co',
            'funding_sub_agency_name':
            'latest_transaction__contract_data__funding_sub_tier_agency_na',
            'gfe_gfp_code':
            'latest_transaction__contract_data__government_furnished_equip',
            'hispanic_american_owned_business':
            'latest_transaction__contract_data__hispanic_american_owned_bu',
            'hispanic_servicing_institution':
            'latest_transaction__contract_data__hispanic_servicing_institu',
            'historically_black_college':
            'latest_transaction__contract_data__historically_black_college',
            'historically_underutilized_business_zone _hubzone_firm':
            'latest_transaction__contract_data__historically_underutilized',
            'hospital_flag': 'latest_transaction__contract_data__hospital_flag',
            'housing_authorities_public_tribal':
            'latest_transaction__contract_data__housing_authorities_public',
            'idv_type_code': 'latest_transaction__contract_data__idv_type',
            'indian_tribe_federally_recognized':
            'latest_transaction__contract_data__indian_tribe_federally_rec',
            'information_technology_commercial_item_category_code':
            'latest_transaction__contract_data__information_technology_com',
            'inter_municipal_local_government':
            'latest_transaction__contract_data__inter_municipal_local_gove',
            'interagency_contracting_authority_code':
            'latest_transaction__contract_data__interagency_contracting_au',
            'international_organization':
            'latest_transaction__contract_data__international_organization',
            'interstate_entity':
            'latest_transaction__contract_data__interstate_entity',
            'joint_venture_economic_disadvantaged_women_owned_small_bus':
            'latest_transaction__contract_data__joint_venture_economically',
            'joint_venture_women_owned_small_business':
            'latest_transaction__contract_data__joint_venture_women_owned',
            'labor_surplus_area_firm':
            'latest_transaction__contract_data__labor_surplus_area_firm',
            'limited_liability_corporation':
            'latest_transaction__contract_data__limited_liability_corporat',
            'local_area_set_aside_code':
            'latest_transaction__contract_data__local_area_set_aside',
            'local_government_owned':
            'latest_transaction__contract_data__local_government_owned',
            'major_program': 'latest_transaction__contract_data__major_program',
            'manufacturer_of_goods':
            'latest_transaction__contract_data__manufacturer_of_goods',
            'minority_institution':
            'latest_transaction__contract_data__minority_institution',
            'minority_owned_business':
            'latest_transaction__contract_data__minority_owned_business',
            'multi_year_contract_code':
            'latest_transaction__contract_data__multi_year_contract',
            'multiple_or_single_award_idv_code':
            'latest_transaction__contract_data__multiple_or_single_award_i',
            'municipality_local_government':
            'latest_transaction__contract_data__municipality_local_governm',
            'naics_code': 'latest_transaction__contract_data__naics',
            'naics_description':
            'latest_transaction__contract_data__naics_description',
            'national_interest_action_code':
            'latest_transaction__contract_data__national_interest_action',
            'native_american_owned_business':
            'latest_transaction__contract_data__native_american_owned_busi',
            'native_hawaiian_owned_business':
            'latest_transaction__contract_data__native_hawaiian_owned_busi',
            'native_hawaiian_servicing_institution':
            'latest_transaction__contract_data__native_hawaiian_servicing',
            'nonprofit_organization':
            'latest_transaction__contract_data__nonprofit_organization',
            'number_of_actions':
            'latest_transaction__contract_data__number_of_actions',
            'number_of_offers_received':
            'latest_transaction__contract_data__number_of_offers_received',
            'ordering_period_end_date':
            'latest_transaction__contract_data__ordering_period_end_date',
            'other_minority_owned_business':
            'latest_transaction__contract_data__other_minority_owned_busin',
            'other_not_for_profit_organization':
            'latest_transaction__contract_data__other_not_for_profit_organ',
            'other_statutory_authority':
            'latest_transaction__contract_data__other_statutory_authority',
            'other_than_full_and_open_competition_code':
            'latest_transaction__contract_data__other_than_full_and_open_c',
            'parent_award_agency_id':
            'latest_transaction__contract_data__referenced_idv_agency_iden',
            'parent_award_id': 'parent_award_id',
            'parent_award_single_or_multiple_code':
            'latest_transaction__contract_data__referenced_mult_or_single',
            'parent_award_type_code':
            'latest_transaction__contract_data__referenced_idv_type',
            'partnership_or_limited_liability_partnership':
            'latest_transaction__contract_data__partnership_or_limited_lia',
            'performance_based_service_acquisition_code':
            'latest_transaction__contract_data__performance_based_service',
            'period_of_performance_current_end_date':
            'latest_transaction__contract_data__period_of_performance_curr',
            'period_of_performance_potential_end_date':
            'latest_transaction__contract_data__period_of_perf_potential_e',
            'period_of_performance_start_date':
            'latest_transaction__contract_data__period_of_performance_star',
            'place_of_manufacture_code':
            'latest_transaction__contract_data__place_of_manufacture',
            'planning_commission':
            'latest_transaction__contract_data__planning_commission',
            'port_authority':
            'latest_transaction__contract_data__port_authority',
            'primary_place_of_performance_city_name':
            'latest_transaction__contract_data__place_of_perform_city_name',
            'primary_place_of_performance_congressional_district':
            'latest_transaction__contract_data__place_of_performance_congr',
            'primary_place_of_performance_country_code':
            'latest_transaction__contract_data__place_of_perform_country_c',
            'primary_place_of_performance_location_code':
            'latest_transaction__contract_data__place_of_performance_locat',
            'primary_place_of_performance_state_code':
            'latest_transaction__contract_data__place_of_performance_state',
            'primary_place_of_performance_zip_4':
            'latest_transaction__contract_data__place_of_performance_zip4a',
            'private_university_or_college':
            'latest_transaction__contract_data__private_university_or_coll',
            'product_or_service_code':
            'latest_transaction__contract_data__product_or_service_code',
            'program_acronym':
            'latest_transaction__contract_data__program_acronym',
            'purchase_card_as_payment_method_code':
            'latest_transaction__contract_data__purchase_card_as_payment_m',
            'receives_contracts':
            'latest_transaction__contract_data__contracts',
            'receives_contracts_and_grants':
            'latest_transaction__contract_data__receives_contracts_and_gra',
            'receives_grants': 'latest_transaction__contract_data__grants',
            'recipient_address_line_1':
            'latest_transaction__contract_data__legal_entity_address_line1',
            'recipient_address_line_2':
            'latest_transaction__contract_data__legal_entity_address_line2',
            'recipient_address_line_3':
            'latest_transaction__contract_data__legal_entity_address_line3',
            'recipient_city_name':
            'latest_transaction__contract_data__legal_entity_city_name',
            'recipient_congressional_district':
            'latest_transaction__contract_data__legal_entity_congressional',
            'recipient_country_code':
            'latest_transaction__contract_data__legal_entity_country_code',
            'recipient_country_name':
            'latest_transaction__contract_data__legal_entity_country_name',
            'recipient_doing_business_as_name':
            'latest_transaction__contract_data__vendor_doing_as_business_n',
            'recipient_duns':
            'latest_transaction__contract_data__awardee_or_recipient_uniqu',
            'recipient_fax_number':
            'latest_transaction__contract_data__vendor_fax_number',
            'recipient_foreign_state_name':
            'latest_transaction__contract_data__legal_entity_state_descrip',
            'recipient_name':
            'latest_transaction__contract_data__awardee_or_recipient_legal',
            'recipient_parent_duns':
            'latest_transaction__contract_data__ultimate_parent_legal_enti',
            'recipient_parent_name':
            'latest_transaction__contract_data__ultimate_parent_unique_ide',
            'recipient_phone_number':
            'latest_transaction__contract_data__vendor_phone_number',
            'recipient_state_code':
            'latest_transaction__contract_data__legal_entity_state_code',
            'recipient_zip_4_code':
            'latest_transaction__contract_data__legal_entity_zip4',
            'recovered_materials_sustainability_code':
            'latest_transaction__contract_data__recovered_materials_sustai',
            'research_code': 'latest_transaction__contract_data__research',
            'sba_certified_8a_joint_venture':
            'latest_transaction__contract_data__sba_certified_8_a_joint_ve',
            'school_district_local_government':
            'latest_transaction__contract_data__school_district_local_gove',
            'school_of_forestry':
            'latest_transaction__contract_data__school_of_forestry',
            'sea_transportation_code':
            'latest_transaction__contract_data__sea_transportation',
            'self_certified_small_disadvantaged_business':
            'latest_transaction__contract_data__self_certified_small_disad',
            'service_contract_act_code':
            'latest_transaction__contract_data__service_contract_act',
            'service_disabled_veteran_owned_business':
            'latest_transaction__contract_data__service_disabled_veteran_o',
            'small_agricultural_cooperative':
            'latest_transaction__contract_data__small_agricultural_coopera',
            'small_business_competitiveness_demonstration _program':
            'latest_transaction__contract_data__small_business_competitive',
            'small_disadvantaged_business':
            'latest_transaction__contract_data__small_disadvantaged_busine',
            'sole_proprietorship':
            'latest_transaction__contract_data__sole_proprietorship',
            'solicitation_identifier':
            'latest_transaction__contract_data__solicitation_identifier',
            'solicitation_procedures_code':
            'latest_transaction__contract_data__solicitation_procedures',
            'state_controlled_institution_of_higher_learning':
            'latest_transaction__contract_data__state_controlled_instituti',
            'subchapter_scorporation':
            'latest_transaction__contract_data__subchapter_s_corporation',
            'subcontinent_asian_asian_indian_american_owned_business':
            'latest_transaction__contract_data__subcontinent_asian_asian_i',
            'subcontracting_plan_code':
            'latest_transaction__contract_data__subcontracting_plan',
            'the_ability_one_program':
            'latest_transaction__contract_data__the_ability_one_program',
            'township_local_government':
            'latest_transaction__contract_data__township_local_government',
            'transit_authority':
            'latest_transaction__contract_data__transit_authority',
            'tribal_college':
            'latest_transaction__contract_data__tribal_college',
            'tribally_owned_business':
            'latest_transaction__contract_data__tribally_owned_business',
            'type_of_contract_pricing_code':
            'latest_transaction__contract_data__type_of_contract_pricing',
            'type_of_idc_code':
            'latest_transaction__contract_data__type_of_idc',
            'type_of_set_aside_code':
            'latest_transaction__contract_data__type_set_aside',
            'us_federal_government':
            'latest_transaction__contract_data__us_federal_government',
            'us_government_entity':
            'latest_transaction__contract_data__us_government_entity',
            'us_local_government':
            'latest_transaction__contract_data__us_local_government',
            'us_state_government':
            'latest_transaction__contract_data__us_state_government',
            'us_tribal_government':
            'latest_transaction__contract_data__us_tribal_government',
            'veteran_owned_business':
            'latest_transaction__contract_data__veteran_owned_business',
            'veterinary_college':
            'latest_transaction__contract_data__veterinary_college',
            'veterinary_hospital':
            'latest_transaction__contract_data__veterinary_hospital',
            'walsh_healey_act_code':
            'latest_transaction__contract_data__walsh_healey_act',
            'woman_owned_business':
            'latest_transaction__contract_data__woman_owned_business',
            'women_owned_small_business':
            'latest_transaction__contract_data__women_owned_small_business'
        },
        'd2': {
            'assistance_type':
            'latest_transaction__assistance_data__assistance_type',
            'award_description':
            'latest_transaction__assistance_data__award_description',
            'award_id_fain': 'fain',
            'award_id_uri': 'uri',
            'awarding_agency_code':
            'latest_transaction__assistance_data__awarding_agency_code',
            'awarding_agency_name':
            'latest_transaction__assistance_data__awarding_agency_name',
            'awarding_office_code':
            'latest_transaction__assistance_data__awarding_office_code',
            'awarding_sub_agency_code':
            'latest_transaction__assistance_data__awarding_sub_tier_agency_c',
            'awarding_sub_agency_name':
            'latest_transaction__assistance_data__awarding_sub_tier_agency_n',
            'business_funds_indicator':
            'latest_transaction__assistance_data__business_funds_indicator',
            'cfda_number': 'latest_transaction__assistance_data__cfda_number',
            'cfda_title': 'latest_transaction__assistance_data__cfda_title',
            'federal_action_obligation':
            'latest_transaction__federal_action_obligation',
            'funding_agency_code':
            'latest_transaction__assistance_data__funding_agency_code',
            'funding_agency_name':
            'latest_transaction__assistance_data__funding_agency_name',
            'funding_office_code':
            'latest_transaction__assistance_data__funding_office_code',
            'funding_sub_agency_code':
            'latest_transaction__assistance_data__funding_sub_tier_agency_co',
            'funding_sub_agency_name':
            'latest_transaction__assistance_data__funding_sub_tier_agency_na',
            'last_modified_date': 'last_modified_date',
            'non_federal_funding_amount':
            'latest_transaction__assistance_data__non_federal_funding_amount',
            'period_of_performance_current_end_date':
            'period_of_performance_current_end_date',
            'period_of_performance_start_date':
            'period_of_performance_start_date',
            'primary_place_of_performance_congressional_district':
            'latest_transaction__assistance_data__place_of_performance_congr',
            'primary_place_of_performance_country_code':
            'latest_transaction__assistance_data__place_of_perform_country_c',
            'primary_place_of_performance_zip_4':
            'latest_transaction__assistance_data__place_of_performance_zip4a',
            'recipient_address_line_1':
            'latest_transaction__assistance_data__legal_entity_address_line1',
            'recipient_address_line_2':
            'latest_transaction__assistance_data__legal_entity_address_line2',
            'recipient_address_line_3':
            'latest_transaction__assistance_data__legal_entity_address_line3',
            'recipient_city_name':
            'latest_transaction__assistance_data__legal_entity_city_name',
            'recipient_congressional_district':
            'latest_transaction__assistance_data__legal_entity_congressional',
            'recipient_country_code':
            'latest_transaction__assistance_data__legal_entity_country_code',
            'recipient_duns':
            'latest_transaction__assistance_data__awardee_or_recipient_uniqu',
            'recipient_name':
            'latest_transaction__assistance_data__awardee_or_recipient_legal',
            'recipient_state_code':
            'latest_transaction__assistance_data__legal_entity_state_code',
            'record_type': 'latest_transaction__assistance_data__record_type',
            'sai_number': 'latest_transaction__assistance_data__sai_number',
            'total_funding_amount':
            'latest_transaction__assistance_data__total_funding_amount'
        }
    },
    'transaction': {
        'd1': {
            '1862_land_grant_college':
            'contract_data__c1862_land_grant_college',
            '1890_land_grant_college':
            'contract_data__c1890_land_grant_college',
            '1994_land_grant_college':
            'contract_data__c1994_land_grant_college',
            'a76_fair_act_action_code': 'contract_data__a_76_fair_act_action',
            'action_date': 'action_date',
            'action_type_code': 'action_type',
            'airport_authority': 'contract_data__airport_authority',
            'alaskan_native_owned_corporation_or_firm':
            'contract_data__alaskan_native_owned_corpo',
            'alaskan_native_servicing_institution':
            'contract_data__alaskan_native_servicing_i',
            'american_indian_owned_business':
            'contract_data__american_indian_owned_busi',
            'asian_pacific_american_owned_business':
            'contract_data__asian_pacific_american_own',
            'award_description': 'contract_data__award_description',
            'award_id_piid': 'contract_data__piid',
            'award_type_code': 'contract_data__contract_award_type',
            'awarding_agency_code': 'contract_data__awarding_agency_code',
            'awarding_agency_name': 'contract_data__awarding_agency_name',
            'awarding_office_code': 'contract_data__awarding_office_code',
            'awarding_office_name': 'contract_data__awarding_office_name',
            'awarding_sub_agency_code':
            'contract_data__awarding_sub_tier_agency_c',
            'awarding_sub_agency_name':
            'contract_data__awarding_sub_tier_agency_n',
            'black_american_owned_business':
            'contract_data__black_american_owned_busin',
            'c8a_program_participant':
            'contract_data__c8a_program_participant',
            'city_local_government': 'contract_data__city_local_government',
            'clinger_cohen_act_planning_code':
            'contract_data__clinger_cohen_act_planning',
            'commercial_item_acquisition_procedures_code':
            'contract_data__commercial_item_acquisitio',
            'commercial_item_test_program_code':
            'contract_data__commercial_item_test_progr',
            'community_developed_corporation_owned_firm':
            'contract_data__community_developed_corpor',
            'community_development_corporation':
            'contract_data__community_development_corp',
            'consolidated_contract_code':
            'contract_data__consolidated_contract',
            'contingency_humanitarian_or_peacekeeping_operation_code':
            'contract_data__contingency_humanitarian_o',
            'contract_bundling_code': 'contract_data__contract_bundling',
            'contract_financing_code': 'contract_data__contract_financing',
            'contracting_officers_determination_of_business_size':
            'contract_data__contracting_officers_deter',
            'corporate_entity_not_tax_exempt':
            'contract_data__corporate_entity_not_tax_e',
            'corporate_entity_tax_exempt':
            'contract_data__corporate_entity_tax_exemp',
            'cost_accounting_standards_clause_code':
            'contract_data__cost_accounting_standards',
            'cost_or_pricing_data_code': 'contract_data__cost_or_pricing_data',
            'council_of_governments': 'contract_data__council_of_governments',
            'country_of_product_or_service_origin_code':
            'contract_data__country_of_product_or_serv',
            'county_local_government':
            'contract_data__county_local_government',
            'davis_bacon_act_code': 'contract_data__davis_bacon_act',
            'dod_acquisition_program_code':
            'contract_data__program_system_or_equipmen',
            'dod_claimant_program_code':
            'contract_data__dod_claimant_program_code',
            'domestic_or_foreign_entity_code':
            'contract_data__domestic_or_foreign_entity',
            'domestic_shelter': 'contract_data__domestic_shelter',
            'dot_certified_disadvantage':
            'contract_data__dot_certified_disadvantage',
            'economically_disadvantaged_women_owned_small_business':
            'contract_data__economically_disadvantaged',
            'educational_institution':
            'contract_data__educational_institution',
            'emerging_small_business':
            'contract_data__emerging_small_business',
            'epa_designated_product_code':
            'contract_data__epa_designated_product',
            'evaluated_preference_code': 'contract_data__evaluated_preference',
            'extent_competed_code': 'contract_data__extent_competed',
            'fair_opportunity_limited_sources_code':
            'contract_data__fair_opportunity_limited_s',
            'fed_biz_opps_code': 'contract_data__fed_biz_opps',
            'federal_action_obligation': 'federal_action_obligation',
            'federal_agency': 'contract_data__federal_agency',
            'federally_funded_research_and_development_corp':
            'contract_data__federally_funded_research',
            'for_profit_organization':
            'contract_data__for_profit_organization',
            'foreign_government': 'contract_data__foreign_government',
            'foreign_owned_and_located':
            'contract_data__foreign_owned_and_located',
            'foundation': 'contract_data__foundation',
            'funding_agency_code': 'contract_data__funding_agency_code',
            'funding_agency_name': 'contract_data__funding_agency_name',
            'funding_office_code': 'contract_data__funding_office_code',
            'funding_office_name': 'contract_data__funding_office_name',
            'funding_sub_agency_code':
            'contract_data__funding_sub_tier_agency_co',
            'funding_sub_agency_name':
            'contract_data__funding_sub_tier_agency_na',
            'gfe_gfp_code': 'contract_data__government_furnished_equip',
            'hispanic_american_owned_business':
            'contract_data__hispanic_american_owned_bu',
            'hispanic_servicing_institution':
            'contract_data__hispanic_servicing_institu',
            'historically_black_college':
            'contract_data__historically_black_college',
            'historically_underutilized_business_zone _hubzone_firm':
            'contract_data__historically_underutilized',
            'hospital_flag': 'contract_data__hospital_flag',
            'housing_authorities_public_tribal':
            'contract_data__housing_authorities_public',
            'idv_type_code': 'contract_data__idv_type',
            'indian_tribe_federally_recognized':
            'contract_data__indian_tribe_federally_rec',
            'information_technology_commercial_item_category_code':
            'contract_data__information_technology_com',
            'inter_municipal_local_government':
            'contract_data__inter_municipal_local_gove',
            'interagency_contracting_authority_code':
            'contract_data__interagency_contracting_au',
            'international_organization':
            'contract_data__international_organization',
            'interstate_entity': 'contract_data__interstate_entity',
            'joint_venture_economic_disadvantaged_women_owned_small_bus':
            'contract_data__joint_venture_economically',
            'joint_venture_women_owned_small_business':
            'contract_data__joint_venture_women_owned',
            'labor_surplus_area_firm':
            'contract_data__labor_surplus_area_firm',
            'limited_liability_corporation':
            'contract_data__limited_liability_corporat',
            'local_area_set_aside_code': 'contract_data__local_area_set_aside',
            'local_government_owned': 'contract_data__local_government_owned',
            'major_program': 'contract_data__major_program',
            'manufacturer_of_goods': 'contract_data__manufacturer_of_goods',
            'minority_institution': 'contract_data__minority_institution',
            'minority_owned_business':
            'contract_data__minority_owned_business',
            'modification_number': 'contract_data__award_modification_amendme',
            'multi_year_contract_code': 'contract_data__multi_year_contract',
            'multiple_or_single_award_idv_code':
            'contract_data__multiple_or_single_award_i',
            'municipality_local_government':
            'contract_data__municipality_local_governm',
            'naics_code': 'contract_data__naics',
            'naics_description': 'contract_data__naics_description',
            'national_interest_action_code':
            'contract_data__national_interest_action',
            'native_american_owned_business':
            'contract_data__native_american_owned_busi',
            'native_hawaiian_owned_business':
            'contract_data__native_hawaiian_owned_busi',
            'native_hawaiian_servicing_institution':
            'contract_data__native_hawaiian_servicing',
            'nonprofit_organization': 'contract_data__nonprofit_organization',
            'number_of_actions': 'contract_data__number_of_actions',
            'number_of_offers_received':
            'contract_data__number_of_offers_received',
            'ordering_period_end_date':
            'contract_data__ordering_period_end_date',
            'other_minority_owned_business':
            'contract_data__other_minority_owned_busin',
            'other_not_for_profit_organization':
            'contract_data__other_not_for_profit_organ',
            'other_statutory_authority':
            'contract_data__other_statutory_authority',
            'other_than_full_and_open_competition_code':
            'contract_data__other_than_full_and_open_c',
            'parent_award_agency_id':
            'contract_data__referenced_idv_agency_iden',
            'parent_award_id': 'contract_data__parent_award_id',
            'parent_award_modification_number':
            'contract_data__referenced_idv_modificatio',
            'parent_award_single_or_multiple_code':
            'contract_data__referenced_mult_or_single',
            'parent_award_type_code': 'contract_data__referenced_idv_type',
            'partnership_or_limited_liability_partnership':
            'contract_data__partnership_or_limited_lia',
            'performance_based_service_acquisition_code':
            'contract_data__performance_based_service',
            'period_of_performance_current_end_date':
            'contract_data__period_of_performance_curr',
            'period_of_performance_potential_end_date':
            'contract_data__period_of_perf_potential_e',
            'period_of_performance_start_date':
            'contract_data__period_of_performance_star',
            'place_of_manufacture_code': 'contract_data__place_of_manufacture',
            'planning_commission': 'contract_data__planning_commission',
            'port_authority': 'contract_data__port_authority',
            'price_evaluation_adjustment_preference_percent_difference':
            'contract_data__price_evaluation_adjustmen',
            'primary_place_of_performance_city_name':
            'contract_data__place_of_perform_city_name',
            'primary_place_of_performance_congressional_district':
            'contract_data__place_of_performance_congr',
            'primary_place_of_performance_country_code':
            'contract_data__place_of_perform_country_c',
            'primary_place_of_performance_location_code':
            'contract_data__place_of_performance_locat',
            'primary_place_of_performance_state_code':
            'contract_data__place_of_performance_state',
            'primary_place_of_performance_zip_4':
            'contract_data__place_of_performance_zip4a',
            'private_university_or_college':
            'contract_data__private_university_or_coll',
            'product_or_service_code':
            'contract_data__product_or_service_code',
            'program_acronym': 'contract_data__program_acronym',
            'purchase_card_as_payment_method_code':
            'contract_data__purchase_card_as_payment_m',
            'receives_contracts': 'contract_data__contracts',
            'receives_contracts_and_grants':
            'contract_data__receives_contracts_and_gra',
            'receives_grants': 'contract_data__grants',
            'recipient_address_line_1':
            'contract_data__legal_entity_address_line1',
            'recipient_address_line_2':
            'contract_data__legal_entity_address_line2',
            'recipient_address_line_3':
            'contract_data__legal_entity_address_line3',
            'recipient_city_name': 'contract_data__legal_entity_city_name',
            'recipient_congressional_district':
            'contract_data__legal_entity_congressional',
            'recipient_country_code':
            'contract_data__legal_entity_country_code',
            'recipient_country_name':
            'contract_data__legal_entity_country_name',
            'recipient_doing_business_as_name':
            'contract_data__vendor_doing_as_business_n',
            'recipient_duns': 'contract_data__awardee_or_recipient_uniqu',
            'recipient_fax_number': 'contract_data__vendor_fax_number',
            'recipient_foreign_state_name':
            'contract_data__legal_entity_state_descrip',
            'recipient_name': 'contract_data__awardee_or_recipient_legal',
            'recipient_parent_duns':
            'contract_data__ultimate_parent_legal_enti',
            'recipient_parent_name':
            'contract_data__ultimate_parent_unique_ide',
            'recipient_phone_number': 'contract_data__vendor_phone_number',
            'recipient_state_code': 'contract_data__legal_entity_state_code',
            'recipient_zip_4_code': 'contract_data__legal_entity_zip4',
            'recovered_materials_sustainability_code':
            'contract_data__recovered_materials_sustai',
            'research_code': 'contract_data__research',
            'sba_certified_8a_joint_venture':
            'contract_data__sba_certified_8_a_joint_ve',
            'school_district_local_government':
            'contract_data__school_district_local_gove',
            'school_of_forestry': 'contract_data__school_of_forestry',
            'sea_transportation_code': 'contract_data__sea_transportation',
            'self_certified_small_disadvantaged_business':
            'contract_data__self_certified_small_disad',
            'service_contract_act_code': 'contract_data__service_contract_act',
            'service_disabled_veteran_owned_business':
            'contract_data__service_disabled_veteran_o',
            'small_agricultural_cooperative':
            'contract_data__small_agricultural_coopera',
            'small_business_competitiveness_demonstration _program':
            'contract_data__small_business_competitive',
            'small_disadvantaged_business':
            'contract_data__small_disadvantaged_busine',
            'sole_proprietorship': 'contract_data__sole_proprietorship',
            'solicitation_identifier': 'contract_data__solicitation_identifier',
            'solicitation_procedures_code':
            'contract_data__solicitation_procedures',
            'state_controlled_institution_of_higher_learning':
            'contract_data__state_controlled_instituti',
            'subchapter_scorporation':
            'contract_data__subchapter_s_corporation',
            'subcontinent_asian_asian_indian_american_owned_business':
            'contract_data__subcontinent_asian_asian_i',
            'subcontracting_plan_code': 'contract_data__subcontracting_plan',
            'the_ability_one_program': 'contract_data__the_ability_one_program',
            'township_local_government':
            'contract_data__township_local_government',
            'transaction_number': 'contract_data__transaction_number',
            'transit_authority': 'contract_data__transit_authority',
            'tribal_college': 'contract_data__tribal_college',
            'tribally_owned_business': 'contract_data__tribally_owned_business',
            'type_of_contract_pricing_code':
            'contract_data__type_of_contract_pricing',
            'type_of_idc_code': 'contract_data__type_of_idc',
            'type_of_set_aside_code': 'contract_data__type_set_aside',
            'undefinitized_action_code': 'contract_data__undefinitized_action',
            'us_federal_government': 'contract_data__us_federal_government',
            'us_government_entity': 'contract_data__us_government_entity',
            'us_local_government': 'contract_data__us_local_government',
            'us_state_government': 'contract_data__us_state_government',
            'us_tribal_government': 'contract_data__us_tribal_government',
            'veteran_owned_business': 'contract_data__veteran_owned_business',
            'veterinary_college': 'contract_data__veterinary_college',
            'veterinary_hospital': 'contract_data__veterinary_hospital',
            'walsh_healey_act_code': 'contract_data__walsh_healey_act',
            'woman_owned_business': 'contract_data__woman_owned_business',
            'women_owned_small_business':
            'contract_data__women_owned_small_business'
        },
        'd2': {
            'action_date': 'action_date',
            'action_type': 'action_type',
            'action_type_code': 'action_type',
            'assistance_type': 'assistance_data__assistance_type',
            'award_description': 'assistance_data__award_description',
            'award_id_fain': 'assistance_data__fain',
            'award_id_uri': 'assistance_data__uri',
            'awarding_agency_code': 'assistance_data__awarding_agency_code',
            'awarding_agency_name': 'assistance_data__awarding_agency_name',
            'awarding_office_code': 'assistance_data__awarding_office_code',
            'awarding_sub_agency_code':
            'assistance_data__awarding_sub_tier_agency_c',
            'awarding_sub_agency_name':
            'assistance_data__awarding_sub_tier_agency_n',
            'business_funds_indicator':
            'assistance_data__business_funds_indicator',
            'cfda_number': 'assistance_data__cfda_number',
            'cfda_title': 'assistance_data__cfda_title',
            'federal_action_obligation': 'federal_action_obligation',
            'funding_agency_code': 'assistance_data__funding_agency_code',
            'funding_agency_name': 'assistance_data__funding_agency_name',
            'funding_office_code': 'assistance_data__funding_office_code',
            'funding_sub_agency_code':
            'assistance_data__funding_sub_tier_agency_co',
            'funding_sub_agency_name':
            'assistance_data__funding_sub_tier_agency_na',
            'last_modified_date': 'last_modified_date',
            'modification_number': 'modification_number',
            'non_federal_funding_amount':
            'assistance_data__non_federal_funding_amount',
            'period_of_performance_current_end_date':
            'period_of_performance_current_end_date',
            'period_of_performance_start_date':
            'period_of_performance_start_date',
            'primary_place_of_performance_congressional_district':
            'assistance_data__place_of_performance_congr',
            'primary_place_of_performance_country_code':
            'assistance_data__place_of_perform_country_c',
            'primary_place_of_performance_zip_4':
            'assistance_data__place_of_performance_zip4a',
            'recipient_address_line_1':
            'assistance_data__legal_entity_address_line1',
            'recipient_address_line_2':
            'assistance_data__legal_entity_address_line2',
            'recipient_address_line_3':
            'assistance_data__legal_entity_address_line3',
            'recipient_city_name': 'assistance_data__legal_entity_city_name',
            'recipient_congressional_district':
            'assistance_data__legal_entity_congressional',
            'recipient_country_code':
            'assistance_data__legal_entity_country_code',
            'recipient_duns': 'assistance_data__awardee_or_recipient_uniqu',
            'recipient_name': 'assistance_data__awardee_or_recipient_legal',
            'recipient_state_code': 'assistance_data__legal_entity_state_code',
            'record_type': 'assistance_data__record_type',
            'sai_number': 'assistance_data__sai_number',
            'total_funding_amount': 'assistance_data__total_funding_amount'
        }
    }
}
human_names = {
    'award': {
        'd1': [
            'award_id_piid', 'parent_award_agency_id', 'parent_award_id',
            'period_of_performance_start_date',
            'period_of_performance_current_end_date',
            'period_of_performance_potential_end_date',
            'ordering_period_end_date', 'awarding_agency_code',
            'awarding_agency_name', 'awarding_sub_agency_code',
            'awarding_sub_agency_name', 'awarding_office_code',
            'awarding_office_name', 'funding_agency_code',
            'funding_agency_name', 'funding_sub_agency_code',
            'funding_sub_agency_name', 'funding_office_code',
            'funding_office_name', 'recipient_duns', 'recipient_name',
            'recipient_doing_business_as_name', 'recipient_parent_name',
            'recipient_parent_duns', 'recipient_country_code',
            'recipient_country_name', 'recipient_address_line_1',
            'recipient_address_line_2', 'recipient_address_line_3',
            'recipient_city_name', 'recipient_state_code',
            'recipient_foreign_state_name', 'recipient_zip_4_code',
            'recipient_congressional_district', 'recipient_phone_number',
            'recipient_fax_number',
            'primary_place_of_performance_country_code',
            'primary_place_of_performance_city_name',
            'primary_place_of_performance_state_code',
            'primary_place_of_performance_zip_4',
            'primary_place_of_performance_congressional_district',
            'primary_place_of_performance_location_code', 'award_type_code',
            'idv_type_code', 'multiple_or_single_award_idv_code',
            'type_of_idc_code', 'type_of_contract_pricing_code',
            'award_description', 'solicitation_identifier',
            'number_of_actions', 'product_or_service_code',
            'contract_bundling_code', 'dod_claimant_program_code',
            'naics_code', 'naics_description',
            'recovered_materials_sustainability_code',
            'domestic_or_foreign_entity_code', 'dod_acquisition_program_code',
            'information_technology_commercial_item_category_code',
            'epa_designated_product_code',
            'country_of_product_or_service_origin_code',
            'place_of_manufacture_code', 'subcontracting_plan_code',
            'extent_competed_code', 'solicitation_procedures_code',
            'type_of_set_aside_code', 'evaluated_preference_code',
            'research_code', 'fair_opportunity_limited_sources_code',
            'other_than_full_and_open_competition_code',
            'number_of_offers_received',
            'commercial_item_acquisition_procedures_code',
            'small_business_competitiveness_demonstration _program',
            'commercial_item_test_program_code', 'a76_fair_act_action_code',
            'fed_biz_opps_code', 'local_area_set_aside_code',
            'clinger_cohen_act_planning_code', 'walsh_healey_act_code',
            'service_contract_act_code', 'davis_bacon_act_code',
            'interagency_contracting_authority_code',
            'other_statutory_authority', 'program_acronym',
            'parent_award_type_code', 'parent_award_single_or_multiple_code',
            'major_program', 'national_interest_action_code',
            'cost_or_pricing_data_code',
            'cost_accounting_standards_clause_code', 'gfe_gfp_code',
            'sea_transportation_code', 'consolidated_contract_code',
            'performance_based_service_acquisition_code',
            'multi_year_contract_code', 'contract_financing_code',
            'purchase_card_as_payment_method_code',
            'contingency_humanitarian_or_peacekeeping_operation_code',
            'alaskan_native_owned_corporation_or_firm',
            'american_indian_owned_business',
            'indian_tribe_federally_recognized',
            'native_hawaiian_owned_business', 'tribally_owned_business',
            'veteran_owned_business',
            'service_disabled_veteran_owned_business', 'woman_owned_business',
            'women_owned_small_business',
            'economically_disadvantaged_women_owned_small_business',
            'joint_venture_women_owned_small_business',
            'joint_venture_economic_disadvantaged_women_owned_small_bus',
            'minority_owned_business',
            'subcontinent_asian_asian_indian_american_owned_business',
            'asian_pacific_american_owned_business',
            'black_american_owned_business',
            'hispanic_american_owned_business',
            'native_american_owned_business', 'other_minority_owned_business',
            'contracting_officers_determination_of_business_size',
            'emerging_small_business',
            'community_developed_corporation_owned_firm',
            'labor_surplus_area_firm', 'us_federal_government',
            'federally_funded_research_and_development_corp', 'federal_agency',
            'us_state_government', 'us_local_government',
            'city_local_government', 'county_local_government',
            'inter_municipal_local_government', 'local_government_owned',
            'municipality_local_government',
            'school_district_local_government', 'township_local_government',
            'us_tribal_government', 'foreign_government',
            'corporate_entity_not_tax_exempt', 'corporate_entity_tax_exempt',
            'partnership_or_limited_liability_partnership',
            'sole_proprietorship', 'small_agricultural_cooperative',
            'international_organization', 'us_government_entity',
            'community_development_corporation', 'domestic_shelter',
            'educational_institution', 'foundation', 'hospital_flag',
            'manufacturer_of_goods', 'veterinary_hospital',
            'hispanic_servicing_institution', 'receives_contracts',
            'receives_grants', 'receives_contracts_and_grants',
            'airport_authority', 'council_of_governments',
            'housing_authorities_public_tribal', 'interstate_entity',
            'planning_commission', 'port_authority', 'transit_authority',
            'subchapter_scorporation', 'limited_liability_corporation',
            'foreign_owned_and_located', 'for_profit_organization',
            'nonprofit_organization', 'other_not_for_profit_organization',
            'the_ability_one_program', 'private_university_or_college',
            'state_controlled_institution_of_higher_learning',
            '1862_land_grant_college', '1890_land_grant_college',
            '1994_land_grant_college', 'minority_institution',
            'historically_black_college', 'tribal_college',
            'alaskan_native_servicing_institution',
            'native_hawaiian_servicing_institution', 'school_of_forestry',
            'veterinary_college', 'dot_certified_disadvantage',
            'self_certified_small_disadvantaged_business',
            'small_disadvantaged_business', 'c8a_program_participant',
            'historically_underutilized_business_zone _hubzone_firm',
            'sba_certified_8a_joint_venture'
        ],
        'd2': [
            'award_id_fain', 'award_id_uri', 'sai_number',
            'federal_action_obligation', 'non_federal_funding_amount',
            'total_funding_amount', 'period_of_performance_start_date',
            'period_of_performance_current_end_date', 'awarding_agency_code',
            'awarding_agency_name', 'awarding_sub_agency_code',
            'awarding_sub_agency_name', 'awarding_office_code',
            'funding_agency_code', 'funding_agency_name',
            'funding_sub_agency_code', 'funding_sub_agency_name',
            'funding_office_code', 'recipient_duns', 'recipient_name',
            'recipient_country_code', 'recipient_address_line_1',
            'recipient_address_line_2', 'recipient_address_line_3',
            'recipient_city_name', 'recipient_country_code',
            'recipient_state_code', 'recipient_congressional_district',
            'primary_place_of_performance_country_code',
            'primary_place_of_performance_zip_4',
            'primary_place_of_performance_congressional_district',
            'cfda_number', 'cfda_title', 'assistance_type',
            'award_description', 'business_funds_indicator', 'record_type',
            'last_modified_date'
        ]
    },
    'transaction': {
        'd1': [
            'award_id_piid', 'modification_number', 'transaction_number',
            'parent_award_agency_id', 'parent_award_id',
            'parent_award_modification_number', 'federal_action_obligation',
            'action_date', 'period_of_performance_start_date',
            'period_of_performance_current_end_date',
            'period_of_performance_potential_end_date',
            'ordering_period_end_date', 'awarding_agency_code',
            'awarding_agency_name', 'awarding_sub_agency_code',
            'awarding_sub_agency_name', 'awarding_office_code',
            'awarding_office_name', 'funding_agency_code',
            'funding_agency_name', 'funding_sub_agency_code',
            'funding_sub_agency_name', 'funding_office_code',
            'funding_office_name', 'recipient_duns', 'recipient_name',
            'recipient_doing_business_as_name', 'recipient_parent_name',
            'recipient_parent_duns', 'recipient_country_code',
            'recipient_country_name', 'recipient_address_line_1',
            'recipient_address_line_2', 'recipient_address_line_3',
            'recipient_city_name', 'recipient_state_code',
            'recipient_foreign_state_name', 'recipient_zip_4_code',
            'recipient_congressional_district', 'recipient_phone_number',
            'recipient_fax_number',
            'primary_place_of_performance_country_code',
            'primary_place_of_performance_city_name',
            'primary_place_of_performance_state_code',
            'primary_place_of_performance_zip_4',
            'primary_place_of_performance_congressional_district',
            'primary_place_of_performance_location_code', 'award_type_code',
            'idv_type_code', 'multiple_or_single_award_idv_code',
            'type_of_idc_code', 'type_of_contract_pricing_code',
            'award_description', 'action_type_code', 'solicitation_identifier',
            'number_of_actions', 'product_or_service_code',
            'contract_bundling_code', 'dod_claimant_program_code',
            'naics_code', 'naics_description',
            'recovered_materials_sustainability_code',
            'domestic_or_foreign_entity_code', 'dod_acquisition_program_code',
            'information_technology_commercial_item_category_code',
            'epa_designated_product_code',
            'country_of_product_or_service_origin_code',
            'place_of_manufacture_code', 'subcontracting_plan_code',
            'extent_competed_code', 'solicitation_procedures_code',
            'type_of_set_aside_code', 'evaluated_preference_code',
            'research_code', 'fair_opportunity_limited_sources_code',
            'other_than_full_and_open_competition_code',
            'number_of_offers_received',
            'commercial_item_acquisition_procedures_code',
            'small_business_competitiveness_demonstration _program',
            'commercial_item_test_program_code', 'a76_fair_act_action_code',
            'fed_biz_opps_code', 'local_area_set_aside_code',
            'price_evaluation_adjustment_preference_percent_difference',
            'clinger_cohen_act_planning_code', 'walsh_healey_act_code',
            'service_contract_act_code', 'davis_bacon_act_code',
            'interagency_contracting_authority_code',
            'other_statutory_authority', 'program_acronym',
            'parent_award_type_code', 'parent_award_single_or_multiple_code',
            'major_program', 'national_interest_action_code',
            'cost_or_pricing_data_code',
            'cost_accounting_standards_clause_code', 'gfe_gfp_code',
            'sea_transportation_code', 'undefinitized_action_code',
            'consolidated_contract_code',
            'performance_based_service_acquisition_code',
            'multi_year_contract_code', 'contract_financing_code',
            'purchase_card_as_payment_method_code',
            'contingency_humanitarian_or_peacekeeping_operation_code',
            'alaskan_native_owned_corporation_or_firm',
            'american_indian_owned_business',
            'indian_tribe_federally_recognized',
            'native_hawaiian_owned_business', 'tribally_owned_business',
            'veteran_owned_business',
            'service_disabled_veteran_owned_business', 'woman_owned_business',
            'women_owned_small_business',
            'economically_disadvantaged_women_owned_small_business',
            'joint_venture_women_owned_small_business',
            'joint_venture_economic_disadvantaged_women_owned_small_bus',
            'minority_owned_business',
            'subcontinent_asian_asian_indian_american_owned_business',
            'asian_pacific_american_owned_business',
            'black_american_owned_business',
            'hispanic_american_owned_business',
            'native_american_owned_business', 'other_minority_owned_business',
            'contracting_officers_determination_of_business_size',
            'emerging_small_business',
            'community_developed_corporation_owned_firm',
            'labor_surplus_area_firm', 'us_federal_government',
            'federally_funded_research_and_development_corp', 'federal_agency',
            'us_state_government', 'us_local_government',
            'city_local_government', 'county_local_government',
            'inter_municipal_local_government', 'local_government_owned',
            'municipality_local_government',
            'school_district_local_government', 'township_local_government',
            'us_tribal_government', 'foreign_government',
            'corporate_entity_not_tax_exempt', 'corporate_entity_tax_exempt',
            'partnership_or_limited_liability_partnership',
            'sole_proprietorship', 'small_agricultural_cooperative',
            'international_organization', 'us_government_entity',
            'community_development_corporation', 'domestic_shelter',
            'educational_institution', 'foundation', 'hospital_flag',
            'manufacturer_of_goods', 'veterinary_hospital',
            'hispanic_servicing_institution', 'receives_contracts',
            'receives_grants', 'receives_contracts_and_grants',
            'airport_authority', 'council_of_governments',
            'housing_authorities_public_tribal', 'interstate_entity',
            'planning_commission', 'port_authority', 'transit_authority',
            'subchapter_scorporation', 'limited_liability_corporation',
            'foreign_owned_and_located', 'for_profit_organization',
            'nonprofit_organization', 'other_not_for_profit_organization',
            'the_ability_one_program', 'private_university_or_college',
            'state_controlled_institution_of_higher_learning',
            '1862_land_grant_college', '1890_land_grant_college',
            '1994_land_grant_college', 'minority_institution',
            'historically_black_college', 'tribal_college',
            'alaskan_native_servicing_institution',
            'native_hawaiian_servicing_institution', 'school_of_forestry',
            'veterinary_college', 'dot_certified_disadvantage',
            'self_certified_small_disadvantaged_business',
            'small_disadvantaged_business', 'c8a_program_participant',
            'historically_underutilized_business_zone '
            '_hubzone_firm', 'sba_certified_8a_joint_venture'
        ],
        'd2': [
            'award_id_fain', 'modification_number', 'award_id_uri',
            'sai_number', 'federal_action_obligation',
            'non_federal_funding_amount', 'total_funding_amount',
            'action_date', 'period_of_performance_start_date',
            'period_of_performance_current_end_date', 'awarding_agency_code',
            'awarding_agency_name', 'awarding_sub_agency_code',
            'awarding_sub_agency_name', 'awarding_office_code',
            'funding_agency_code', 'funding_agency_name',
            'funding_sub_agency_code', 'funding_sub_agency_name',
            'funding_office_code', 'recipient_duns', 'recipient_name',
            'recipient_country_code', 'recipient_address_line_1',
            'recipient_address_line_2', 'recipient_address_line_3',
            'recipient_city_name', 'recipient_country_code',
            'recipient_state_code', 'recipient_congressional_district',
            'primary_place_of_performance_country_code',
            'primary_place_of_performance_zip_4',
            'primary_place_of_performance_congressional_district',
            'cfda_number', 'cfda_title', 'assistance_type',
            'award_description', 'business_funds_indicator',
            'action_type_code', 'action_type', 'record_type',
            'last_modified_date'
        ]
    }
}
