"""
Sets up mappings from column names used in downloads to the
query paths used to get the data from django.
"""

from bidict import bidict


all_query_paths = {
    'award_id_piid': 'piid',
    'award_id_fain': 'fain',
    'award_id_uri': 'uri',
    'parent_award_id': 'parent_award_id',
    '1862_land_grant_college':
    'transaction__recipient__c1862_land_grant_college',
    '1890_land_grant_college':
    'transaction__recipient__c1890_land_grant_college',
    '1994_land_grant_college':
    'transaction__recipient__c1994_land_grant_college',
    '8a_program_participant':
    'transaction__recipient__c8a_program_participant',
    'a76_fair_act_action_code': 'a76_fair_act_action',
    'action_date': 'transaction__action_date',
    'action_type': 'transaction__action_type_description',
    'action_type_code': 'transaction__action_type',
    'airport_authority': 'transaction__recipient__airport_authority',
    'alaskan_native_owned_corporation_or_firm':
    'transaction__recipient__alaskan_native_owned_corporation_or_firm',
    'alaskan_native_servicing_institution':
    'transaction__recipient__alaskan_native_servicing_institution',
    'american_indian_owned_business':
    'transaction__recipient__american_indian_owned_business',
    'asian_pacific_american_owned_business':
    'transaction__recipient__asian_pacific_american_owned_business',
    'award_description': 'transaction__award__description',
    'award_type': 'transaction__award__type_description',
    'award_type_code': 'transaction__award__type',
    'awarding_agency_code':
    'transaction__award__awarding_agency__toptier_agency__cgac_code',
    'awarding_agency_name':
    'transaction__award__awarding_agency__toptier_agency__name',
    'awarding_office_code':
    'transaction__award__awarding_agency__office_agency__aac_code',
    'awarding_office_name':
    'transaction__award__awarding_agency__office_agency__name',
    'awarding_sub_agency_code':
    'transaction__award__awarding_agency__subtier_agency__subtier_code',
    'awarding_sub_agency_name':
    'transaction__award__awarding_agency__subtier_agency__name',
    'black_american_owned_business':
    'transaction__recipient__black_american_owned_business',
    'city_local_government': 'transaction__recipient__city_local_government',
    'clinger-cohen_act_planning_compliance_code': 'clinger_cohen_act_planning',
    'commercial_item_acquisition_procedures':
    'commercial_item_acquisition_procedures_description',
    'commercial_item_acquisition_procedures_code':
    'commercial_item_acquisition_procedures',
    'commercial_item_test_program_code': 'commercial_item_test_program',
    'community_developed_corporation_owned_firm':
    'transaction__recipient__community_developed_corporation_owned_firm',
    'community_development_corporation':
    'transaction__recipient__community_development_corporation',
    'consolidated_contract_code': 'consolidated_contract',
    'contingency_humanitarian_or_peacekeeping_operation':
    'contingency_humanitarian_or_peacekeeping_operation_description',
    'contingency_humanitarian_or_peacekeeping_operation_code':
    'contingency_humanitarian_or_peacekeeping_operation',
    'contract_bundling': 'contract_bundling_description',
    'contract_bundling_code': 'contract_bundling',
    'contract_financing': 'contract_financing_description',
    'contract_financing_code': 'contract_financing',
    "contracting_officer's_determination_of_business_size":
    'contracting_officers_determination_of_business_size',
    'corporate_entity_not_tax_exempt':
    'transaction__recipient__corporate_entity_not_tax_exempt',
    'corporate_entity_tax_exempt':
    'transaction__recipient__corporate_entity_tax_exempt',
    'cost_accounting_standards_clause':
    'cost_accounting_standards_description',
    'cost_accounting_standards_clause_code': 'cost_accounting_standards',
    'cost_or_pricing_data': 'cost_or_pricing_data_description',
    'cost_or_pricing_data_code': 'cost_or_pricing_data',
    'council_of_governments': 'transaction__recipient__council_of_governments',
    'country_of_product_or_service_origin_code':
    'country_of_product_or_service_origin',
    'county_local_government':
    'transaction__recipient__county_local_government',
    'davis_bacon_act': 'davis_bacon_act_description',
    'davis_bacon_act_code': 'davis_bacon_act',
    'dod_acquisition_program_code': 'program_system_or_equipment_code',
    'dod_claimant_program_code': 'dod_claimant_program_code',
    'domestic_or_foreign_entity':
    'transaction__recipient__domestic_or_foreign_entity_description',
    'domestic_or_foreign_entity_code':
    'transaction__recipient__domestic_or_foreign_entity',
    'domestic_shelter': 'transaction__recipient__domestic_shelter',
    'dot_certified_disadvantaged_business_enterprise':
    'transaction__recipient__dot_certified_disadvantage',
    'economically_disadvantaged_women_owned_small_business':
    'transaction__recipient__economically_disadvantaged_women_owned_small_business',
    'educational_institution':
    'transaction__recipient__educational_institution',
    'emerging_small_business':
    'transaction__recipient__emerging_small_business',
    'epa_designated_product': 'epa_designated_product_description',
    'epa_designated_product_code': 'epa_designated_product',
    'evaluated_preference': 'evaluated_preference_description',
    'evaluated_preference_code': 'evaluated_preference',
    'extent_competed': 'extent_competed_description',
    'extent_competed_code': 'extent_competed',
    'fair_opportunity_limited_sources':
    'fair_opportunity_limited_sources_description',
    'fair_opportunity_limited_sources_code':
    'fair_opportunity_limited_sources',
    'fed_biz_opps': 'fed_biz_opps_description',
    'fed_biz_opps_code': 'fed_biz_opps',
    'federal_action_obligation': 'transaction__federal_action_obligation',
    'federal_agency': 'transaction__recipient__federal_agency',
    'federally_funded_research_and_development_corp':
    'transaction__recipient__federally_funded_research_and_development_corp',
    'for_profit_organization':
    'transaction__recipient__for_profit_organization',
    'foreign_funding': 'foreign_funding_description',
    'foreign_funding_code': 'foreign_funding',
    'foreign_government': 'transaction__recipient__foreign_government',
    'foreign_owned_and_located':
    'transaction__recipient__foreign_owned_and_located',
    'foundation': 'transaction__recipient__foundation',
    'funding_agency_code':
    'transaction__award__funding_agency__toptier_agency__cgac_code',
    'funding_agency_name':
    'transaction__award__funding_agency__toptier_agency__name',
    'funding_office_code':
    'transaction__award__funding_agency__office_agency__aac_code',
    'funding_office_name':
    'transaction__award__funding_agency__office_agency__name',
    'funding_sub_agency_code':
    'transaction__award__funding_agency__subtier_agency__subtier_code',
    'funding_sub_agency_name':
    'transaction__award__funding_agency__subtier_agency__name',
    'gfe_gfp_code': 'gfe_gfp',
    'hispanic_american_owned_business':
    'transaction__recipient__hispanic_american_owned_business',
    'hispanic_servicing_institution':
    'transaction__recipient__hispanic_servicing_institution',
    'historically_black_college_or_university':
    'transaction__recipient__historically_black_college',
    'historically_underutilized_business_zone_hubzone_firm':
    'transaction__recipient__historically_underutilized_business_zone',
    'hospital_flag': 'transaction__recipient__hospital_flag',
    'housing_authorities_public/tribal':
    'transaction__recipient__housing_authorities_public_tribal',
    'idv_type': 'idv_type_description',
    'idv_type_code': 'idv_type',
    'indian_tribe_federally_recognized':
    'transaction__recipient__indian_tribe_federally_recognized',
    'information_technology_commercial_item_category':
    'information_technology_commercial_item_category_description',
    'information_technology_commercial_item_category_code':
    'information_technology_commercial_item_category',
    'inter_municipal_local_government':
    'transaction__recipient__inter_municipal_local_government',
    'interagency_contracting_authority':
    'interagency_contracting_authority_description',
    'interagency_contracting_authority_code':
    'interagency_contracting_authority',
    'international_organization':
    'transaction__recipient__international_organization',
    'interstate_entity': 'transaction__recipient__interstate_entity',
    'joint_venture_economically_disadvantaged_women_owned_small_business':
    'transaction__recipient__joint_venture_economic_disadvantaged_women_owned_small_bus',
    'joint_venture_women_owned_small_business':
    'transaction__recipient__joint_venture_women_owned_small_business',
    'labor_surplus_area_firm':
    'transaction__recipient__labor_surplus_area_firm',
    'last_modified_date': 'last_modified_date',
    'limited_liability_corporation':
    'transaction__recipient__limited_liability_corporation',
    'local_area_set_aside_code': 'local_area_set_aside',
    'local_government_owned': 'transaction__recipient__local_government_owned',
    'major_program': 'major_program',
    'manufacturer_of_goods': 'transaction__recipient__manufacturer_of_goods',
    'minority_institution': 'transaction__recipient__minority_institution',
    'minority_owned_business':
    'transaction__recipient__minority_owned_business',
    'modification_number': 'transaction__modification_number',
    'multi_year_contract_code': 'multi_year_contract',
    'multiple_or_single_award_idv': 'multiple_or_single_award_idv_description',
    'multiple_or_single_award_idv_code': 'multiple_or_single_award_idv',
    'municipality_local_government':
    'transaction__recipient__municipality_local_government',
    'naics_code': 'naics',
    'naics_description': 'naics_description',
    'national_interest_action': 'national_interest_action_description',
    'national_interest_action_code': 'national_interest_action',
    'native_american_owned_business':
    'transaction__recipient__native_american_owned_business',
    'native_hawaiian_owned_business':
    'transaction__recipient__native_hawaiian_owned_business',
    'native_hawaiian_servicing_institution':
    'transaction__recipient__native_hawaiian_servicing_institution',
    'non_federal_funding_amount': 'non_federal_funding_amount',
    'nonprofit_organization': 'transaction__recipient__nonprofit_organization',
    'number_of_actions': 'number_of_actions',
    'number_of_offers_received': 'number_of_offers_received',
    'ordering_period_end_date': 'ordering_period_end_date',
    'other_minority_owned_business':
    'transaction__recipient__other_minority_owned_business',
    'other_not_for_profit_organization':
    'transaction__recipient__other_not_for_profit_organization',
    'other_statutory_authority': 'other_statutory_authority',
    'other_than_full_and_open_competition_code':
    'other_than_full_and_open_competition',
    'parent_award_agency_id': 'referenced_idv_agency_identifier',
    'parent_award_modification_number': 'referenced_idv_modification_number',
    'partnership_or_limited_liability_partnership':
    'transaction__recipient__partnership_or_limited_liability_partnership',
    'performance-based_service_acquisition':
    'performance_based_service_acquisition_description',
    'performance-based_service_acquisition_code':
    'performance_based_service_acquisition',
    'period_of_performance_current_end_date':
    'transaction__period_of_performance_current_end_date',
    'period_of_performance_potential_end_date':
    'period_of_performance_potential_end_date',
    'period_of_performance_start_date':
    'transaction__period_of_performance_start_date',
    'place_of_manufacture': 'place_of_manufacture_description',
    'place_of_manufacture_code': 'place_of_manufacture',
    'planning_commission': 'transaction__recipient__planning_commission',
    'port_authority': 'transaction__recipient__port_authority',
    'price_evaluation_adjustment_preference_percent_difference':
    'price_evaluation_adjustment_preference_percent_difference',
    'primary_place_of_performance_city_name':
    'transaction__place_of_performance__city_name',
    'primary_place_of_performance_congressional_district':
    'transaction__place_of_performance__congressional_code',
    'primary_place_of_performance_country_code':
    'transaction__place_of_performance__location_country_code',
    'primary_place_of_performance_country_name':
    'transaction__place_of_performance__country_name',
    'primary_place_of_performance_county_code':
    'transaction__place_of_performance__county_code',
    'primary_place_of_performance_county_name':
    'transaction__place_of_performance__county_name',
    'primary_place_of_performance_state_code':
    'transaction__place_of_performance__state_code',
    'primary_place_of_performance_state_name':
    'transaction__place_of_performance__state_name',
    'primary_place_of_performance_zip+4':
    'transaction__place_of_performance__zip4',
    'private_university_or_college\xa0':
    'transaction__recipient__private_university_or_college',
    'product_or_service_code': 'product_or_service_code',
    'program_acronym': 'program_acronym',
    'purchase_card_as_payment_method_code': 'purchase_card_as_payment_method',
    'receives_contracts': 'transaction__recipient__contracts',
    'receives_contracts_and_grants':
    'transaction__recipient__receives_contracts_and_grants',
    'receives_grants': 'transaction__recipient__grants',
    'recipient_address_line_1':
    'transaction__recipient__location__address_line1',
    'recipient_address_line_2':
    'transaction__recipient__location__address_line2',
    'recipient_address_line_3':
    'transaction__recipient__location__address_line3',
    'recipient_city_name': 'transaction__recipient__location__city_name',
    'recipient_congressional_district':
    'transaction__recipient__location__congressional_code',
    'recipient_country_code':
    'transaction__recipient__location__location_country_code',
    'recipient_country_name': 'transaction__recipient__location__country_name',
    'recipient_doing_business_as_name':
    'transaction__recipient__vendor_doing_as_business_name',
    'recipient_duns': 'transaction__recipient__recipient_unique_id',
    'recipient_fax_number': 'transaction__recipient__vendor_fax_number',
    'recipient_foreign_state_name':
    'transaction__recipient__location__foreign_province',
    'recipient_name': 'transaction__recipient__recipient_name',
    'recipient_parent_name':
    'transaction__recipient__parent_recipient_unique_id',
    'recipient_phone_number': 'transaction__recipient__vendor_phone_number',
    'recipient_state_code': 'transaction__recipient__location__state_code',
    'recipient_state_name': 'transaction__recipient__location__state_name',
    'recipient_zip_4_code': 'transaction__recipient__location__zip4',
    'recovered_materials/sustainability':
    'recovered_materials_sustainability_description',
    'recovered_materials/sustainability_code':
    'recovered_materials_sustainability',
    'research': 'research_description',
    'research_code': 'research',
    'sai_number': 'sai_number',
    'sam_exception': 'transaction__recipient__sam_exception',
    'sba_certified_8a_joint_venture':
    'transaction__recipient__sba_certified_8a_joint_venture',
    'school_district_local_government':
    'transaction__recipient__school_district_local_government',
    'school_of_forestry': 'transaction__recipient__school_of_forestry',
    'sea_transportation': 'sea_transportation_description',
    'sea_transportation_code': 'sea_transportation',
    'self-certified_small_disadvantaged_business':
    'transaction__recipient__self_certified_small_disadvantaged_business',
    'service_contract_act': 'service_contract_act_description',
    'service_contract_act_code': 'service_contract_act',
    'service_disabled_veteran_owned_business':
    'transaction__recipient__service_disabled_veteran_owned_business',
    'small_agricultural_cooperative':
    'transaction__recipient__small_agricultural_cooperative',
    'small_business_competitiveness_demonstration_program':
    'small_business_competitiveness_demonstration_program',
    'small_disadvantaged_business':
    'transaction__recipient__small_disadvantaged_business',
    'sole_proprietorship': 'transaction__recipient__sole_proprietorship',
    'solicitation_identifier': 'solicitation_identifier',
    'solicitation_procedures': 'solicitation_procedures_description',
    'solicitation_procedures_code': 'solicitation_procedures',
    'state_controlled_institution_of_higher_learning':
    'transaction__recipient__state_controlled_institution_of_higher_learning',
    'subchapter_s_corporation':
    'transaction__recipient__subchapter_scorporation',
    'subcontinent_asian_asian_indian_american_owned_business':
    'transaction__recipient__subcontinent_asian_asian_indian_american_owned_business',
    'subcontracting_plan': 'subcontracting_plan_description',
    'subcontracting_plan_code': 'subcontracting_plan',
    'the_abilityone_program': 'transaction__recipient__the_ability_one_program',
    'total_funding_amount': 'total_funding_amount',
    'township_local_government':
    'transaction__recipient__township_local_government',
    'transaction_number': 'transaction_number',
    'transit_authority': 'transaction__recipient__transit_authority',
    'tribal_college': 'transaction__recipient__tribal_college',
    'tribally_owned_business':
    'transaction__recipient__tribally_owned_business',
    'type_of_contract_pricing': 'type_of_contract_pricing_description',
    'type_of_contract_pricing_code': 'type_of_contract_pricing',
    'type_of_idc': 'type_of_idc_description',
    'type_of_idc_code': 'type_of_idc',
    'type_of_set_aside': 'type_set_aside_description',
    'type_of_set_aside_code': 'type_set_aside',
    'us_federal_government': 'transaction__recipient__us_federal_government',
    'us_government_entity': 'transaction__recipient__us_government_entity',
    'us_local_government': 'transaction__recipient__us_local_government',
    'us_state_government': 'transaction__recipient__us_state_government',
    'us_tribal_government': 'transaction__recipient__us_tribal_government',
    'undefinitized_action_code': 'transaction__recipient__undefinitized_action',
    'veteran_owned_business': 'transaction__recipient__veteran_owned_business',
    'veterinary_college': 'transaction__recipient__veterinary_college',
    'veterinary_hospital': 'transaction__recipient__veterinary_hospital',
    'walsh_healey_act_code': 'walsh_healey_act',
    'woman_owned_business': 'transaction__recipient__woman_owned_business',
    'women_owned_small_business':
    'transaction__recipient__women_owned_small_business'
}
"""
Columns not yet mapped

    # "Parent Award Agency Name": "transaction__award__parent_award__name",  # TODO  # not in spreadsheet
    # "Change in Current Award Amount": "base_exercised_options_val",
    # "Change in Potential Award Amount": "base_and_all_options_value",
    # "Recipient Parent DUNS": "recipient_parent_name",  # Goes to ultimate_parent_name, then nowhere?
    # "Primary Place of Performance Location Code": "transaction__place_of_performance__location_code",  # Ross says: field is worthless... it was deprecated years ago in FPDS
    # "Award or Parent Award Flag": "award_or_parent_award_flag",  # Ross says: this is a field i had alisa add to indicate whether the pull is from the award or idv feed.  you'll have to ask her what she mapped it to
    # "Product or Service Code (PSC) Description": "product_or_service_code_description",  # TODO   we don't have
    # "DoD Claimant Program Description": "dod_claimant_program_description",  # TODO   we don't have
    # "DoD Acquisition Program Description": "dod_acquisition_program_description",  # TODO    we dont' have
    # "Country of Product or Service Origin": "country_of_product_or_service_origin",  # TODO not quite right   we don't have
    # "Other than Full and Open Competition": "other_than_full_and_open_competition",  # TODO: we don't have
    # "Commercial Item Test Program": "commercial_item_test_program_description",  # TODO   we don't have
    # "A-76 FAIR Act Action": "a76_fair_act_action_description",  # TODO   we don't have
    # "Local Area Set Aside": "local_area_set_aside_description",  # TODO   we don't have
    # "Clinger-Cohen Act Planning Compliance": "clinger_cohen_act_planning_description",  # TODO   we don't have
    # "Walsh Healey Act": "walsh_healey_act_description",  # TODO   we don't have
    # "Parent Award Type Code": "referenced_idv_type",
    # "Parent Award Type": "parent_award_type",  # TODO - referenced_idv_type and referenced_idv_type_desc exist in broker db, but never in submission download
    # "Parent Award Single or Multiple Code": "multiple_or_single_award_idv",
    # "Parent Award Single or Multiple": "multiple_or_single_award_idv_description",  referenced_mult_or_single exists in db
    # "GFE and GFP": "gfe_gfp",  # TODO: Code or description?
    # "Undefinitized Action": "undefinitized_action_description",  # TODO
    # "Consolidated Contract": "consolidated_contract_description",  # TODO: Code or description?
    # "Multi Year Contract": "multi_year_contract",  # TODO: Code or description?
    # "Purchase Card as Payment Method": "purchase_card_as_payment_method_description",  # TODO: Code or description?
    # "Contracting Officer's Determination of Business Size Code": "contracting_officers_determination_of_business_size_code",

    """

query_paths = {
    'transaction': {
        'd1': bidict(),
        'd2': bidict()
    },
    'award': {
        'd1': bidict(),
        'd2': bidict()
    }
}

for (human_name, query_path) in all_query_paths.items():
    for (txn_source, detail_path) in (('d1', 'contract'),
                                      ('d2', 'assistance')):
        query_paths['transaction'][txn_source][human_name] = query_path

# a few fields should be reported for both contracts and assistance,
# but differently
query_paths['transaction']['d2'][
    'period_of_performance_current_end_date'] = 'period_of_performance_current_end_date'
query_paths['transaction']['d2'][
    'period_of_performance_start_date'] = 'period_of_performance_start_date'

# These fields have the same paths from either the transaction or the award
unmodified_path_for_award = ('last_modified_date', 'award_id_piid',
                             'award_id_fain', 'award_id_uri',
                             'period_of_performance_start_date',
                             'period_of_performance_current_end_date')

# Other query paths change when they are queried from Award rather than Transaction
for (txn_source, detail_path) in (('d1', 'contract'), ('d2', 'assistance')):
    for (human_name,
         query_path) in query_paths['transaction'][txn_source].items():
        if query_path in unmodified_path_for_award:
            path_from_award = query_path
        elif query_path.startswith('transaction__award__'):
            path_from_award = query_path[len('transaction__award__'):]
        elif query_path.startswith(
                'transaction__recipient__') or query_path.startswith(
                    'transaction__place_of_performance__'):
            # recipient and PoP are directly attached to Award
            path_from_award = query_path[len('transaction__'):]
        else:
            # Navigate to latest child of transaction (contract or assistance)
            path_from_award = 'latest_transaction__{}_data__{}'.format(
                detail_path, query_path)
        query_paths['award'][txn_source][human_name] = path_from_award

# now prune out those not called for, by human name

human_names = {
    'award': {
        'd1': [
            'award_id_piid', 'parent_award_agency_id',
            'parent_award_agency_name', 'parent_award_id',
            'period_of_performance_start_date',
            'period_of_performance_current_end_date',
            'period_of_performance_potential_end_date',
            'ordering_period_end_date', 'awarding_agency_code',
            'awarding_agency_name', 'awarding_sub_agency_code',
            'awarding_sub_agency_name', 'awarding_office_code',
            'awarding_office_name', 'funding_agency_code',
            'funding_agency_name', 'funding_sub_agency_code',
            'funding_sub_agency_name', 'funding_office_code',
            'funding_office_name', 'foreign_funding', 'sam_exception',
            'recipient_duns', 'recipient_name',
            'recipient_doing_business_as_name', 'recipient_parent_name',
            'recipient_parent_duns', 'recipient_country_code',
            'recipient_country_name', 'recipient_address_line_1',
            'recipient_address_line_2', 'recipient_address_line_3',
            'recipient_city_name', 'recipient_state_code',
            'recipient_state_name', 'recipient_foreign_state_name',
            'recipient_zip_4_code', 'recipient_congressional_district',
            'recipient_phone_number', 'recipient_fax_number',
            'primary_place_of_performance_country_code',
            'primary_place_of_performance_country_name',
            'primary_place_of_performance_city_name',
            'primary_place_of_performance_county_name',
            'primary_place_of_performance_state_code',
            'primary_place_of_performance_state_name',
            'primary_place_of_performance_zip_4',
            'primary_place_of_performance_congressional_district',
            'primary_place_of_performance_location_code',
            'award_or_parent_award_flag', 'award_type_code', 'award_type',
            'idv_type_code', 'idv_type', 'multiple_or_single_award_idv_code',
            'multiple_or_single_award_idv', 'type_of_idc_code', 'type_of_idc',
            'type_of_contract_pricing_code', 'type_of_contract_pricing',
            'award_description', 'solicitation_identifier',
            'number_of_actions', 'product_or_service_code',
            'product_or_service_code_description', 'contract_bundling_code',
            'contract_bundling', 'dod_claimant_program_code',
            'dod_claimant_program_description', 'naics_code',
            'naics_description', 'recovered_materials_sustainability_code',
            'recovered_materials_sustainability',
            'domestic_or_foreign_entity_code', 'domestic_or_foreign_entity',
            'dod_acquisition_program_code',
            'dod_acquisition_program_description',
            'information_technology_commercial_item_category_code',
            'information_technology_commercial_item_category',
            'epa_designated_product_code', 'epa_designated_product',
            'country_of_product_or_service_origin_code',
            'country_of_product_or_service_origin',
            'place_of_manufacture_code', 'place_of_manufacture',
            'subcontracting_plan_code', 'subcontracting_plan',
            'extent_competed_code', 'extent_competed',
            'solicitation_procedures_code', 'solicitation_procedures',
            'type_of_set_aside_code', 'type_of_set_aside',
            'evaluated_preference_code', 'evaluated_preference',
            'research_code', 'research',
            'fair_opportunity_limited_sources_code',
            'fair_opportunity_limited_sources',
            'other_than_full_and_open_competition_code',
            'other_than_full_and_open_competition',
            'number_of_offers_received',
            'commercial_item_acquisition_procedures_code',
            'commercial_item_acquisition_procedures',
            'small_business_competitiveness_demonstration _program',
            'commercial_item_test_program_code',
            'commercial_item_test_program', 'a76_fair_act_action_code',
            'a76_fair_act_action', 'fed_biz_opps_code', 'fed_biz_opps',
            'local_area_set_aside_code', 'local_area_set_aside',
            'clinger_cohen_act_planning_code', 'clinger_cohen_act_planning',
            'walsh_healey_act_code', 'walsh_healey_act',
            'service_contract_act_code', 'service_contract_act',
            'davis_bacon_act_code', 'davis_bacon_act',
            'interagency_contracting_authority_code',
            'interagency_contracting_authority', 'other_statutory_authority',
            'program_acronym', 'parent_award_type_code', 'parent_award_type',
            'parent_award_single_or_multiple_code',
            'parent_award_single_or_multiple', 'major_program',
            'national_interest_action_code', 'national_interest_action',
            'cost_or_pricing_data_code', 'cost_or_pricing_data',
            'cost_accounting_standards_clause_code',
            'cost_accounting_standards_clause', 'gfe_gfp_code', 'gfe_gfp',
            'sea_transportation_code', 'sea_transportation',
            'consolidated_contract_code', 'consolidated_contract',
            'performance_based_service_acquisition_code',
            'performance_based_service_acquisition',
            'multi_year_contract_code', 'multi_year_contract',
            'contract_financing_code', 'contract_financing',
            'purchase_card_as_payment_method_code',
            'purchase_card_as_payment_method',
            'contingency_humanitarian_or_peacekeeping_operation_code',
            'contingency_humanitarian_or_peacekeeping_operation',
            'alaskan_native_owned_corporation_or_firm',
            'american_indian_owned_business',
            'indian_tribe_federally_recognized',
            'native_hawaiian_owned_business', 'tribally_owned_business',
            'veteran_owned_business', 'service_disabled_veteran_owned_business',
            'woman_owned_business', 'women_owned_small_business',
            'economically_disadvantaged_women_owned_small_business',
            'joint_venture_women_owned_small_business',
            'joint_venture_economic_disadvantaged_women_owned_small_bus',
            'minority_owned_business',
            'subcontinent_asian_asian_indian_american_owned_business',
            'asian_pacific_american_owned_business',
            'black_american_owned_business', 'hispanic_american_owned_business',
            'native_american_owned_business', 'other_minority_owned_business',
            'contracting_officers_determination_of_business_size',
            'contracting_officers_determination_of_business_size_code',
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
            'native_hawaiian_servicing_institution', 'school_of_forestry',
            'veterinary_college', 'dot_certified_disadvantage',
            'self_certified_small_disadvantaged_business',
            'small_disadvantaged_business', 'c8a_program_participant',
            'historically_underutilized_business_zone _hubzone_firm',
            'sba_certified_8a_joint_venture', 'last_modified_date'
        ],
        'd2': [
            'award_id_fain', 'award_id_uri', 'sai_number',
            'federal_action_obligation', 'non_federal_funding_amount',
            'total_funding_amount', 'face_value_of_loan',
            'original_subsidy_cost', 'period_of_performance_start_date',
            'period_of_performance_current_end_date', 'awarding_agency_code',
            'awarding_agency_name', 'awarding_sub_agency_code',
            'awarding_sub_agency_name', 'awarding_office_code',
            'awarding_office_name', 'funding_agency_code',
            'funding_agency_name', 'funding_sub_agency_code',
            'funding_sub_agency_name', 'funding_office_code',
            'funding_office_name', 'recipient_duns', 'recipient_name',
            'recipient_country_code', 'recipient_country_name',
            'recipient_address_line_1', 'recipient_address_line_2',
            'recipient_address_line_3', 'recipient_city_code',
            'recipient_city_name', 'recipient_country_code',
            'recipient_country_name', 'recipient_state_code',
            'recipient_state_name', 'recipient_zip_code',
            'recipient_zip_last_4_code', 'recipient_congressional_district',
            'recipient_foreign_city_name', 'recipient_foreign_province_name',
            'recipient_foreign_postal_code',
            'primary_place_of_performance_country_code',
            'primary_place_of_performance_country_name',
            'primary_place_of_performance_code',
            'primary_place_of_performance_city_name',
            'primary_place_of_performance_county_code',
            'primary_place_of_performance_county_name',
            'primary_place_of_performance_state_name',
            'primary_place_of_performance_zip_4',
            'primary_place_of_performance_congressional_district',
            'primary_place_of_performance_foreign_location', 'cfda_number',
            'cfda_title', 'assistance_type_code', 'assistance_type',
            'award_description', 'business_funds_indicator_code',
            'business_funds_indicator', 'business_types_code',
            'Business Types', 'record_type_code', 'record_type',
            'last_modified_date'
        ]
    },
    'transaction': {
        'd1': [
            'award_id_piid', 'modification_number', 'transaction_number',
            'parent_award_agency_id', 'parent_award_agency_name',
            'parent_award_id', 'parent_award_modification_number',
            'federal_action_obligation', 'change_in_current_award_amount',
            'change_in_potential_award_amount', 'action_date',
            'period_of_performance_start_date',
            'period_of_performance_current_end_date',
            'period_of_performance_potential_end_date',
            'ordering_period_end_date', 'awarding_agency_code',
            'awarding_agency_name', 'awarding_sub_agency_code',
            'awarding_sub_agency_name', 'awarding_office_code',
            'awarding_office_name', 'funding_agency_code',
            'funding_agency_name', 'funding_sub_agency_code',
            'funding_sub_agency_name', 'funding_office_code',
            'funding_office_name', 'foreign_funding', 'sam_exception',
            'recipient_duns', 'recipient_name',
            'recipient_doing_business_as_name', 'recipient_parent_name',
            'recipient_parent_duns', 'recipient_country_code',
            'recipient_country_name', 'recipient_address_line_1',
            'recipient_address_line_2', 'recipient_address_line_3',
            'recipient_city_name', 'recipient_state_code',
            'recipient_state_name', 'recipient_foreign_state_name',
            'recipient_zip_4_code', 'recipient_congressional_district',
            'recipient_phone_number', 'recipient_fax_number',
            'primary_place_of_performance_country_code',
            'primary_place_of_performance_country_name',
            'primary_place_of_performance_city_name',
            'primary_place_of_performance_county_name',
            'primary_place_of_performance_state_code',
            'primary_place_of_performance_state_name',
            'primary_place_of_performance_zip_4',
            'primary_place_of_performance_congressional_district',
            'primary_place_of_performance_location_code',
            'award_or_parent_award_flag', 'award_type_code', 'award_type',
            'idv_type_code', 'idv_type', 'multiple_or_single_award_idv_code',
            'multiple_or_single_award_idv', 'type_of_idc_code', 'type_of_idc',
            'type_of_contract_pricing_code', 'type_of_contract_pricing',
            'award_description', 'action_type_code', 'action_type',
            'solicitation_identifier', 'number_of_actions',
            'product_or_service_code', 'product_or_service_code_description',
            'contract_bundling_code', 'contract_bundling',
            'dod_claimant_program_code', 'dod_claimant_program_description',
            'naics_code', 'naics_description',
            'recovered_materials_sustainability_code',
            'recovered_materials_sustainability',
            'domestic_or_foreign_entity_code', 'domestic_or_foreign_entity',
            'dod_acquisition_program_code',
            'dod_acquisition_program_description',
            'information_technology_commercial_item_category_code',
            'information_technology_commercial_item_category',
            'epa_designated_product_code', 'epa_designated_product',
            'country_of_product_or_service_origin_code',
            'country_of_product_or_service_origin',
            'place_of_manufacture_code', 'place_of_manufacture',
            'subcontracting_plan_code', 'subcontracting_plan',
            'extent_competed_code', 'extent_competed',
            'solicitation_procedures_code', 'solicitation_procedures',
            'type_of_set_aside_code', 'type_of_set_aside',
            'evaluated_preference_code', 'evaluated_preference',
            'research_code', 'research',
            'fair_opportunity_limited_sources_code',
            'fair_opportunity_limited_sources',
            'other_than_full_and_open_competition_code',
            'other_than_full_and_open_competition',
            'number_of_offers_received',
            'commercial_item_acquisition_procedures_code',
            'commercial_item_acquisition_procedures',
            'small_business_competitiveness_demonstration '
            '_program', 'commercial_item_test_program_code',
            'commercial_item_test_program', 'a76_fair_act_action_code',
            'a76_fair_act_action', 'fed_biz_opps_code', 'fed_biz_opps',
            'local_area_set_aside_code', 'local_area_set_aside',
            'price_evaluation_adjustment_preference_percent_difference',
            'clinger_cohen_act_planning_code', 'clinger_cohen_act_planning',
            'walsh_healey_act_code', 'walsh_healey_act',
            'service_contract_act_code', 'service_contract_act',
            'davis_bacon_act_code', 'davis_bacon_act',
            'interagency_contracting_authority_code',
            'interagency_contracting_authority', 'other_statutory_authority',
            'program_acronym', 'parent_award_type_code', 'parent_award_type',
            'parent_award_single_or_multiple_code',
            'parent_award_single_or_multiple', 'major_program',
            'national_interest_action_code', 'national_interest_action',
            'cost_or_pricing_data_code', 'cost_or_pricing_data',
            'cost_accounting_standards_clause_code',
            'cost_accounting_standards_clause', 'gfe_gfp_code', 'gfe_gfp',
            'sea_transportation_code', 'sea_transportation',
            'undefinitized_action_code', 'undefinitized_action',
            'consolidated_contract_code', 'consolidated_contract',
            'performance_based_service_acquisition_code',
            'performance_based_service_acquisition', 'multi_year_contract_code',
            'multi_year_contract', 'contract_financing_code',
            'contract_financing', 'purchase_card_as_payment_method_code',
            'purchase_card_as_payment_method',
            'contingency_humanitarian_or_peacekeeping_operation_code',
            'contingency_humanitarian_or_peacekeeping_operation',
            'alaskan_native_owned_corporation_or_firm',
            'american_indian_owned_business',
            'indian_tribe_federally_recognized',
            'native_hawaiian_owned_business', 'tribally_owned_business',
            'veteran_owned_business', 'service_disabled_veteran_owned_business',
            'woman_owned_business', 'women_owned_small_business',
            'economically_disadvantaged_women_owned_small_business',
            'joint_venture_women_owned_small_business',
            'joint_venture_economic_disadvantaged_women_owned_small_bus',
            'minority_owned_business',
            'subcontinent_asian_asian_indian_american_owned_business',
            'asian_pacific_american_owned_business',
            'black_american_owned_business', 'hispanic_american_owned_business',
            'native_american_owned_business', 'other_minority_owned_business',
            'contracting_officers_determination_of_business_size',
            'contracting_officers_determination_of_business_size_code',
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
            'native_hawaiian_servicing_institution', 'school_of_forestry',
            'veterinary_college', 'dot_certified_disadvantage',
            'self_certified_small_disadvantaged_business',
            'small_disadvantaged_business', 'c8a_program_participant',
            'historically_underutilized_business_zone '
            '_hubzone_firm', 'sba_certified_8a_joint_venture',
            'last_modified_date'
        ],
        'd2': [
            'award_id_fain', 'modification_number', 'award_id_uri',
            'sai_number', 'federal_action_obligation',
            'non_federal_funding_amount', 'total_funding_amount',
            'face_value_of_loan', 'original_subsidy_cost', 'action_date',
            'period_of_performance_start_date',
            'period_of_performance_current_end_date', 'awarding_agency_code',
            'awarding_agency_name', 'awarding_sub_agency_code',
            'awarding_sub_agency_name', 'awarding_office_code',
            'awarding_office_name', 'funding_agency_code',
            'funding_agency_name', 'funding_sub_agency_code',
            'funding_sub_agency_name', 'funding_office_code',
            'funding_office_name', 'recipient_duns', 'recipient_name',
            'recipient_country_code', 'recipient_country_name',
            'recipient_address_line_1', 'recipient_address_line_2',
            'recipient_address_line_3', 'recipient_city_code',
            'recipient_city_name', 'recipient_country_code',
            'recipient_country_name', 'recipient_state_code',
            'recipient_state_name', 'recipient_zip_code',
            'recipient_zip_last_4_code', 'recipient_congressional_district',
            'recipient_foreign_city_name', 'recipient_foreign_province_name',
            'recipient_foreign_postal_code',
            'primary_place_of_performance_country_code',
            'primary_place_of_performance_country_name',
            'primary_place_of_performance_code',
            'primary_place_of_performance_city_name',
            'primary_place_of_performance_county_code',
            'primary_place_of_performance_county_name',
            'primary_place_of_performance_state_name',
            'primary_place_of_performance_zip_4',
            'primary_place_of_performance_congressional_district',
            'primary_place_of_performance_foreign_location', 'cfda_number',
            'cfda_title', 'assistance_type_code', 'assistance_type',
            'award_description', 'business_funds_indicator_code',
            'business_funds_indicator', 'business_types_code',
            'Business Types', 'action_type_code', 'action_type',
            'record_type_code', 'record_type',
            'fiscal_year_and_quarter_correction', 'last_modified_date'
        ]
    }
}

# this leaves us with a bunch of paths not actually in use for each type
# prune those out now

for model_type in ('transaction', 'award'):
    for file_type in ('d1', 'd2'):
        query_paths[model_type][file_type] = {
            k: v
            for (k, v) in query_paths[model_type][file_type].items()
            if k in human_names[model_type][file_type]
        }

        missing = set(human_names[model_type][file_type]) - set(query_paths[
            model_type][file_type])
        if missing:
            msg = 'missing query paths: {}'.format(missing)
            # raise KeyError(msg)
            print
