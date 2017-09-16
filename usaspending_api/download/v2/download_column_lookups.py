from usaspending_api.awards.models import Award, TransactionContract, TransactionAssistance

from bidict import bidict


all_query_paths = {
    "Award ID": "piid",  # DONE  # TODO : differs for contract, assistance; fain ...
    "Modification Number": "transaction__modification_number",  # DONE
    "Transaction Number": "transaction_number",  # DONE
    "Parent Award Agency ID": "referenced_idv_agency_identifier",  # DONE
    # "Parent Award Agency Name": "transaction__award__parent_award__name",  # TODO  # not in spreadsheet
    "Parent Award ID": "parent_award_id",  # TODO: Need to actually get the PIID/FAIN/ID from the parent award, NOT the surrogate PK
    "Parent Award Modification Number": "referenced_idv_modification_number",
    "Federal Action Obligation": "transaction__federal_action_obligation",  # DONE
    # Per Ross, BaseAndExercisedOptionsValue is "Change in Current Award Amount" and BaseAndAllOptionsValue is "Change in Potential Award Amount" - Alisa has just finished a story to add these to the broker side
    # "Change in Current Award Amount": "change_in_current_award_amount",
    # "Change in Potential Award Amount": "change_in_potential_award_amount",
    "Action Date": "transaction__action_date",  # DONE
    "Period of Performance Start Date": "transaction__period_of_performance_start_date",  # DONE
    "Period of Performance Current End Date": "transaction__period_of_performance_current_end_date",  # DONE
    "Period of Performance Potential End Date": "period_of_performance_potential_end_date",  # DONE
    "Ordering Period End Date": "ordering_period_end_date",  # DONE
    "Awarding Agency Code": "transaction__award__awarding_agency__toptier_agency__cgac_code",  # CDone
    "Awarding Agency Name": "transaction__award__awarding_agency__toptier_agency__name",  # CDone
    "Awarding Sub Agency Code": "transaction__award__awarding_agency__subtier_agency__subtier_code",  # CDone
    "Awarding Sub Agency Name": "transaction__award__awarding_agency__subtier_agency__name",  # CDone
    "Awarding Office Code": "transaction__award__awarding_agency__office_agency__aac_code",  # CDone
    "Awarding Office Name": "transaction__award__awarding_agency__office_agency__name",  # CDone
    "Funding Agency Code": "transaction__award__funding_agency__toptier_agency__cgac_code",  # CDone
    "Funding Agency Name": "transaction__award__funding_agency__toptier_agency__name",  # CDone
    "Funding Sub Agency Code": "transaction__award__funding_agency__subtier_agency__subtier_code",  # CDone
    "Funding Sub Agency Name": "transaction__award__funding_agency__subtier_agency__name",  # CDone
    "Funding Office Code": "transaction__award__funding_agency__office_agency__aac_code",  # CDone
    "Funding Office Name": "transaction__award__funding_agency__office_agency__name",  # CDone
    "Foreign Funding Code": "foreign_funding",  # DONE
    "Foreign Funding": "foreign_funding_description",  # DONE
    "SAM Exception": "transaction__recipient__sam_exception",
    "Recipient DUNS": "transaction__recipient__recipient_unique_id",  # DONE
    "Recipient Name": "transaction__recipient__recipient_name",  # DONE
    "Recipient Doing Business As Name": "transaction__recipient__vendor_doing_as_business_name",  # DONE
    # "Recipient Parent Name": "recipient_parent_name",  # Long explanation from Ross in slack...
    "Recipient Parent DUNS": "transaction__recipient__parent_recipient_unique_id",  # DONE
    "Recipient Country Code": "transaction__recipient__location__location_country_code",  # DONE
    "Recipient Country Name": "transaction__recipient__location__country_name",  # DONE
    "Recipient Address Line 1": "transaction__recipient__location__address_line1",  # DONE
    "Recipient Address Line 2": "transaction__recipient__location__address_line2",  # DONE
    "Recipient Address Line 3": "transaction__recipient__location__address_line3",  # DONE
    "Recipient City Name": "transaction__recipient__location__city_name",  # DONE
    "Recipient State Code": "transaction__recipient__location__state_code",  # DONE
    "Recipient State Name": "transaction__recipient__location__state_name",  # DONE
    "Recipient Foreign State Name": "transaction__recipient__location__foreign_province",  # TODO probably right
    "Recipient Zip+4 Code": "transaction__recipient__location__zip4",  # Confirmed, zip4 to zip4 in settings.py
    "Recipient Congressional District": "transaction__recipient__location__congressional_code",  # TODO probably right
    "Recipient Phone Number": "transaction__recipient__vendor_phone_number",  # DONE
    "Recipient Fax Number": "transaction__recipient__vendor_fax_number",  # DONE
    "Primary Place of Performance City Name": "transaction__place_of_performance__city_name",  # DONE
    "Primary Place of Performance County Name": "transaction__place_of_performance__county_name",  # DONE
    "Primary Place of Performance State Code": "transaction__place_of_performance__state_code",  # DONE
    "Primary Place of Performance State Name": "transaction__place_of_performance__state_name",  # DONE
    "Primary Place of Performance Zip+4": "transaction__place_of_performance__zip4",  # DONE
    "Primary Place of Performance Congressional District": "transaction__place_of_performance__congressional_code",  # TODO probably right
    "Primary Place of Performance Country Code": "transaction__place_of_performance__location_country_code",  # DONE
    "Primary Place of Performance Country Name": "transaction__place_of_performance__country_name",  # DONE
    # "Primary Place of Performance Location Code": "transaction__place_of_performance__location_code",  # TODO
    # "Award or Parent Award Flag": "award_or_parent_award_flag",  # TODO wut
    "Award Type Code": "transaction__award__type",  # DONE
    "Award Type": "transaction__award__type_description",  # DONE
    "IDV Type Code": "idv_type",  # DONE
    "IDV Type": "idv_type_description",  # DONE
    "Multiple or Single Award IDV Code": "multiple_or_single_award_idv",  # DONE
    "Multiple or Single Award IDV": "multiple_or_single_award_idv_description",  # DONE
    "Type of IDC Code": "type_of_idc",  # DONE
    "Type of IDC": "type_of_idc_description",  # DONE
    "Type of Contract Pricing Code": "type_of_contract_pricing",  # DONE
    "Type of Contract Pricing": "type_of_contract_pricing_description",  # DONE
    "Award Description": "transaction__award__description",
    "Action Type Code": "transaction__action_type",  # DONE
    "Action Type": "transaction__action_type_description",  # DONE
    "Solicitation Identifier": "solicitation_identifier",  # DONE
    "Number of Actions": "number_of_actions",  # DONE
    "Product or Service Code (PSC)": "product_or_service_code",  # DONE
    # "Product or Service Code (PSC) Description": "product_or_service_code_description",  # TODO   we don't have
    "Contract Bundling Code": "contract_bundling",  # DONE
    "Contract Bundling": "contract_bundling_description",  # DONE
    "DoD Claimant Program Code": "dod_claimant_program_code",  # DONE
    # "DoD Claimant Program Description": "dod_claimant_program_description",  # TODO   we don't have
    "NAICS Code": "naics",  # DONE
    "NAICS Description": "naics_description",  # DONE
    "Recovered Materials/Sustainability Code": "recovered_materials_sustainability",  # DONE
    "Recovered Materials/Sustainability": "recovered_materials_sustainability_description",  # DONE
    "Domestic or Foreign Entity Code": "transaction__recipient__domestic_or_foreign_entity",
    "Domestic or Foreign Entity": "transaction__recipient__domestic_or_foreign_entity_description",
    "DoD Acquisition Program Code": "program_system_or_equipment_code",  # DONE
    # "DoD Acquisition Program Description": "dod_acquisition_program_description",  # TODO    we dont' have
    "Information Technology Commercial Item Category Code": "information_technology_commercial_item_category",  # DONE
    "Information Technology Commercial Item Category": "information_technology_commercial_item_category_description",  # DONE
    "EPA-Designated Product Code": "epa_designated_product",  # DONE
    "EPA-Designated Product": "epa_designated_product_description",  # DONE
    "Country of Product or Service Origin Code": "country_of_product_or_service_origin",  # TODO not quite right
    # "Country of Product or Service Origin": "country_of_product_or_service_origin",  # TODO not quite right   we don't have
    "Place of Manufacture Code": "place_of_manufacture",  # DONE
    "Place of Manufacture": "place_of_manufacture_description",  # DONE
    "Subcontracting Plan Code": "subcontracting_plan",  # DONE
    "Subcontracting Plan": "subcontracting_plan_description",  # DONE
    "Extent Competed Code": "extent_competed",  # DONE
    "Extent Competed": "extent_competed_description",  # DONE
    "Solicitation Procedures Code": "solicitation_procedures",  # DONE
    "Solicitation Procedures": "solicitation_procedures_description",  # DONE
    "Type of Set Aside Code": "type_set_aside",  # DONE
    "Type of Set Aside": "type_set_aside_description",  # DONE
    "Evaluated Preference Code": "evaluated_preference",  # DONE
    "Evaluated Preference": "evaluated_preference_description",  # DONE
    "Research Code": "research",  # DONE
    "Research": "research_description",  # DONE
    "Fair Opportunity Limited Sources Code": "fair_opportunity_limited_sources",  # DONE
    "Fair Opportunity Limited Sources": "fair_opportunity_limited_sources_description",  # DONE
    "Other than Full and Open Competition Code": "other_than_full_and_open_competition",  # TODO: Code or description?
    # "Other than Full and Open Competition": "other_than_full_and_open_competition",  # TODO: Code or description?   we don't have
    "Number of Offers Received": "number_of_offers_received",  # DONE
    "Commercial Item Acquisition Procedures Code": "commercial_item_acquisition_procedures",  # DONE
    "Commercial Item Acquisition Procedures": "commercial_item_acquisition_procedures_description",  # DONE
    "Small Business Competitiveness Demonstration Program": "small_business_competitiveness_demonstration_program",  # DONE
    "Commercial Item Test Program Code": "commercial_item_test_program",
    # "Commercial Item Test Program": "commercial_item_test_program_description",  # TODO   we don't have
    "A-76 FAIR Act Action Code": "a76_fair_act_action",
    # "A-76 FAIR Act Action": "a76_fair_act_action_description",  # TODO   we don't have
    "FedBizOpps Code": "fed_biz_opps",  # DONE
    "FedBizOpps": "fed_biz_opps_description",  # DONE
    "Local Area Set Aside Code": "local_area_set_aside",
    # "Local Area Set Aside": "local_area_set_aside_description",  # TODO   we don't have
    "Price Evaluation Adjustment Preference Percent Difference": "price_evaluation_adjustment_preference_percent_difference",  # DONE
    "Clinger-Cohen Act Planning Compliance Code": "clinger_cohen_act_planning",
    # "Clinger-Cohen Act Planning Compliance": "clinger_cohen_act_planning_description",  # TODO   we don't have
    "Walsh Healey Act Code": "walsh_healey_act",
    # "Walsh Healey Act": "walsh_healey_act_description",  # TODO   we don't have
    "Service Contract Act Code": "service_contract_act",  # DONE
    "Service Contract Act": "service_contract_act_description",  # DONE
    "Davis Bacon Act Code": "davis_bacon_act",  # DONE
    "Davis Bacon Act": "davis_bacon_act_description",  # DONE
    "Interagency Contracting Authority Code": "interagency_contracting_authority",  # DONE
    "Interagency Contracting Authority": "interagency_contracting_authority_description",  # DONE
    "Other Statutory Authority": "other_statutory_authority",  # DONE
    "Program Acronym": "program_acronym",  # DONE
    # "Parent Award Type Code": "referenced_idv_type",
    # "Parent Award Type": "parent_award_type",  # TODO
    # "Parent Award Single or Multiple Code": "multiple_or_single_award_idv",
    # "Parent Award Single or Multiple": "multiple_or_single_award_idv_description",
    "Major Program": "major_program",  # DONE
    "National Interest Action Code": "national_interest_action",  # DONE
    "National Interest Action": "national_interest_action_description",  # DONE
    "Cost or Pricing Data Code": "cost_or_pricing_data",  # DONE
    "Cost or Pricing Data": "cost_or_pricing_data_description",  # DONE
    "Cost Accounting Standards Clause Code": "cost_accounting_standards",
    "Cost Accounting Standards Clause": "cost_accounting_standards_description",
    "GFE and GFP Code": "gfe_gfp",
    # "GFE and GFP": "gfe_gfp",  # TODO: Code or description?
    "Sea Transportation Code": "sea_transportation",  # DONE
    "Sea Transportation": "sea_transportation_description",  # DONE
    "Undefinitized Action Code": "transaction__recipient__undefinitized_action",
    # "Undefinitized Action": "undefinitized_action_description",  # TODO
    "Consolidated Contract Code": "consolidated_contract",
    # "Consolidated Contract": "consolidated_contract_description",  # TODO: Code or description?
    "Performance-Based Service Acquisition Code": "performance_based_service_acquisition",  # DONE
    "Performance-Based Service Acquisition": "performance_based_service_acquisition_description",  # DONE
    "Multi Year Contract Code": "multi_year_contract",
    # "Multi Year Contract": "multi_year_contract",  # TODO: Code or description?
    "Contract Financing Code": "contract_financing",  # DONE
    "Contract Financing": "contract_financing_description",  # DONE
    "Purchase Card as Payment Method Code": "purchase_card_as_payment_method",
    # "Purchase Card as Payment Method": "purchase_card_as_payment_method_description",  # TODO: Code or description?
    "Contingency Humanitarian or Peacekeeping Operation Code": "contingency_humanitarian_or_peacekeeping_operation",  # DONE
    "Contingency Humanitarian or Peacekeeping Operation": "contingency_humanitarian_or_peacekeeping_operation_description",  # DONE
    "Alaskan Native Owned Corporation or Firm": "transaction__recipient__alaskan_native_owned_corporation_or_firm",
    "American Indian Owned Business": "transaction__recipient__american_indian_owned_business",
    "Indian Tribe Federally Recognized": "transaction__recipient__indian_tribe_federally_recognized",
    "Native Hawaiian Owned Business": "transaction__recipient__native_hawaiian_owned_business",
    "Tribally Owned Business": "transaction__recipient__tribally_owned_business",
    "Veteran Owned Business": "transaction__recipient__veteran_owned_business",
    "Service Disabled Veteran Owned Business": "transaction__recipient__service_disabled_veteran_owned_business",
    "Woman Owned Business": "transaction__recipient__woman_owned_business",
    "Women Owned Small Business": "transaction__recipient__women_owned_small_business",
    "Economically Disadvantaged Women Owned Small Business": "transaction__recipient__economically_disadvantaged_women_owned_small_business",
    "Joint Venture Women Owned Small Business": "transaction__recipient__joint_venture_women_owned_small_business",
    "Joint Venture Economically Disadvantaged Women Owned Small Business": "transaction__recipient__joint_venture_economic_disadvantaged_women_owned_small_bus",
    "Minority Owned Business": "transaction__recipient__minority_owned_business",
    "Subcontinent Asian Asian - Indian American Owned Business": "transaction__recipient__subcontinent_asian_asian_indian_american_owned_business",
    "Asian Pacific American Owned Business": "transaction__recipient__asian_pacific_american_owned_business",
    "Black American Owned Business": "transaction__recipient__black_american_owned_business",
    "Hispanic American Owned Business": "transaction__recipient__hispanic_american_owned_business",
    "Native American Owned Business": "transaction__recipient__native_american_owned_business",
    "Other Minority Owned Business": "transaction__recipient__other_minority_owned_business",
    "Contracting Officer's Determination of Business Size": "contracting_officers_determination_of_business_size",
    # "Contracting Officer's Determination of Business Size Code": "contracting_officers_determination_of_business_size_code",
    "Emerging Small Business": "transaction__recipient__emerging_small_business",
    "Community Developed Corporation Owned Firm": "transaction__recipient__community_developed_corporation_owned_firm",
    "Labor Surplus Area Firm": "transaction__recipient__labor_surplus_area_firm",
    "U.S. Federal Government": "transaction__recipient__us_federal_government",  # DONE
    "Federally Funded Research and Development Corp": "transaction__recipient__federally_funded_research_and_development_corp",
    "Federal Agency": "transaction__recipient__federal_agency",  # DONE
    "U.S. State Government": "transaction__recipient__us_state_government",  # DONE
    "U.S. Local Government": "transaction__recipient__us_local_government",  # DONE
    "City Local Government": "transaction__recipient__city_local_government",  # DONE
    "County Local Government": "transaction__recipient__county_local_government",  # DONE
    "Inter-Municipal Local Government": "transaction__recipient__inter_municipal_local_government",  # DONE
    "Local Government Owned": "transaction__recipient__local_government_owned",  # DONE
    "Municipality Local Government": "transaction__recipient__municipality_local_government",  # DONE
    "School District Local Government": "transaction__recipient__school_district_local_government",  # DONE
    "Township Local Government": "transaction__recipient__township_local_government",  # DONE
    "U.S. Tribal Government": "transaction__recipient__us_tribal_government",  # DONE
    "Foreign Government": "transaction__recipient__foreign_government",  # DONE
    "Corporate Entity Not Tax Exempt": "transaction__recipient__corporate_entity_not_tax_exempt",  # DONE
    "Corporate Entity Tax Exempt": "transaction__recipient__corporate_entity_tax_exempt",  # DONE
    "Partnership or Limited Liability Partnership": "transaction__recipient__partnership_or_limited_liability_partnership",  # DONE
    "Sole Proprietorship": "transaction__recipient__sole_proprietorship",  # DONE
    "Small Agricultural Cooperative": "transaction__recipient__small_agricultural_cooperative",  # DONE
    "International Organization": "transaction__recipient__international_organization",  # DONE
    "U.S. Government Entity": "transaction__recipient__us_government_entity",  # DONE
    "Community Development Corporation": "transaction__recipient__community_development_corporation",  # DONE
    "Domestic Shelter": "transaction__recipient__domestic_shelter",  # DONE
    "Educational Institution": "transaction__recipient__educational_institution",  # DONE
    "Foundation": "transaction__recipient__foundation",  # DONE
    "Hospital Flag": "transaction__recipient__hospital_flag",  # DONE
    "Manufacturer of Goods": "transaction__recipient__manufacturer_of_goods",  # DONE
    "Veterinary Hospital": "transaction__recipient__veterinary_hospital",  # DONE
    "Hispanic Servicing Institution": "transaction__recipient__hispanic_servicing_institution",  # DONE
    # "Receives Contracts": "receives_contracts",  # TODO receives_contracts_and_grants
    # "Receives Grants": "receives_grants",  # TODO receives_contracts_and_grants
    "Receives Contracts and Grants": "transaction__recipient__receives_contracts_and_grants",  # DONE
    "Airport Authority": "transaction__recipient__airport_authority",  # DONE
    "Council of Governments": "transaction__recipient__council_of_governments",  # DONE
    "Housing Authorities Public/Tribal": "transaction__recipient__housing_authorities_public_tribal",  # DONE
    "Interstate Entity": "transaction__recipient__interstate_entity",  # DONE
    "Planning Commission": "transaction__recipient__planning_commission",  # DONE
    "Port Authority": "transaction__recipient__port_authority",  # DONE
    "Transit Authority": "transaction__recipient__transit_authority",  # DONE
    "Subchapter S Corporation": "transaction__recipient__subchapter_scorporation",  # DONE
    "Limited Liability Corporation": "transaction__recipient__limited_liability_corporation",  # DONE
    "Foreign Owned and Located": "transaction__recipient__foreign_owned_and_located",  # DONE
    "For Profit Organization": "transaction__recipient__for_profit_organization",  # DONE
    "Nonprofit Organization": "transaction__recipient__nonprofit_organization",  # DONE
    "Other Not For Profit Organization": "transaction__recipient__other_not_for_profit_organization",  # DONE
    "The AbilityOne Program": "transaction__recipient__the_ability_one_program",  # DONE
    "Private University or College ": "transaction__recipient__private_university_or_college",  # DONE
    "State Controlled Institution of Higher Learning": "transaction__recipient__state_controlled_institution_of_higher_learning",  # DONE
    "1862 Land Grant College": "transaction__recipient__c1862_land_grant_college",  # DONE
    "1890 Land Grant College": "transaction__recipient__c1890_land_grant_college",  # DONE
    "1994 Land Grant College": "transaction__recipient__c1994_land_grant_college",  # DONE
    "Minority Institution": "transaction__recipient__minority_institution",  # DONE
    "Historically Black College or University": "transaction__recipient__historically_black_college",  # DONE
    "Tribal College": "transaction__recipient__tribal_college",  # DONE
    "Alaskan Native Servicing Institution": "transaction__recipient__alaskan_native_servicing_institution",  # DONE
    "Native Hawaiian Servicing Institution": "transaction__recipient__native_hawaiian_servicing_institution",  # DONE
    "School of Forestry": "transaction__recipient__school_of_forestry",  # DONE
    "Veterinary College": "transaction__recipient__veterinary_college",  # DONE
    "DoT Certified Disadvantaged Business Enterprise": "transaction__recipient__dot_certified_disadvantage",  # DONE
    "Self-Certified Small Disadvantaged Business": "transaction__recipient__self_certified_small_disadvantaged_business",  # DONE
    "Small Disadvantaged Business": "transaction__recipient__small_disadvantaged_business",  # DONE
    "8a Program Participant": "transaction__recipient__c8a_program_participant",  # DONE
    "Historically Underutilized Business Zone HUBZone Firm": "transaction__recipient__historically_underutilized_business_zone",
    "SBA Certified 8a Joint Venture": "transaction__recipient__sba_certified_8a_joint_venture",  # DONE
    "Last Modified Date": "last_modified_date",  # DONE

    'Non Federal Funding Amount': 'non_federal_funding_amount',
    'Primary Place of Performance County Code': 'transaction__place_of_performance__county_code',
    'SAI Number': 'sai_number',
    'Total Funding Amount': 'total_funding_amount',
    'URI': 'uri',
}



query_paths = {
    'transaction': {'d1': bidict(), 'd2': bidict()},
    'award': {'d1': bidict(), 'd2': bidict()}
}

# Other query paths change when they are queried from Award rather than Transaction
for (human_name, query_path) in all_query_paths.items():
    for (txn_source, detail_path) in (('d1', 'contract'), ('d2', 'assistance')):
        query_paths['transaction'][txn_source][human_name] = query_path

# a few fields should be reported for both contracts and assistance,
# but differently
query_paths['transaction']['d2']['Period of Performance Current End Date'] = 'period_of_performance_current_end_date'
query_paths['transaction']['d2']['Period of Performance Start Date'] = 'period_of_performance_start_date'

# These fields have the same paths from either the transaction or the award
unmodified_path_for_award = ('last_modified_date', 'period_of_performance_start_date', 'period_of_performance_current_end_date' )

# Other query paths change when they are queried from Award rather than Transaction
for (human_name, query_path) in all_query_paths.items():
    for (txn_source, detail_path) in (('d1', 'contract'), ('d2', 'assistance')):
        if query_path in unmodified_path_for_award:
            path_from_award = query_path
        elif query_path.startswith('transaction__award__'):
            path_from_award = query_path[len('transaction__award__'):]
        elif query_path.startswith('transaction__recipient__') or query_path.startswith('transaction__place_of_performance__'):
            # recipient and PoP are directly attached to Award
            path_from_award = query_path[len('transaction__'):]
        else:
            # Navigate to latest child of transaction (contract or assistance)
            path_from_award = 'latest_transaction__{}_data__{}'.format(detail_path, query_path)
        query_paths['award'][txn_source][human_name] = path_from_award


# now prune out those not called for, by human name

human_names = {
'award': {'d1': ['Award ID',
                  'Parent Award Agency ID',
                  'Parent Award Agency Name',
                  'Parent Award ID',
                  'Period of Performance Start Date',
                  'Period of Performance Current End Date',
                  'Period of Performance Potential End Date',
                  'Ordering Period End Date',
                  'Awarding Agency Code',
                  'Awarding Agency Name',
                  'Awarding Sub Agency Code',
                  'Awarding Sub Agency Name',
                  'Awarding Office Code',
                  'Awarding Office Name',
                  'Funding Agency Code',
                  'Funding Agency Name',
                  'Funding Sub Agency Code',
                  'Funding Sub Agency Name',
                  'Funding Office Code',
                  'Funding Office Name',
                  'Foreign Funding',
                  'SAM Exception',
                  'Recipient DUNS',
                  'Recipient Name',
                  'Recipient Doing Business As Name',
                  'Recipient Parent Name',
                  'Recipient Parent DUNS',
                  'Recipient Country Code',
                  'Recipient Country Name',
                  'Recipient Address Line 1',
                  'Recipient Address Line 2',
                  'Recipient Address Line 3',
                  'Recipient City Name',
                  'Recipient State Code',
                  'Recipient State Name',
                  'Recipient Foreign State Name',
                  'Recipient Zip+4 Code',
                  'Recipient Congressional District',
                  'Recipient Phone Number',
                  'Recipient Fax Number',
                  'Primary Place of Performance Country Code',
                  'Primary Place of Performance Country Name',
                  'Primary Place of Performance City Name',
                  'Primary Place of Performance County Name',
                  'Primary Place of Performance State Code',
                  'Primary Place of Performance State Name',
                  'Primary Place of Performance Zip+4',
                  'Primary Place of Performance Congressional District',
                  'Primary Place of Performance Location Code',
                  'Award or Parent Award Flag',
                  'Award Type Code',
                  'Award Type',
                  'IDV Type Code',
                  'IDV Type',
                  'Multiple or Single Award IDV Code',
                  'Multiple or Single Award IDV',
                  'Type of IDC Code',
                  'Type of IDC',
                  'Type of Contract Pricing Code',
                  'Type of Contract Pricing',
                  'Award Description',
                  'Solicitation Identifier',
                  'Number of Actions',
                  'Product or Service Code (PSC)',
                  'Product or Service Code (PSC) Description',
                  'Contract Bundling Code',
                  'Contract Bundling',
                  'DoD Claimant Program Code',
                  'DoD Claimant Program Description',
                  'NAICS Code',
                  'NAICS Description',
                  'Recovered Materials/Sustainability Code',
                  'Recovered Materials/Sustainability',
                  'Domestic or Foreign Entity Code',
                  'Domestic or Foreign Entity',
                  'DoD Acquisition Program Code',
                  'DoD Acquisition Program Description',
                  'Information Technology Commercial Item Category Code',
                  'Information Technology Commercial Item Category',
                  'EPA-Designated Product Code',
                  'EPA-Designated Product',
                  'Country of Product or Service Origin Code',
                  'Country of Product or Service Origin',
                  'Place of Manufacture Code',
                  'Place of Manufacture',
                  'Subcontracting Plan Code',
                  'Subcontracting Plan',
                  'Extent Competed Code',
                  'Extent Competed',
                  'Solicitation Procedures Code',
                  'Solicitation Procedures',
                  'Type of Set Aside Code',
                  'Type of Set Aside',
                  'Evaluated Preference Code',
                  'Evaluated Preference',
                  'Research Code',
                  'Research',
                  'Fair Opportunity Limited Sources Code',
                  'Fair Opportunity Limited Sources',
                  'Other than Full and Open Competition Code',
                  'Other than Full and Open Competition',
                  'Number of Offers Received',
                  'Commercial Item Acquisition Procedures Code',
                  'Commercial Item Acquisition Procedures',
                  'Small Business\nCompetitiveness Demonstration Program',
                  'Commercial Item Test Program Code',
                  'Commercial Item Test Program',
                  'A-76 FAIR Act Action Code',
                  'A-76 FAIR Act Action',
                  'FedBizOpps Code',
                  'FedBizOpps',
                  'Local Area Set Aside Code',
                  'Local Area Set Aside',
                  'Clinger-Cohen Act Planning\n  Compliance Code',
                  'Clinger-Cohen Act Planning\n  Compliance',
                  'Walsh Healey Act Code',
                  'Walsh Healey Act',
                  'Service Contract Act Code',
                  'Service Contract Act',
                  'Davis Bacon Act Code',
                  'Davis Bacon Act',
                  'Interagency Contracting\nAuthority Code',
                  'Interagency Contracting\nAuthority',
                  'Other Statutory Authority',
                  'Program Acronym',
                  'Parent Award Type Code',
                  'Parent Award Type',
                  'Parent Award Single or Multiple Code',
                  'Parent Award Single or Multiple',
                  'Major Program',
                  'National Interest Action Code',
                  'National Interest Action',
                  'Cost or Pricing Data Code',
                  'Cost or Pricing Data',
                  'Cost Accounting Standards Clause Code',
                  'Cost Accounting Standards Clause',
                  'GFE and GFP Code',
                  'GFE and GFP',
                  'Sea Transportation Code',
                  'Sea Transportation',
                  'Consolidated Contract Code',
                  'Consolidated Contract',
                  'Performance-Based Service Acquisition Code',
                  'Performance-Based Service Acquisition',
                  'Multi Year Contract Code',
                  'Multi Year Contract',
                  'Contract Financing Code',
                  'Contract Financing',
                  'Purchase Card as Payment Method Code',
                  'Purchase Card as Payment Method',
                  'Contingency Humanitarian or Peacekeeping Operation Code',
                  'Contingency Humanitarian or Peacekeeping Operation',
                  'Alaskan Native Owned\nCorporation or Firm',
                  'American Indian Owned Business',
                  'Indian Tribe Federally Recognized',
                  'Native Hawaiian Owned Business',
                  'Tribally Owned Business',
                  'Veteran Owned Business',
                  'Service Disabled Veteran Owned Business',
                  'Woman Owned Business',
                  'Women Owned Small Business',
                  'Economically Disadvantaged Women Owned Small Business',
                  'Joint Venture Women Owned Small Business',
                  'Joint Venture Economically Disadvantaged Women Owned Small '
                  'Business',
                  'Minority Owned Business',
                  'Subcontinent Asian Asian - Indian American Owned Business',
                  'Asian Pacific American Owned Business',
                  'Black American Owned Business',
                  'Hispanic American Owned Business',
                  'Native American Owned\nBusiness',
                  'Other Minority Owned Business',
                  "Contracting Officer's Determination of Business Size",
                  "Contracting Officer's Determination of Business Size Code",
                  'Emerging Small Business',
                  'Community Developed Corporation Owned Firm',
                  'Labor Surplus Area Firm',
                  'U.S. Federal Government',
                  'Federally Funded Research and Development Corp',
                  'Federal Agency',
                  'U.S. State Government',
                  'U.S. Local Government',
                  'City Local Government',
                  'County Local Government',
                  'Inter-Municipal Local Government',
                  'Local Government Owned',
                  'Municipality Local Government',
                  'School District Local\nGovernment',
                  'Township Local Government',
                  'U.S. Tribal Government',
                  'Foreign Government',
                  'Corporate Entity Not Tax Exempt',
                  'Corporate Entity Tax Exempt',
                  'Partnership or Limited Liability Partnership',
                  'Sole Proprietorship',
                  'Small Agricultural\nCooperative',
                  'International Organization',
                  'U.S. Government Entity',
                  'Community Development Corporation',
                  'Domestic Shelter',
                  'Educational Institution',
                  'Foundation',
                  'Hospital Flag',
                  'Manufacturer of Goods',
                  'Veterinary Hospital',
                  'Hispanic Servicing\nInstitution',
                  'Receives Contracts',
                  'Receives Grants',
                  'Receives Contracts and Grants',
                  'Airport Authority',
                  'Council of Governments',
                  'Housing Authorities\nPublic/Tribal',
                  'Interstate Entity',
                  'Planning Commission',
                  'Port Authority',
                  'Transit Authority',
                  'Subchapter S Corporation',
                  'Limited Liability Corporation',
                  'Foreign Owned and Located',
                  'For Profit Organization',
                  'Nonprofit Organization',
                  'Other Not For Profit\nOrganization',
                  'The AbilityOne Program',
                  'Private University or College\xa0',
                  'State Controlled Institution\nof Higher Learning',
                  '1862 Land Grant College',
                  '1890 Land Grant College',
                  '1994 Land Grant College',
                  'Minority Institution',
                  'Historically Black College or University',
                  'Tribal College',
                  'Native Hawaiian Servicing  Institution',
                  'School of Forestry',
                  'Veterinary College',
                  'DoT Certified Disadvantaged Business Enterprise',
                  'Self-Certified Small Disadvantaged Business',
                  'Small Disadvantaged Business',
                  '8a Program Participant',
                  'Historically Underutilized\nBusiness Zone HUBZone Firm',
                  'SBA Certified 8a Joint Venture',
                  'Last Modified Date'],
           'd2': ['Award ID',
                  'URI',
                  'SAI Number',
                  'Federal Action Obligation',
                  'Non Federal Funding Amount',
                  'Total Funding Amount',
                  'Face Value of Loan',
                  'Original Subsidy Cost',
                  'Period of Performance Start Date',
                  'Period of Performance Current End Date',
                  'Awarding Agency Code',
                  'Awarding Agency Name',
                  'Awarding Sub Agency Code',
                  'Awarding Sub Agency Name',
                  'Awarding Office Code',
                  'Awarding Office Name',
                  'Funding Agency Code',
                  'Funding Agency Name',
                  'Funding Sub Agency Code',
                  'Funding Sub Agency Name',
                  'Funding Office Code',
                  'Funding Office Name',
                  'Recipient DUNS',
                  'Recipient Name',
                  'Recipient Country Code',
                  'Recipient Country Name',
                  'Recipient Address Line 1',
                  'Recipient Address Line 2',
                  'Recipient Address Line 3',
                  'Recipient City Code',
                  'Recipient City Name',
                  'Recipient Country Code',
                  'Recipient Country Name',
                  'Recipient State Code',
                  'Recipient State Name',
                  'Recipient Zip Code',
                  'Recipient Zip Last 4 Code',
                  'Recipient Congressional District',
                  'Recipient Foreign City Name',
                  'Recipient Foreign Province Name',
                  'Recipient Foreign Postal Code',
                  'Primary Place of Performance Country Code',
                  'Primary Place of Performance Country Name',
                  'Primary Place of Performance Code',
                  'Primary Place of Performance City Name',
                  'Primary Place of Performance County Code',
                  'Primary Place of Performance County Name',
                  'Primary Place of Performance State Name',
                  'Primary Place of Performance Zip+4',
                  'Primary Place of Performance Congressional District',
                  'Primary Place of Performance Foreign Location',
                  'CFDA Number',
                  'CFDA Title',
                  'Assistance Type Code',
                  'Assistance Type',
                  'Award Description',
                  'Business Funds Indicator Code',
                  'Business Funds Indicator',
                  'Business Types Code',
                  'Business Types',
                  'Record Type Code',
                  'Record Type',
                  'Last Modified Date']},
 'transaction': {'d1': ['Award ID',
                        'Modification Number',
                        'Transaction Number',
                        'Parent Award Agency ID',
                        'Parent Award Agency Name',
                        'Parent Award ID',
                        'Parent Award Modification Number',
                        'Federal Action Obligation',
                        'Change in Current Award Amount',
                        'Change in Potential Award Amount',
                        'Action Date',
                        'Period of Performance Start Date',
                        'Period of Performance Current End Date',
                        'Period of Performance Potential End Date',
                        'Ordering Period End Date',
                        'Awarding Agency Code',
                        'Awarding Agency Name',
                        'Awarding Sub Agency Code',
                        'Awarding Sub Agency Name',
                        'Awarding Office Code',
                        'Awarding Office Name',
                        'Funding Agency Code',
                        'Funding Agency Name',
                        'Funding Sub Agency Code',
                        'Funding Sub Agency Name',
                        'Funding Office Code',
                        'Funding Office Name',
                        'Foreign Funding',
                        'SAM Exception',
                        'Recipient DUNS',
                        'Recipient Name',
                        'Recipient Doing Business As Name',
                        'Recipient Parent Name',
                        'Recipient Parent DUNS',
                        'Recipient Country Code',
                        'Recipient Country Name',
                        'Recipient Address Line 1',
                        'Recipient Address Line 2',
                        'Recipient Address Line 3',
                        'Recipient City Name',
                        'Recipient State Code',
                        'Recipient State Name',
                        'Recipient Foreign State Name',
                        'Recipient Zip+4 Code',
                        'Recipient Congressional District',
                        'Recipient Phone Number',
                        'Recipient Fax Number',
                        'Primary Place of Performance Country Code',
                        'Primary Place of Performance Country Name',
                        'Primary Place of Performance City Name',
                        'Primary Place of Performance County Name',
                        'Primary Place of Performance State Code',
                        'Primary Place of Performance State Name',
                        'Primary Place of Performance Zip+4',
                        'Primary Place of Performance Congressional District',
                        'Primary Place of Performance Location Code',
                        'Award or Parent Award Flag',
                        'Award Type Code',
                        'Award Type',
                        'IDV Type Code',
                        'IDV Type',
                        'Multiple or Single Award IDV Code',
                        'Multiple or Single Award IDV',
                        'Type of IDC Code',
                        'Type of IDC',
                        'Type of Contract Pricing Code',
                        'Type of Contract Pricing',
                        'Award Description',
                        'Action Type Code',
                        'Action Type',
                        'Solicitation Identifier',
                        'Number of Actions',
                        'Product or Service Code (PSC)',
                        'Product or Service Code (PSC) Description',
                        'Contract Bundling Code',
                        'Contract Bundling',
                        'DoD Claimant Program Code',
                        'DoD Claimant Program Description',
                        'NAICS Code',
                        'NAICS Description',
                        'Recovered Materials/Sustainability Code',
                        'Recovered Materials/Sustainability',
                        'Domestic or Foreign Entity Code',
                        'Domestic or Foreign Entity',
                        'DoD Acquisition Program Code',
                        'DoD Acquisition Program Description',
                        'Information Technology Commercial Item Category Code',
                        'Information Technology Commercial Item Category',
                        'EPA-Designated Product Code',
                        'EPA-Designated Product',
                        'Country of Product or Service Origin Code',
                        'Country of Product or Service Origin',
                        'Place of Manufacture Code',
                        'Place of Manufacture',
                        'Subcontracting Plan Code',
                        'Subcontracting Plan',
                        'Extent Competed Code',
                        'Extent Competed',
                        'Solicitation Procedures Code',
                        'Solicitation Procedures',
                        'Type of Set Aside Code',
                        'Type of Set Aside',
                        'Evaluated Preference Code',
                        'Evaluated Preference',
                        'Research Code',
                        'Research',
                        'Fair Opportunity Limited Sources Code',
                        'Fair Opportunity Limited Sources',
                        'Other than Full and Open Competition Code',
                        'Other than Full and Open Competition',
                        'Number of Offers Received',
                        'Commercial Item Acquisition Procedures Code',
                        'Commercial Item Acquisition Procedures',
                        'Small Business\nCompetitiveness Demonstration Program',
                        'Commercial Item Test Program Code',
                        'Commercial Item Test Program',
                        'A-76 FAIR Act Action Code',
                        'A-76 FAIR Act Action',
                        'FedBizOpps Code',
                        'FedBizOpps',
                        'Local Area Set Aside Code',
                        'Local Area Set Aside',
                        'Price Evaluation Adjustment Preference Percent '
                        'Difference',
                        'Clinger-Cohen Act Planning\n  Compliance Code',
                        'Clinger-Cohen Act Planning\n  Compliance',
                        'Walsh Healey Act Code',
                        'Walsh Healey Act',
                        'Service Contract Act Code',
                        'Service Contract Act',
                        'Davis Bacon Act Code',
                        'Davis Bacon Act',
                        'Interagency Contracting\nAuthority Code',
                        'Interagency Contracting\nAuthority',
                        'Other Statutory Authority',
                        'Program Acronym',
                        'Parent Award Type Code',
                        'Parent Award Type',
                        'Parent Award Single or Multiple Code',
                        'Parent Award Single or Multiple',
                        'Major Program',
                        'National Interest Action Code',
                        'National Interest Action',
                        'Cost or Pricing Data Code',
                        'Cost or Pricing Data',
                        'Cost Accounting Standards Clause Code',
                        'Cost Accounting Standards Clause',
                        'GFE and GFP Code',
                        'GFE and GFP',
                        'Sea Transportation Code',
                        'Sea Transportation',
                        'Undefinitized Action Code',
                        'Undefinitized Action',
                        'Consolidated Contract Code',
                        'Consolidated Contract',
                        'Performance-Based Service Acquisition Code',
                        'Performance-Based Service Acquisition',
                        'Multi Year Contract Code',
                        'Multi Year Contract',
                        'Contract Financing Code',
                        'Contract Financing',
                        'Purchase Card as Payment Method Code',
                        'Purchase Card as Payment Method',
                        'Contingency Humanitarian or Peacekeeping Operation '
                        'Code',
                        'Contingency Humanitarian or Peacekeeping Operation',
                        'Alaskan Native Owned\nCorporation or Firm',
                        'American Indian Owned Business',
                        'Indian Tribe Federally Recognized',
                        'Native Hawaiian Owned Business',
                        'Tribally Owned Business',
                        'Veteran Owned Business',
                        'Service Disabled Veteran Owned Business',
                        'Woman Owned Business',
                        'Women Owned Small Business',
                        'Economically Disadvantaged Women Owned Small Business',
                        'Joint Venture Women Owned Small Business',
                        'Joint Venture Economically Disadvantaged Women Owned '
                        'Small Business',
                        'Minority Owned Business',
                        'Subcontinent Asian Asian - Indian American Owned '
                        'Business',
                        'Asian Pacific American Owned Business',
                        'Black American Owned Business',
                        'Hispanic American Owned Business',
                        'Native American Owned\nBusiness',
                        'Other Minority Owned Business',
                        "Contracting Officer's Determination of Business Size",
                        "Contracting Officer's Determination of Business Size "
                        'Code',
                        'Emerging Small Business',
                        'Community Developed Corporation Owned Firm',
                        'Labor Surplus Area Firm',
                        'U.S. Federal Government',
                        'Federally Funded Research and Development Corp',
                        'Federal Agency',
                        'U.S. State Government',
                        'U.S. Local Government',
                        'City Local Government',
                        'County Local Government',
                        'Inter-Municipal Local Government',
                        'Local Government Owned',
                        'Municipality Local Government',
                        'School District Local\nGovernment',
                        'Township Local Government',
                        'U.S. Tribal Government',
                        'Foreign Government',
                        'Corporate Entity Not Tax Exempt',
                        'Corporate Entity Tax Exempt',
                        'Partnership or Limited Liability Partnership',
                        'Sole Proprietorship',
                        'Small Agricultural\nCooperative',
                        'International Organization',
                        'U.S. Government Entity',
                        'Community Development Corporation',
                        'Domestic Shelter',
                        'Educational Institution',
                        'Foundation',
                        'Hospital Flag',
                        'Manufacturer of Goods',
                        'Veterinary Hospital',
                        'Hispanic Servicing\nInstitution',
                        'Receives Contracts',
                        'Receives Grants',
                        'Receives Contracts and Grants',
                        'Airport Authority',
                        'Council of Governments',
                        'Housing Authorities\nPublic/Tribal',
                        'Interstate Entity',
                        'Planning Commission',
                        'Port Authority',
                        'Transit Authority',
                        'Subchapter S Corporation',
                        'Limited Liability Corporation',
                        'Foreign Owned and Located',
                        'For Profit Organization',
                        'Nonprofit Organization',
                        'Other Not For Profit\nOrganization',
                        'The AbilityOne Program',
                        'Private University or College\xa0',
                        'State Controlled Institution\nof Higher Learning',
                        '1862 Land Grant College',
                        '1890 Land Grant College',
                        '1994 Land Grant College',
                        'Minority Institution',
                        'Historically Black College or University',
                        'Tribal College',
                        'Native Hawaiian Servicing  Institution',
                        'School of Forestry',
                        'Veterinary College',
                        'DoT Certified Disadvantaged Business Enterprise',
                        'Self-Certified Small Disadvantaged Business',
                        'Small Disadvantaged Business',
                        '8a Program Participant',
                        'Historically Underutilized\n'
                        'Business Zone HUBZone Firm',
                        'SBA Certified 8a Joint Venture',
                        'Last Modified Date'],
                 'd2': ['Award ID',
                        'Modification Number',
                        'URI',
                        'SAI Number',
                        'Federal Action Obligation',
                        'Non Federal Funding Amount',
                        'Total Funding Amount',
                        'Face Value of Loan',
                        'Original Subsidy Cost',
                        'Action Date',
                        'Period of Performance Start Date',
                        'Period of Performance Current End Date',
                        'Awarding Agency Code',
                        'Awarding Agency Name',
                        'Awarding Sub Agency Code',
                        'Awarding Sub Agency Name',
                        'Awarding Office Code',
                        'Awarding Office Name',
                        'Funding Agency Code',
                        'Funding Agency Name',
                        'Funding Sub Agency Code',
                        'Funding Sub Agency Name',
                        'Funding Office Code',
                        'Funding Office Name',
                        'Recipient DUNS',
                        'Recipient Name',
                        'Recipient Country Code',
                        'Recipient Country Name',
                        'Recipient Address Line 1',
                        'Recipient Address Line 2',
                        'Recipient Address Line 3',
                        'Recipient City Code',
                        'Recipient City Name',
                        'Recipient Country Code',
                        'Recipient Country Name',
                        'Recipient State Code',
                        'Recipient State Name',
                        'Recipient Zip Code',
                        'Recipient Zip Last 4 Code',
                        'Recipient Congressional District',
                        'Recipient Foreign City Name',
                        'Recipient Foreign Province Name',
                        'Recipient Foreign Postal Code',
                        'Primary Place of Performance Country Code',
                        'Primary Place of Performance Country Name',
                        'Primary Place of Performance Code',
                        'Primary Place of Performance City Name',
                        'Primary Place of Performance County Code',
                        'Primary Place of Performance County Name',
                        'Primary Place of Performance State Name',
                        'Primary Place of Performance Zip+4',
                        'Primary Place of Performance Congressional District',
                        'Primary Place of Performance Foreign Location',
                        'CFDA Number',
                        'CFDA Title',
                        'Assistance Type Code',
                        'Assistance Type',
                        'Award Description',
                        'Business Funds Indicator Code',
                        'Business Funds Indicator',
                        'Business Types Code',
                        'Business Types',
                        'Action Type Code',
                        'Action Type',
                        'Record Type Code',
                        'Record Type',
                        'Fiscal Year and Quarter Correction',
                        'Last Modified Date']}}


# this leaves us with a bunch of paths not actually in use for each type
# prune those out now

for model_type in ('transaction', 'award'):
    for file_type in ('d1', 'd2'):
        query_paths[model_type][file_type] = {
            k: v for (k, v) in query_paths[model_type][file_type].items()
                 if k in human_names[model_type][file_type]}

        missing = set(human_names[model_type][file_type]) - set(query_paths[model_type][file_type])
        if missing:
            msg = 'missing query paths: {}'.format(missing)
            # raise KeyError(msg)
            print
