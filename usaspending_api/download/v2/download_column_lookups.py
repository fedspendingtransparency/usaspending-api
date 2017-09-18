from bidict import bidict

from django.db.models.functions import Coalesce

from usaspending_api.awards.models import (Award, TransactionAssistance,
                                           TransactionContract)

all_query_paths = {
    "Modification Number": "transaction__modification_number",
    "Transaction Number": "transaction_number",
    "Parent Award Agency ID": "referenced_idv_agency_identifier",
    # "Parent Award Agency Name": "transaction__award__parent_award__name",  # TODO  # not in spreadsheet
    "Parent Award Modification Number": "referenced_idv_modification_number",
    "Federal Action Obligation": "transaction__federal_action_obligation",
    # "Change in Current Award Amount": "base_exercised_options_val",
    # "Change in Potential Award Amount": "base_and_all_options_value",
    "Action Date": "transaction__action_date",
    "Period of Performance Start Date":
    "transaction__period_of_performance_start_date",
    "Period of Performance Current End Date":
    "transaction__period_of_performance_current_end_date",
    "Period of Performance Potential End Date":
    "period_of_performance_potential_end_date",
    "Ordering Period End Date": "ordering_period_end_date",
    "Awarding Agency Code":
    "transaction__award__awarding_agency__toptier_agency__cgac_code",
    "Awarding Agency Name":
    "transaction__award__awarding_agency__toptier_agency__name",
    "Awarding Sub Agency Code":
    "transaction__award__awarding_agency__subtier_agency__subtier_code",
    "Awarding Sub Agency Name":
    "transaction__award__awarding_agency__subtier_agency__name",
    "Awarding Office Code":
    "transaction__award__awarding_agency__office_agency__aac_code",
    "Awarding Office Name":
    "transaction__award__awarding_agency__office_agency__name",
    "Funding Agency Code":
    "transaction__award__funding_agency__toptier_agency__cgac_code",
    "Funding Agency Name":
    "transaction__award__funding_agency__toptier_agency__name",
    "Funding Sub Agency Code":
    "transaction__award__funding_agency__subtier_agency__subtier_code",
    "Funding Sub Agency Name":
    "transaction__award__funding_agency__subtier_agency__name",
    "Funding Office Code":
    "transaction__award__funding_agency__office_agency__aac_code",
    "Funding Office Name":
    "transaction__award__funding_agency__office_agency__name",
    "Foreign Funding Code": "foreign_funding",
    "Foreign Funding": "foreign_funding_description",
    "SAM Exception": "transaction__recipient__sam_exception",
    "Recipient DUNS": "transaction__recipient__recipient_unique_id",
    "Recipient Name": "transaction__recipient__recipient_name",
    "Recipient Doing Business As Name":
    "transaction__recipient__vendor_doing_as_business_name",
    # "Recipient Parent DUNS": "recipient_parent_name",  # Goes to ultimate_parent_name, then nowhere?
    "Recipient Parent Name":
    "transaction__recipient__parent_recipient_unique_id",  # TODO: We end up loading this as a name
    "Recipient Country Code":
    "transaction__recipient__location__location_country_code",
    "Recipient Country Name": "transaction__recipient__location__country_name",
    "Recipient Address Line 1":
    "transaction__recipient__location__address_line1",
    "Recipient Address Line 2":
    "transaction__recipient__location__address_line2",
    "Recipient Address Line 3":
    "transaction__recipient__location__address_line3",
    "Recipient City Name": "transaction__recipient__location__city_name",
    "Recipient State Code": "transaction__recipient__location__state_code",
    "Recipient State Name": "transaction__recipient__location__state_name",
    "Recipient Foreign State Name":
    "transaction__recipient__location__foreign_province",  # TODO probably right
    "Recipient Zip+4 Code":
    "transaction__recipient__location__zip4",  # Confirmed, zip4 to zip4 in settings.py
    "Recipient Congressional District":
    "transaction__recipient__location__congressional_code",  # TODO probably right
    "Recipient Phone Number": "transaction__recipient__vendor_phone_number",
    "Recipient Fax Number": "transaction__recipient__vendor_fax_number",
    "Primary Place of Performance City Name":
    "transaction__place_of_performance__city_name",
    "Primary Place of Performance County Name":
    "transaction__place_of_performance__county_name",
    "Primary Place of Performance State Code":
    "transaction__place_of_performance__state_code",
    "Primary Place of Performance State Name":
    "transaction__place_of_performance__state_name",
    "Primary Place of Performance Zip+4":
    "transaction__place_of_performance__zip4",
    "Primary Place of Performance Congressional District":
    "transaction__place_of_performance__congressional_code",  # TODO probably right
    "Primary Place of Performance Country Code":
    "transaction__place_of_performance__location_country_code",
    "Primary Place of Performance Country Name":
    "transaction__place_of_performance__country_name",
    # "Primary Place of Performance Location Code": "transaction__place_of_performance__location_code",  # Ross says: field is worthless... it was deprecated years ago in FPDS
    # "Award or Parent Award Flag": "award_or_parent_award_flag",  # Ross says: this is a field i had alisa add to indicate whether the pull is from the award or idv feed.  you'll have to ask her what she mapped it to
    "Award Type Code": "transaction__award__type",
    "Award Type": "transaction__award__type_description",
    "IDV Type Code": "idv_type",
    "IDV Type": "idv_type_description",
    "Multiple or Single Award IDV Code": "multiple_or_single_award_idv",
    "Multiple or Single Award IDV": "multiple_or_single_award_idv_description",
    "Type of IDC Code": "type_of_idc",
    "Type of IDC": "type_of_idc_description",
    "Type of Contract Pricing Code": "type_of_contract_pricing",
    "Type of Contract Pricing": "type_of_contract_pricing_description",
    "Award Description": "transaction__award__description",
    "Action Type Code": "transaction__action_type",
    "Action Type": "transaction__action_type_description",
    "Solicitation Identifier": "solicitation_identifier",
    "Number of Actions": "number_of_actions",
    "Product or Service Code (PSC)": "product_or_service_code",
    # "Product or Service Code (PSC) Description": "product_or_service_code_description",  # TODO   we don't have
    "Contract Bundling Code": "contract_bundling",
    "Contract Bundling": "contract_bundling_description",
    "DoD Claimant Program Code": "dod_claimant_program_code",
    # "DoD Claimant Program Description": "dod_claimant_program_description",  # TODO   we don't have
    "NAICS Code": "naics",
    "NAICS Description": "naics_description",
    "Recovered Materials/Sustainability Code":
    "recovered_materials_sustainability",
    "Recovered Materials/Sustainability":
    "recovered_materials_sustainability_description",
    "Domestic or Foreign Entity Code":
    "transaction__recipient__domestic_or_foreign_entity",
    "Domestic or Foreign Entity":
    "transaction__recipient__domestic_or_foreign_entity_description",
    "DoD Acquisition Program Code": "program_system_or_equipment_code",
    # "DoD Acquisition Program Description": "dod_acquisition_program_description",  # TODO    we dont' have
    "Information Technology Commercial Item Category Code":
    "information_technology_commercial_item_category",
    "Information Technology Commercial Item Category":
    "information_technology_commercial_item_category_description",
    "EPA-Designated Product Code": "epa_designated_product",
    "EPA-Designated Product": "epa_designated_product_description",
    "Country of Product or Service Origin Code":
    "country_of_product_or_service_origin",  # TODO not quite right
    # "Country of Product or Service Origin": "country_of_product_or_service_origin",  # TODO not quite right   we don't have
    "Place of Manufacture Code": "place_of_manufacture",
    "Place of Manufacture": "place_of_manufacture_description",
    "Subcontracting Plan Code": "subcontracting_plan",
    "Subcontracting Plan": "subcontracting_plan_description",
    "Extent Competed Code": "extent_competed",
    "Extent Competed": "extent_competed_description",
    "Solicitation Procedures Code": "solicitation_procedures",
    "Solicitation Procedures": "solicitation_procedures_description",
    "Type of Set Aside Code": "type_set_aside",
    "Type of Set Aside": "type_set_aside_description",
    "Evaluated Preference Code": "evaluated_preference",
    "Evaluated Preference": "evaluated_preference_description",
    "Research Code": "research",
    "Research": "research_description",
    "Fair Opportunity Limited Sources Code":
    "fair_opportunity_limited_sources",
    "Fair Opportunity Limited Sources":
    "fair_opportunity_limited_sources_description",
    "Other than Full and Open Competition Code":
    "other_than_full_and_open_competition",
    # "Other than Full and Open Competition": "other_than_full_and_open_competition",  # TODO: we don't have
    "Number of Offers Received": "number_of_offers_received",
    "Commercial Item Acquisition Procedures Code":
    "commercial_item_acquisition_procedures",
    "Commercial Item Acquisition Procedures":
    "commercial_item_acquisition_procedures_description",
    "Small Business Competitiveness Demonstration Program":
    "small_business_competitiveness_demonstration_program",
    "Commercial Item Test Program Code": "commercial_item_test_program",
    # "Commercial Item Test Program": "commercial_item_test_program_description",  # TODO   we don't have
    "A-76 FAIR Act Action Code": "a76_fair_act_action",
    # "A-76 FAIR Act Action": "a76_fair_act_action_description",  # TODO   we don't have
    "FedBizOpps Code": "fed_biz_opps",
    "FedBizOpps": "fed_biz_opps_description",
    "Local Area Set Aside Code": "local_area_set_aside",
    # "Local Area Set Aside": "local_area_set_aside_description",  # TODO   we don't have
    "Price Evaluation Adjustment Preference Percent Difference":
    "price_evaluation_adjustment_preference_percent_difference",
    "Clinger-Cohen Act Planning Compliance Code": "clinger_cohen_act_planning",
    # "Clinger-Cohen Act Planning Compliance": "clinger_cohen_act_planning_description",  # TODO   we don't have
    "Walsh Healey Act Code": "walsh_healey_act",
    # "Walsh Healey Act": "walsh_healey_act_description",  # TODO   we don't have
    "Service Contract Act Code": "service_contract_act",
    "Service Contract Act": "service_contract_act_description",
    "Davis Bacon Act Code": "davis_bacon_act",
    "Davis Bacon Act": "davis_bacon_act_description",
    "Interagency Contracting Authority Code":
    "interagency_contracting_authority",
    "Interagency Contracting Authority":
    "interagency_contracting_authority_description",
    "Other Statutory Authority": "other_statutory_authority",
    "Program Acronym": "program_acronym",
    # "Parent Award Type Code": "referenced_idv_type",
    # "Parent Award Type": "parent_award_type",  # TODO - referenced_idv_type and referenced_idv_type_desc exist in broker db, but never in submission download
    # "Parent Award Single or Multiple Code": "multiple_or_single_award_idv",
    # "Parent Award Single or Multiple": "multiple_or_single_award_idv_description",  referenced_mult_or_single exists in db
    "Major Program": "major_program",
    "National Interest Action Code": "national_interest_action",
    "National Interest Action": "national_interest_action_description",
    "Cost or Pricing Data Code": "cost_or_pricing_data",
    "Cost or Pricing Data": "cost_or_pricing_data_description",
    "Cost Accounting Standards Clause Code": "cost_accounting_standards",
    "Cost Accounting Standards Clause":
    "cost_accounting_standards_description",
    "GFE and GFP Code": "gfe_gfp",
    # "GFE and GFP": "gfe_gfp",  # TODO: Code or description?
    "Sea Transportation Code": "sea_transportation",
    "Sea Transportation": "sea_transportation_description",
    "Undefinitized Action Code":
    "transaction__recipient__undefinitized_action",
    # "Undefinitized Action": "undefinitized_action_description",  # TODO
    "Consolidated Contract Code": "consolidated_contract",
    # "Consolidated Contract": "consolidated_contract_description",  # TODO: Code or description?
    "Performance-Based Service Acquisition Code":
    "performance_based_service_acquisition",
    "Performance-Based Service Acquisition":
    "performance_based_service_acquisition_description",
    "Multi Year Contract Code": "multi_year_contract",
    # "Multi Year Contract": "multi_year_contract",  # TODO: Code or description?
    "Contract Financing Code": "contract_financing",
    "Contract Financing": "contract_financing_description",
    "Purchase Card as Payment Method Code": "purchase_card_as_payment_method",
    # "Purchase Card as Payment Method": "purchase_card_as_payment_method_description",  # TODO: Code or description?
    "Contingency Humanitarian or Peacekeeping Operation Code":
    "contingency_humanitarian_or_peacekeeping_operation",
    "Contingency Humanitarian or Peacekeeping Operation":
    "contingency_humanitarian_or_peacekeeping_operation_description",
    "Alaskan Native Owned Corporation or Firm":
    "transaction__recipient__alaskan_native_owned_corporation_or_firm",
    "American Indian Owned Business":
    "transaction__recipient__american_indian_owned_business",
    "Indian Tribe Federally Recognized":
    "transaction__recipient__indian_tribe_federally_recognized",
    "Native Hawaiian Owned Business":
    "transaction__recipient__native_hawaiian_owned_business",
    "Tribally Owned Business":
    "transaction__recipient__tribally_owned_business",
    "Veteran Owned Business": "transaction__recipient__veteran_owned_business",
    "Service Disabled Veteran Owned Business":
    "transaction__recipient__service_disabled_veteran_owned_business",
    "Woman Owned Business": "transaction__recipient__woman_owned_business",
    "Women Owned Small Business":
    "transaction__recipient__women_owned_small_business",
    "Economically Disadvantaged Women Owned Small Business":
    "transaction__recipient__economically_disadvantaged_women_owned_small_business",
    "Joint Venture Women Owned Small Business":
    "transaction__recipient__joint_venture_women_owned_small_business",
    "Joint Venture Economically Disadvantaged Women Owned Small Business":
    "transaction__recipient__joint_venture_economic_disadvantaged_women_owned_small_bus",
    "Minority Owned Business":
    "transaction__recipient__minority_owned_business",
    "Subcontinent Asian Asian - Indian American Owned Business":
    "transaction__recipient__subcontinent_asian_asian_indian_american_owned_business",
    "Asian Pacific American Owned Business":
    "transaction__recipient__asian_pacific_american_owned_business",
    "Black American Owned Business":
    "transaction__recipient__black_american_owned_business",
    "Hispanic American Owned Business":
    "transaction__recipient__hispanic_american_owned_business",
    "Native American Owned Business":
    "transaction__recipient__native_american_owned_business",
    "Other Minority Owned Business":
    "transaction__recipient__other_minority_owned_business",
    "Contracting Officer's Determination of Business Size":
    "contracting_officers_determination_of_business_size",
    # "Contracting Officer's Determination of Business Size Code": "contracting_officers_determination_of_business_size_code",
    "Emerging Small Business":
    "transaction__recipient__emerging_small_business",
    "Community Developed Corporation Owned Firm":
    "transaction__recipient__community_developed_corporation_owned_firm",
    "Labor Surplus Area Firm":
    "transaction__recipient__labor_surplus_area_firm",
    "U.S. Federal Government": "transaction__recipient__us_federal_government",
    "Federally Funded Research and Development Corp":
    "transaction__recipient__federally_funded_research_and_development_corp",
    "Federal Agency": "transaction__recipient__federal_agency",
    "U.S. State Government": "transaction__recipient__us_state_government",
    "U.S. Local Government": "transaction__recipient__us_local_government",
    "City Local Government": "transaction__recipient__city_local_government",
    "County Local Government":
    "transaction__recipient__county_local_government",
    "Inter-Municipal Local Government":
    "transaction__recipient__inter_municipal_local_government",
    "Local Government Owned": "transaction__recipient__local_government_owned",
    "Municipality Local Government":
    "transaction__recipient__municipality_local_government",
    "School District Local Government":
    "transaction__recipient__school_district_local_government",
    "Township Local Government":
    "transaction__recipient__township_local_government",
    "U.S. Tribal Government": "transaction__recipient__us_tribal_government",
    "Foreign Government": "transaction__recipient__foreign_government",
    "Corporate Entity Not Tax Exempt":
    "transaction__recipient__corporate_entity_not_tax_exempt",
    "Corporate Entity Tax Exempt":
    "transaction__recipient__corporate_entity_tax_exempt",
    "Partnership or Limited Liability Partnership":
    "transaction__recipient__partnership_or_limited_liability_partnership",
    "Sole Proprietorship": "transaction__recipient__sole_proprietorship",
    "Small Agricultural Cooperative":
    "transaction__recipient__small_agricultural_cooperative",
    "International Organization":
    "transaction__recipient__international_organization",
    "U.S. Government Entity": "transaction__recipient__us_government_entity",
    "Community Development Corporation":
    "transaction__recipient__community_development_corporation",
    "Domestic Shelter": "transaction__recipient__domestic_shelter",
    "Educational Institution":
    "transaction__recipient__educational_institution",
    "Foundation": "transaction__recipient__foundation",
    "Hospital Flag": "transaction__recipient__hospital_flag",
    "Manufacturer of Goods": "transaction__recipient__manufacturer_of_goods",
    "Veterinary Hospital": "transaction__recipient__veterinary_hospital",
    "Hispanic Servicing Institution":
    "transaction__recipient__hispanic_servicing_institution",
    "Receives Contracts": "transaction__recipient__contracts",
    "Receives Grants": "transaction__recipient__grants",
    "Receives Contracts and Grants":
    "transaction__recipient__receives_contracts_and_grants",
    "Airport Authority": "transaction__recipient__airport_authority",
    "Council of Governments": "transaction__recipient__council_of_governments",
    "Housing Authorities Public/Tribal":
    "transaction__recipient__housing_authorities_public_tribal",
    "Interstate Entity": "transaction__recipient__interstate_entity",
    "Planning Commission": "transaction__recipient__planning_commission",
    "Port Authority": "transaction__recipient__port_authority",
    "Transit Authority": "transaction__recipient__transit_authority",
    "Subchapter S Corporation":
    "transaction__recipient__subchapter_scorporation",
    "Limited Liability Corporation":
    "transaction__recipient__limited_liability_corporation",
    "Foreign Owned and Located":
    "transaction__recipient__foreign_owned_and_located",
    "For Profit Organization":
    "transaction__recipient__for_profit_organization",
    "Nonprofit Organization": "transaction__recipient__nonprofit_organization",
    "Other Not For Profit Organization":
    "transaction__recipient__other_not_for_profit_organization",
    "The AbilityOne Program": "transaction__recipient__the_ability_one_program",
    "Private University or College ":
    "transaction__recipient__private_university_or_college",
    "State Controlled Institution of Higher Learning":
    "transaction__recipient__state_controlled_institution_of_higher_learning",
    "1862 Land Grant College":
    "transaction__recipient__c1862_land_grant_college",
    "1890 Land Grant College":
    "transaction__recipient__c1890_land_grant_college",
    "1994 Land Grant College":
    "transaction__recipient__c1994_land_grant_college",
    "Minority Institution": "transaction__recipient__minority_institution",
    "Historically Black College or University":
    "transaction__recipient__historically_black_college",
    "Tribal College": "transaction__recipient__tribal_college",
    "Alaskan Native Servicing Institution":
    "transaction__recipient__alaskan_native_servicing_institution",
    "Native Hawaiian Servicing Institution":
    "transaction__recipient__native_hawaiian_servicing_institution",
    "School of Forestry": "transaction__recipient__school_of_forestry",
    "Veterinary College": "transaction__recipient__veterinary_college",
    "DoT Certified Disadvantaged Business Enterprise":
    "transaction__recipient__dot_certified_disadvantage",
    "Self-Certified Small Disadvantaged Business":
    "transaction__recipient__self_certified_small_disadvantaged_business",
    "Small Disadvantaged Business":
    "transaction__recipient__small_disadvantaged_business",
    "8a Program Participant": "transaction__recipient__c8a_program_participant",
    "Historically Underutilized Business Zone HUBZone Firm":
    "transaction__recipient__historically_underutilized_business_zone",
    "SBA Certified 8a Joint Venture":
    "transaction__recipient__sba_certified_8a_joint_venture",
    "Last Modified Date": "last_modified_date",
    'Non Federal Funding Amount': 'non_federal_funding_amount',
    'Primary Place of Performance County Code':
    'transaction__place_of_performance__county_code',
    'SAI Number': 'sai_number',
    'Total Funding Amount': 'total_funding_amount',
    'URI': 'uri',
}

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
    'Period of Performance Current End Date'] = 'period_of_performance_current_end_date'
query_paths['transaction']['d2'][
    'Period of Performance Start Date'] = 'period_of_performance_start_date'

# These fields have the same paths from either the transaction or the award
unmodified_path_for_award = ('last_modified_date',
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

# Award IDs are complex, we'll do them separately
query_paths['transaction']['d1']['Award ID'] = 'piid'
query_paths['award']['d1']['Award ID'] = 'piid'
query_paths['transaction']['d2']['Award ID'] = Coalesce('fain', 'uri')
query_paths['award']['d2']['Award ID'] = Coalesce('fain', 'uri')
query_paths['transaction']['d1']['Parent Award ID'] = 'transaction__award__parent_award__piid'
query_paths['award']['d1']['Parent Award ID'] = 'parent_award__piid'
query_paths['transaction']['d2']['Parent Award ID'] = Coalesce('transaction__award__parent_award__fain', 'ttransaction__award__parent_award__uri')
query_paths['award']['d2']['Parent Award ID'] = Coalesce('parent_award__fain', 'parent_award__uri')

# now prune out those not called for, by human name

human_names = {
    'award': {
        'd1': [
            'Award ID', 'Parent Award Agency ID', 'Parent Award Agency Name',
            'Parent Award ID', 'Period of Performance Start Date',
            'Period of Performance Current End Date',
            'Period of Performance Potential End Date',
            'Ordering Period End Date', 'Awarding Agency Code',
            'Awarding Agency Name', 'Awarding Sub Agency Code',
            'Awarding Sub Agency Name', 'Awarding Office Code',
            'Awarding Office Name', 'Funding Agency Code',
            'Funding Agency Name', 'Funding Sub Agency Code',
            'Funding Sub Agency Name', 'Funding Office Code',
            'Funding Office Name', 'Foreign Funding', 'SAM Exception',
            'Recipient DUNS', 'Recipient Name',
            'Recipient Doing Business As Name', 'Recipient Parent Name',
            'Recipient Parent DUNS', 'Recipient Country Code',
            'Recipient Country Name', 'Recipient Address Line 1',
            'Recipient Address Line 2', 'Recipient Address Line 3',
            'Recipient City Name', 'Recipient State Code',
            'Recipient State Name', 'Recipient Foreign State Name',
            'Recipient Zip+4 Code', 'Recipient Congressional District',
            'Recipient Phone Number', 'Recipient Fax Number',
            'Primary Place of Performance Country Code',
            'Primary Place of Performance Country Name',
            'Primary Place of Performance City Name',
            'Primary Place of Performance County Name',
            'Primary Place of Performance State Code',
            'Primary Place of Performance State Name',
            'Primary Place of Performance Zip+4',
            'Primary Place of Performance Congressional District',
            'Primary Place of Performance Location Code',
            'Award or Parent Award Flag', 'Award Type Code', 'Award Type',
            'IDV Type Code', 'IDV Type', 'Multiple or Single Award IDV Code',
            'Multiple or Single Award IDV', 'Type of IDC Code', 'Type of IDC',
            'Type of Contract Pricing Code', 'Type of Contract Pricing',
            'Award Description', 'Solicitation Identifier',
            'Number of Actions', 'Product or Service Code (PSC)',
            'Product or Service Code (PSC) Description',
            'Contract Bundling Code', 'Contract Bundling',
            'DoD Claimant Program Code', 'DoD Claimant Program Description',
            'NAICS Code', 'NAICS Description',
            'Recovered Materials/Sustainability Code',
            'Recovered Materials/Sustainability',
            'Domestic or Foreign Entity Code', 'Domestic or Foreign Entity',
            'DoD Acquisition Program Code',
            'DoD Acquisition Program Description',
            'Information Technology Commercial Item Category Code',
            'Information Technology Commercial Item Category',
            'EPA-Designated Product Code', 'EPA-Designated Product',
            'Country of Product or Service Origin Code',
            'Country of Product or Service Origin',
            'Place of Manufacture Code', 'Place of Manufacture',
            'Subcontracting Plan Code', 'Subcontracting Plan',
            'Extent Competed Code', 'Extent Competed',
            'Solicitation Procedures Code', 'Solicitation Procedures',
            'Type of Set Aside Code', 'Type of Set Aside',
            'Evaluated Preference Code', 'Evaluated Preference',
            'Research Code', 'Research',
            'Fair Opportunity Limited Sources Code',
            'Fair Opportunity Limited Sources',
            'Other than Full and Open Competition Code',
            'Other than Full and Open Competition',
            'Number of Offers Received',
            'Commercial Item Acquisition Procedures Code',
            'Commercial Item Acquisition Procedures',
            'Small Business\nCompetitiveness Demonstration Program',
            'Commercial Item Test Program Code',
            'Commercial Item Test Program', 'A-76 FAIR Act Action Code',
            'A-76 FAIR Act Action', 'FedBizOpps Code', 'FedBizOpps',
            'Local Area Set Aside Code', 'Local Area Set Aside',
            'Clinger-Cohen Act Planning\n  Compliance Code',
            'Clinger-Cohen Act Planning\n  Compliance',
            'Walsh Healey Act Code', 'Walsh Healey Act',
            'Service Contract Act Code', 'Service Contract Act',
            'Davis Bacon Act Code', 'Davis Bacon Act',
            'Interagency Contracting\nAuthority Code',
            'Interagency Contracting\nAuthority', 'Other Statutory Authority',
            'Program Acronym', 'Parent Award Type Code', 'Parent Award Type',
            'Parent Award Single or Multiple Code',
            'Parent Award Single or Multiple', 'Major Program',
            'National Interest Action Code', 'National Interest Action',
            'Cost or Pricing Data Code', 'Cost or Pricing Data',
            'Cost Accounting Standards Clause Code',
            'Cost Accounting Standards Clause', 'GFE and GFP Code',
            'GFE and GFP', 'Sea Transportation Code', 'Sea Transportation',
            'Consolidated Contract Code', 'Consolidated Contract',
            'Performance-Based Service Acquisition Code',
            'Performance-Based Service Acquisition', 'Multi Year Contract Code',
            'Multi Year Contract', 'Contract Financing Code',
            'Contract Financing', 'Purchase Card as Payment Method Code',
            'Purchase Card as Payment Method',
            'Contingency Humanitarian or Peacekeeping Operation Code',
            'Contingency Humanitarian or Peacekeeping Operation',
            'Alaskan Native Owned\nCorporation or Firm',
            'American Indian Owned Business',
            'Indian Tribe Federally Recognized',
            'Native Hawaiian Owned Business', 'Tribally Owned Business',
            'Veteran Owned Business', 'Service Disabled Veteran Owned Business',
            'Woman Owned Business', 'Women Owned Small Business',
            'Economically Disadvantaged Women Owned Small Business',
            'Joint Venture Women Owned Small Business',
            'Joint Venture Economically Disadvantaged Women Owned Small '
            'Business', 'Minority Owned Business',
            'Subcontinent Asian Asian - Indian American Owned Business',
            'Asian Pacific American Owned Business',
            'Black American Owned Business', 'Hispanic American Owned Business',
            'Native American Owned\nBusiness', 'Other Minority Owned Business',
            "Contracting Officer's Determination of Business Size",
            "Contracting Officer's Determination of Business Size Code",
            'Emerging Small Business',
            'Community Developed Corporation Owned Firm',
            'Labor Surplus Area Firm', 'U.S. Federal Government',
            'Federally Funded Research and Development Corp', 'Federal Agency',
            'U.S. State Government', 'U.S. Local Government',
            'City Local Government', 'County Local Government',
            'Inter-Municipal Local Government', 'Local Government Owned',
            'Municipality Local Government',
            'School District Local\nGovernment', 'Township Local Government',
            'U.S. Tribal Government', 'Foreign Government',
            'Corporate Entity Not Tax Exempt', 'Corporate Entity Tax Exempt',
            'Partnership or Limited Liability Partnership',
            'Sole Proprietorship', 'Small Agricultural\nCooperative',
            'International Organization', 'U.S. Government Entity',
            'Community Development Corporation', 'Domestic Shelter',
            'Educational Institution', 'Foundation', 'Hospital Flag',
            'Manufacturer of Goods', 'Veterinary Hospital',
            'Hispanic Servicing\nInstitution', 'Receives Contracts',
            'Receives Grants', 'Receives Contracts and Grants',
            'Airport Authority', 'Council of Governments',
            'Housing Authorities\nPublic/Tribal', 'Interstate Entity',
            'Planning Commission', 'Port Authority', 'Transit Authority',
            'Subchapter S Corporation', 'Limited Liability Corporation',
            'Foreign Owned and Located', 'For Profit Organization',
            'Nonprofit Organization', 'Other Not For Profit\nOrganization',
            'The AbilityOne Program', 'Private University or College\xa0',
            'State Controlled Institution\nof Higher Learning',
            '1862 Land Grant College', '1890 Land Grant College',
            '1994 Land Grant College', 'Minority Institution',
            'Historically Black College or University', 'Tribal College',
            'Native Hawaiian Servicing  Institution', 'School of Forestry',
            'Veterinary College',
            'DoT Certified Disadvantaged Business Enterprise',
            'Self-Certified Small Disadvantaged Business',
            'Small Disadvantaged Business', '8a Program Participant',
            'Historically Underutilized\nBusiness Zone HUBZone Firm',
            'SBA Certified 8a Joint Venture', 'Last Modified Date'
        ],
        'd2': [
            'Award ID', 'URI', 'SAI Number', 'Federal Action Obligation',
            'Non Federal Funding Amount', 'Total Funding Amount',
            'Face Value of Loan', 'Original Subsidy Cost',
            'Period of Performance Start Date',
            'Period of Performance Current End Date', 'Awarding Agency Code',
            'Awarding Agency Name', 'Awarding Sub Agency Code',
            'Awarding Sub Agency Name', 'Awarding Office Code',
            'Awarding Office Name', 'Funding Agency Code',
            'Funding Agency Name', 'Funding Sub Agency Code',
            'Funding Sub Agency Name', 'Funding Office Code',
            'Funding Office Name', 'Recipient DUNS', 'Recipient Name',
            'Recipient Country Code', 'Recipient Country Name',
            'Recipient Address Line 1', 'Recipient Address Line 2',
            'Recipient Address Line 3', 'Recipient City Code',
            'Recipient City Name', 'Recipient Country Code',
            'Recipient Country Name', 'Recipient State Code',
            'Recipient State Name', 'Recipient Zip Code',
            'Recipient Zip Last 4 Code', 'Recipient Congressional District',
            'Recipient Foreign City Name', 'Recipient Foreign Province Name',
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
            'Primary Place of Performance Foreign Location', 'CFDA Number',
            'CFDA Title', 'Assistance Type Code', 'Assistance Type',
            'Award Description', 'Business Funds Indicator Code',
            'Business Funds Indicator', 'Business Types Code',
            'Business Types', 'Record Type Code', 'Record Type',
            'Last Modified Date'
        ]
    },
    'transaction': {
        'd1': [
            'Award ID', 'Modification Number', 'Transaction Number',
            'Parent Award Agency ID', 'Parent Award Agency Name',
            'Parent Award ID', 'Parent Award Modification Number',
            'Federal Action Obligation', 'Change in Current Award Amount',
            'Change in Potential Award Amount', 'Action Date',
            'Period of Performance Start Date',
            'Period of Performance Current End Date',
            'Period of Performance Potential End Date',
            'Ordering Period End Date', 'Awarding Agency Code',
            'Awarding Agency Name', 'Awarding Sub Agency Code',
            'Awarding Sub Agency Name', 'Awarding Office Code',
            'Awarding Office Name', 'Funding Agency Code',
            'Funding Agency Name', 'Funding Sub Agency Code',
            'Funding Sub Agency Name', 'Funding Office Code',
            'Funding Office Name', 'Foreign Funding', 'SAM Exception',
            'Recipient DUNS', 'Recipient Name',
            'Recipient Doing Business As Name', 'Recipient Parent Name',
            'Recipient Parent DUNS', 'Recipient Country Code',
            'Recipient Country Name', 'Recipient Address Line 1',
            'Recipient Address Line 2', 'Recipient Address Line 3',
            'Recipient City Name', 'Recipient State Code',
            'Recipient State Name', 'Recipient Foreign State Name',
            'Recipient Zip+4 Code', 'Recipient Congressional District',
            'Recipient Phone Number', 'Recipient Fax Number',
            'Primary Place of Performance Country Code',
            'Primary Place of Performance Country Name',
            'Primary Place of Performance City Name',
            'Primary Place of Performance County Name',
            'Primary Place of Performance State Code',
            'Primary Place of Performance State Name',
            'Primary Place of Performance Zip+4',
            'Primary Place of Performance Congressional District',
            'Primary Place of Performance Location Code',
            'Award or Parent Award Flag', 'Award Type Code', 'Award Type',
            'IDV Type Code', 'IDV Type', 'Multiple or Single Award IDV Code',
            'Multiple or Single Award IDV', 'Type of IDC Code', 'Type of IDC',
            'Type of Contract Pricing Code', 'Type of Contract Pricing',
            'Award Description', 'Action Type Code', 'Action Type',
            'Solicitation Identifier', 'Number of Actions',
            'Product or Service Code (PSC)',
            'Product or Service Code (PSC) Description',
            'Contract Bundling Code', 'Contract Bundling',
            'DoD Claimant Program Code', 'DoD Claimant Program Description',
            'NAICS Code', 'NAICS Description',
            'Recovered Materials/Sustainability Code',
            'Recovered Materials/Sustainability',
            'Domestic or Foreign Entity Code', 'Domestic or Foreign Entity',
            'DoD Acquisition Program Code',
            'DoD Acquisition Program Description',
            'Information Technology Commercial Item Category Code',
            'Information Technology Commercial Item Category',
            'EPA-Designated Product Code', 'EPA-Designated Product',
            'Country of Product or Service Origin Code',
            'Country of Product or Service Origin',
            'Place of Manufacture Code', 'Place of Manufacture',
            'Subcontracting Plan Code', 'Subcontracting Plan',
            'Extent Competed Code', 'Extent Competed',
            'Solicitation Procedures Code', 'Solicitation Procedures',
            'Type of Set Aside Code', 'Type of Set Aside',
            'Evaluated Preference Code', 'Evaluated Preference',
            'Research Code', 'Research',
            'Fair Opportunity Limited Sources Code',
            'Fair Opportunity Limited Sources',
            'Other than Full and Open Competition Code',
            'Other than Full and Open Competition',
            'Number of Offers Received',
            'Commercial Item Acquisition Procedures Code',
            'Commercial Item Acquisition Procedures',
            'Small Business\nCompetitiveness Demonstration Program',
            'Commercial Item Test Program Code',
            'Commercial Item Test Program', 'A-76 FAIR Act Action Code',
            'A-76 FAIR Act Action', 'FedBizOpps Code', 'FedBizOpps',
            'Local Area Set Aside Code', 'Local Area Set Aside',
            'Price Evaluation Adjustment Preference Percent '
            'Difference', 'Clinger-Cohen Act Planning\n  Compliance Code',
            'Clinger-Cohen Act Planning\n  Compliance',
            'Walsh Healey Act Code', 'Walsh Healey Act',
            'Service Contract Act Code', 'Service Contract Act',
            'Davis Bacon Act Code', 'Davis Bacon Act',
            'Interagency Contracting\nAuthority Code',
            'Interagency Contracting\nAuthority', 'Other Statutory Authority',
            'Program Acronym', 'Parent Award Type Code', 'Parent Award Type',
            'Parent Award Single or Multiple Code',
            'Parent Award Single or Multiple', 'Major Program',
            'National Interest Action Code', 'National Interest Action',
            'Cost or Pricing Data Code', 'Cost or Pricing Data',
            'Cost Accounting Standards Clause Code',
            'Cost Accounting Standards Clause', 'GFE and GFP Code',
            'GFE and GFP', 'Sea Transportation Code', 'Sea Transportation',
            'Undefinitized Action Code', 'Undefinitized Action',
            'Consolidated Contract Code', 'Consolidated Contract',
            'Performance-Based Service Acquisition Code',
            'Performance-Based Service Acquisition', 'Multi Year Contract Code',
            'Multi Year Contract', 'Contract Financing Code',
            'Contract Financing', 'Purchase Card as Payment Method Code',
            'Purchase Card as Payment Method',
            'Contingency Humanitarian or Peacekeeping Operation '
            'Code', 'Contingency Humanitarian or Peacekeeping Operation',
            'Alaskan Native Owned\nCorporation or Firm',
            'American Indian Owned Business',
            'Indian Tribe Federally Recognized',
            'Native Hawaiian Owned Business', 'Tribally Owned Business',
            'Veteran Owned Business', 'Service Disabled Veteran Owned Business',
            'Woman Owned Business', 'Women Owned Small Business',
            'Economically Disadvantaged Women Owned Small Business',
            'Joint Venture Women Owned Small Business',
            'Joint Venture Economically Disadvantaged Women Owned '
            'Small Business', 'Minority Owned Business',
            'Subcontinent Asian Asian - Indian American Owned '
            'Business', 'Asian Pacific American Owned Business',
            'Black American Owned Business', 'Hispanic American Owned Business',
            'Native American Owned\nBusiness', 'Other Minority Owned Business',
            "Contracting Officer's Determination of Business Size",
            "Contracting Officer's Determination of Business Size "
            'Code', 'Emerging Small Business',
            'Community Developed Corporation Owned Firm',
            'Labor Surplus Area Firm', 'U.S. Federal Government',
            'Federally Funded Research and Development Corp', 'Federal Agency',
            'U.S. State Government', 'U.S. Local Government',
            'City Local Government', 'County Local Government',
            'Inter-Municipal Local Government', 'Local Government Owned',
            'Municipality Local Government',
            'School District Local\nGovernment', 'Township Local Government',
            'U.S. Tribal Government', 'Foreign Government',
            'Corporate Entity Not Tax Exempt', 'Corporate Entity Tax Exempt',
            'Partnership or Limited Liability Partnership',
            'Sole Proprietorship', 'Small Agricultural\nCooperative',
            'International Organization', 'U.S. Government Entity',
            'Community Development Corporation', 'Domestic Shelter',
            'Educational Institution', 'Foundation', 'Hospital Flag',
            'Manufacturer of Goods', 'Veterinary Hospital',
            'Hispanic Servicing\nInstitution', 'Receives Contracts',
            'Receives Grants', 'Receives Contracts and Grants',
            'Airport Authority', 'Council of Governments',
            'Housing Authorities\nPublic/Tribal', 'Interstate Entity',
            'Planning Commission', 'Port Authority', 'Transit Authority',
            'Subchapter S Corporation', 'Limited Liability Corporation',
            'Foreign Owned and Located', 'For Profit Organization',
            'Nonprofit Organization', 'Other Not For Profit\nOrganization',
            'The AbilityOne Program', 'Private University or College\xa0',
            'State Controlled Institution\nof Higher Learning',
            '1862 Land Grant College', '1890 Land Grant College',
            '1994 Land Grant College', 'Minority Institution',
            'Historically Black College or University', 'Tribal College',
            'Native Hawaiian Servicing  Institution', 'School of Forestry',
            'Veterinary College',
            'DoT Certified Disadvantaged Business Enterprise',
            'Self-Certified Small Disadvantaged Business',
            'Small Disadvantaged Business', '8a Program Participant',
            'Historically Underutilized\n'
            'Business Zone HUBZone Firm', 'SBA Certified 8a Joint Venture',
            'Last Modified Date'
        ],
        'd2': [
            'Award ID', 'Modification Number', 'URI', 'SAI Number',
            'Federal Action Obligation', 'Non Federal Funding Amount',
            'Total Funding Amount', 'Face Value of Loan',
            'Original Subsidy Cost', 'Action Date',
            'Period of Performance Start Date',
            'Period of Performance Current End Date', 'Awarding Agency Code',
            'Awarding Agency Name', 'Awarding Sub Agency Code',
            'Awarding Sub Agency Name', 'Awarding Office Code',
            'Awarding Office Name', 'Funding Agency Code',
            'Funding Agency Name', 'Funding Sub Agency Code',
            'Funding Sub Agency Name', 'Funding Office Code',
            'Funding Office Name', 'Recipient DUNS', 'Recipient Name',
            'Recipient Country Code', 'Recipient Country Name',
            'Recipient Address Line 1', 'Recipient Address Line 2',
            'Recipient Address Line 3', 'Recipient City Code',
            'Recipient City Name', 'Recipient Country Code',
            'Recipient Country Name', 'Recipient State Code',
            'Recipient State Name', 'Recipient Zip Code',
            'Recipient Zip Last 4 Code', 'Recipient Congressional District',
            'Recipient Foreign City Name', 'Recipient Foreign Province Name',
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
            'Primary Place of Performance Foreign Location', 'CFDA Number',
            'CFDA Title', 'Assistance Type Code', 'Assistance Type',
            'Award Description', 'Business Funds Indicator Code',
            'Business Funds Indicator', 'Business Types Code',
            'Business Types', 'Action Type Code', 'Action Type',
            'Record Type Code', 'Record Type',
            'Fiscal Year and Quarter Correction', 'Last Modified Date'
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
