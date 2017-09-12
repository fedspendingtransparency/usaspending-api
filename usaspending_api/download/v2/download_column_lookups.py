from bidict import bidict

transaction_d1_columns = {
    "Award ID": "piid",  # DONE
    "Modification Number": "transaction__modification_number",  # DONE
    "Transaction Number": "transaction_number",  # DONE
    "Parent Award Agency ID": "referenced_idv_agency_identifier",  # DONE
    # "Parent Award Agency Name": "transaction__award__parent_award__name",  # TODO  # not in spreadsheet
    "Parent Award ID": "parent_award_id",  # DONE
    # "Parent Award Modification Number": "parent_award_modification_number",  # TODO
    "Federal Action Obligation": "transaction__federal_action_obligation",  # DONE
    # "Change in Current Award Amount": "change_in_current_award_amount",  # TODO
    # "Change in Potential Award Amount": "change_in_potential_award_amount",  # TODO
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
    # "SAM Exception": "sam_exception",  # TODO
    "Recipient DUNS": "transaction__recipient__recipient_unique_id",  # DONE
    "Recipient Name": "transaction__recipient__recipient_name",  # DONE
    "Recipient Doing Business As Name": "transaction__recipient__vendor_doing_as_business_name",  # DONE
    # "Recipient Parent Name": "recipient_parent_name",  # TODO
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
    # "Recipient Zip+4 Code": "transaction__recipient__location__zip4",  # TODO probably right
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
    "Domestic or Foreign Entity Code": "transaction__award__recipient__domestic_or_foreign_entity",
    "Domestic or Foreign Entity": "transaction__award__recipient__domestic_or_foreign_entity_description",
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
    # "Parent Award Type Code": "parent_award_type_code",  # TODO
    # "Parent Award Type": "parent_award_type",  # TODO
    # "Parent Award Single or Multiple Code": "parent_award_single_or_multiple_code",  # TODO
    # "Parent Award Single or Multiple": "parent_award_single_or_multiple",  # TODO
    "Major Program": "major_program",  # DONE
    "National Interest Action Code": "national_interest_action",  # DONE
    "National Interest Action": "national_interest_action_description",  # DONE
    "Cost or Pricing Data Code": "cost_or_pricing_data",  # DONE
    "Cost or Pricing Data": "cost_or_pricing_data_description",  # DONE
    # "Cost Accounting Standards Clause Code": "cost_accounting_standards_clause_code",  # TODO
    # "Cost Accounting Standards Clause": "cost_accounting_standards_clause_description",  # TODO
    # "GFE and GFP Code": "gfe_gfp",  # TODO: Code or description?
    # "GFE and GFP": "gfe_gfp",  # TODO: Code or description?
    "Sea Transportation Code": "sea_transportation",  # DONE
    "Sea Transportation": "sea_transportation_description",  # DONE
    # "Undefinitized Action Code": "undefinitized_action_code",  # TODO
    # "Undefinitized Action": "undefinitized_action_description",  # TODO
    # "Consolidated Contract Code": "consolidated_contract_code",  # TODO: Code or description?
    # "Consolidated Contract": "consolidated_contract_description",  # TODO: Code or description?
    "Performance-Based Service Acquisition Code": "performance_based_service_acquisition",  # DONE
    "Performance-Based Service Acquisition": "performance_based_service_acquisition_description",  # DONE
    # "Multi Year Contract Code": "multi_year_contract",  # TODO: Code or description?
    # "Multi Year Contract": "multi_year_contract",  # TODO: Code or description?
    "Contract Financing Code": "contract_financing",  # DONE
    "Contract Financing": "contract_financing_description",  # DONE
    # "Purchase Card as Payment Method Code": "purchase_card_as_payment_method_code",  # TODO: Code or description?
    # "Purchase Card as Payment Method": "purchase_card_as_payment_method_description",  # TODO: Code or description?
    "Contingency Humanitarian or Peacekeeping Operation Code": "contingency_humanitarian_or_peacekeeping_operation",  # DONE
    "Contingency Humanitarian or Peacekeeping Operation": "contingency_humanitarian_or_peacekeeping_operation_description",  # DONE
    "Alaskan Native Owned Corporation or Firm": "transaction__award__recipient__alaskan_native_owned_corporation_or_firm",
    "American Indian Owned Business": "transaction__award__recipient__american_indian_owned_business",
    "Indian Tribe Federally Recognized": "transaction__award__recipient__indian_tribe_federally_recognized",
    "Native Hawaiian Owned Business": "transaction__award__recipient__native_hawaiian_owned_business",
    "Tribally Owned Business": "transaction__award__recipient__tribally_owned_business",
    "Veteran Owned Business": "transaction__award__recipient__veteran_owned_business",
    "Service Disabled Veteran Owned Business": "transaction__award__recipient__service_disabled_veteran_owned_business",
    "Woman Owned Business": "transaction__award__recipient__woman_owned_business",
    "Women Owned Small Business": "transaction__award__recipient__women_owned_small_business",
    "Economically Disadvantaged Women Owned Small Business": "transaction__award__recipient__economically_disadvantaged_women_owned_small_business",
    "Joint Venture Women Owned Small Business": "transaction__award__recipient__joint_venture_women_owned_small_business",
    "Joint Venture Economically Disadvantaged Women Owned Small Business": "transaction__award__recipient__joint_venture_economic_disadvantaged_women_owned_small_bus",
    "Minority Owned Business": "transaction__award__recipient__minority_owned_business",
    "Subcontinent Asian Asian - Indian American Owned Business": "transaction__award__recipient__subcontinent_asian_asian_indian_american_owned_business",
    "Asian Pacific American Owned Business": "transaction__award__recipient__asian_pacific_american_owned_business",
    "Black American Owned Business": "transaction__award__recipient__black_american_owned_business",
    "Hispanic American Owned Business": "transaction__award__recipient__hispanic_american_owned_business",
    "Native American Owned Business": "transaction__award__recipient__native_american_owned_business",
    "Other Minority Owned Business": "transaction__award__recipient__other_minority_owned_business",
    # "Contracting Officer's Determination of Business Size": "contracting_officers_determination_of_business_size",
    # "Contracting Officer's Determination of Business Size Code": "contracting_officers_determination_of_business_size_code",
    "Emerging Small Business": "transaction__award__recipient__emerging_small_business",
    "Community Developed Corporation Owned Firm": "transaction__award__recipient__community_developed_corporation_owned_firm",
    "Labor Surplus Area Firm": "transaction__award__recipient__labor_surplus_area_firm",
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
    # "Historically Underutilized Business Zone HUBZone Firm": "transaction__recipient__historically_underutilized_business_zone_hubzone_firm",
    "SBA Certified 8a Joint Venture": "transaction__recipient__sba_certified_8a_joint_venture",  # DONE
    "Last Modified Date": "last_modified_date"  # DONE
}

transaction_d1_columns = bidict(transaction_d1_columns)

transaction_d2_columns = {
    "Award ID": "fain",
    "Modification Number": "transaction__modification_number",
    "URI": "uri",
    "SAI Number": "sai_number",
    "Federal Action Obligation": "transaction__federal_action_obligation",  # CDone
    "Non Federal Funding Amount": "non_federal_funding_amount",
    "Total Funding Amount": "total_funding_amount",
    # "Face Value of Loan": "face_value_of_loan",
    # "Original Subsidy Cost": "original_subsidy_cost",
    "Action Date": "transaction__action_date",
    "Period of Performance Start Date": "period_of_performance_start_date",
    "Period of Performance Current End Date": "period_of_performance_current_end_date",
    "Awarding Agency Code": "transaction__award__awarding_agency__toptier_agency__cgac_code",  # CDone
    "Awarding Agency Name": "transaction__award__awarding_agency__toptier_agency__name",  # CDone
    "Awarding Sub Agency Code": "transaction__award__awarding_agency__subtier_agency__subtier_code",  # CDone
    "Awarding Sub Agency Name": "transaction__award__awarding_agency__subtier_agency__name",  # CDone
    # "Awarding Office Code": "awarding_office_code",
    # "Awarding Office Name": "awarding_office_name",
    "Funding Agency Code": "transaction__award__funding_agency__toptier_agency__cgac_code",  # CDone
    "Funding Agency Name": "transaction__award__funding_agency__toptier_agency__name",  # CDone
    "Funding Sub Agency Code": "transaction__award__funding_agency__subtier_agency__subtier_code",  # CDone
    "Funding Sub Agency Name": "transaction__award__funding_agency__subtier_agency__name",  # CDone
    # "Funding Office Code": "funding_office_code",
    # "Funding Office Name": "funding_office_name",
    "Recipient DUNS": "transaction__recipient__recipient_unique_id",  # DONE
    "Recipient Name": "transaction__recipient__recipient_name",  # DONE
    "Recipient Country Code": "transaction__recipient__location__location_country_code",  # DONE
    "Recipient Country Name": "transaction__recipient__location__country_name",  # DONE
    "Recipient Address Line 1": "transaction__recipient__location__address_line1",  # DONE
    "Recipient Address Line 2": "transaction__recipient__location__address_line2",  # DONE
    "Recipient Address Line 3": "transaction__recipient__location__address_line3",  # DONE
    # "Recipient City Code": "recipient_city_code",
    "Recipient City Name": "transaction__recipient__location__city_name",  # DONE
    # "Recipient Country Code": "recipient_country_code",
    # "Recipient Country Name": "recipient_country_name",
    "Recipient State Code": "transaction__recipient__location__state_code",  # DONE
    "Recipient State Name": "transaction__recipient__location__state_name",  # DONE
    "Recipient Foreign State Name": "transaction__recipient__location__foreign_province",  # TODO probably right
    "Recipient Zip+4 Code": "transaction__recipient__location__zip4",  # TODO probably right
    "Recipient Congressional District": "transaction__recipient__location__congressional_code",  # TODO probably right
    "Recipient Phone Number": "transaction__recipient__vendor_phone_number",  # DONE
    "Recipient Fax Number": "transaction__recipient__vendor_fax_number",  # DONE
    # "Recipient Zip Code": "recipient_zip_code",
    # "Recipient Zip Last 4 Code": "recipient_zip_last_4_code",
    # "Recipient Congressional District": "recipient_congressional_district",
    # "Recipient Foreign City Name": "recipient_foreign_city_name",
    # "Recipient Foreign Province Name": "recipient_foreign_province_name",
    # "Recipient Foreign Postal Code": "recipient_foreign_postal_code",
    # "Primary Place of Performance Country Code": "primary_place_of_performance_country_code",
    "Primary Place of Performance Country Name": "transaction__place_of_performance__country_name",
    # "Primary Place of Performance Code": "primary_place_of_performance_code",
    "Primary Place of Performance City Name": "transaction__place_of_performance__city_name",
    "Primary Place of Performance County Code": "transaction__place_of_performance__county_code",
    "Primary Place of Performance County Name": "transaction__place_of_performance__county_name",
    "Primary Place of Performance State Name": "transaction__place_of_performance__state_name",
    "Primary Place of Performance Zip+4": "transaction__place_of_performance__zip4",  # TODO: or 4a?
    # "Primary Place of Performance Congressional District": "primary_place_of_performance_congressional_district",
    # "Primary Place of Performance Foreign Location": "primary_place_of_performance_foreign_location",
    # "CFDA Number": "cfda_number",
    # "CFDA Title": "cfda_title",
    # "Assistance Type Code": "assistance_type_code",
    # "Assistance Type": "assistance_type",
    "Award Description": "transaction__award__description",
    # "Business Funds Indicator Code": "business_funds_indicator_code",
    # "Business Funds Indicator": "business_funds_indicator",
    # "Business Types Code": "business_types_code",
    # "Business Types": "Business Types",
    # "Action Type Code": "action_type_code",
    # "Action Type": "action_type",
    # "Record Type Code": "record_type_code",
    # "Record Type": "record_type",
    # "Submitted Type": "submitted_type",
    # "Fiscal Year and Quarter Correction": "fiscal_year_and_quarter_correction",
    "Last Modified Date": "last_modified_date"
}

transaction_d2_columns = bidict(transaction_d2_columns)

AWARD_PREFIX = 'transaction__award__'

award_column_map = {
    k: v[len(AWARD_PREFIX):]
    for (k, v) in transaction_d1_columns.items()
    if v.startswith(AWARD_PREFIX)
    }

award_column_map.update({
    k: v[len('transaction__')]
    for (k, v) in transaction_d1_columns.items()
    if v.startswith('transaction__recipient__') or
       v.startswith('transaction__place_of_performance__')
})

award_column_map.update({
    "Parent Award ID": "parent_award_id",  # DONE
    "Total Dollars Obligated": "total_obligation",
    # "Current Total Value Of Award": "",   # TODO
    "Potential Total Value Of Award": "potential_total_value_of_award",
    "Period of Performance Start Date": "period_of_performance_start_date",  # DONE
    "Period of Performance Current End Date": "period_of_performance_current_end_date",  # DONE
    # "Period of Performance Potential End Date": "period_of_performance_current_end_date",  # DONE
    # "OrderingPeriodEndDate": "ordering_period_end_date",
    # "Foreign Funding": "foreign_funding",  # at transaction level
    # "Foreign Funding Description Tag": "",
})

"""
Columns marked Y for award level

    "Foreign Funding": "foreign_funding",
    "Foreign Funding Description Tag": "",
    "SAM Exception": "sam_exception",
    "SAM Exception Description Tag": "",
    "AwardeeOrRecipientUniqueIdentifier": "awardee_or_recipient_uniqu",
    "AwardeeOrRecipientLegalEntityName": "awardee_or_recipient_legal",
    "Vendor Doing As Business Name": "vendor_doing_as_business_n",
    "UltimateParentUniqueIdentifier": "ultimate_parent_unique_ide",
    "UltimateParentLegalEntityName": "ultimate_parent_legal_enti",
    "LegalEntityCountryCode": "legal_entity_country_code",
    "LegalEntityCountryName": "legal_entity_country_name",
    "LegalEntityAddressLine1": "legal_entity_address_line1",
    "LegalEntityAddressLine2": "legal_entity_address_line2",
    "LegalEntityAddressLine3": "legal_entity_address_line3",
    "LegalEntityCityName": "legal_entity_city_name",
    "LegalEntityStateCode": "legal_entity_state_code",
    "LegalEntityStateCode Description Tag": "",
    "LegalEntityStateDescription": "legal_entity_state_descrip",
    "LegalEntityZIP+4": "legal_entity_zip4",
    "LegalEntityCongressionalDistrict": "legal_entity_congressional",
    "Vendor Phone Number": "vendor_phone_number",
    "Vendor Fax Number": "vendor_fax_number",
    "PrimaryPlaceOfPerformanceCityName": "place_of_perform_city_name",
    "PrimaryPlaceOfPerformanceCountyName ": "/ns1:award/ns1:placeOfPerformance/ns1:placeOfPerformanceZIPCode@county",
    "PrimaryPlaceOfPerformanceStateCode": "place_of_performance_state",
    "PrimaryPlaceOfPerformanceStateName": "/ns1:award/ns1:placeOfPerformance/ns1:principalPlaceOfPerformance/ns1:stateCode@name",
    "PrimaryPlaceOfPerformanceZIP+4": "place_of_performance_zip4a",
    "PrimaryPlaceOfPerformanceCongressionalDistrict": "place_of_performance_congr",
    "PrimaryPlaceOfPerformanceCountryCode": "place_of_perform_country_c",
    "PrimaryPlaceOfPerformanceCountryName": "/ns1:award/ns1:placeOfPerformance/ns1:principalPlaceOfPerformance/ns1:countryCode@name",
    "PrimaryPlaceOfPerformanceLocationCode": "place_of_performance_locat",
    "Award_Or_IDV_Flag": "",
    "ContractAwardType ": "contract_award_type",
    "ContractAwardType Description Tag": "",
    "IDV_Type": "idv_type",
    "IDV_Type Description Tag": "",
    "Multiple or Single Award IDV": "multiple_or_single_award_i",
    "Multiple or Single Award IDV Description Tag": "",
    "Type of IDC": "type_of_idc",
    "Type of IDC Description Tag": "",
    "TypeOfContractPricing": "type_of_contract_pricing",
    "TypeOfContractPricing Description Tag": "",
    "Solicitation Identifier": "solicitation_identifier",
    "Number of Actions": "number_of_actions",
    "Product or Service Code": "product_or_service_code",
    "Product or Service Code Description Tag": "",
    "Contract Bundling": "contract_bundling",
    "Contract Bundling Description Tag": "",
    "DoD Claimant Program Code": "dod_claimant_program_code",
    "DoD Claimant Program Code Description Tag": "",
    "NAICS": "naics",
    "NAICS_Description": "naics_description",
    "Recovered Materials/Sustainability": "recovered_materials_sustai",
    "Recovered Materials/Sustainability Description Tag": "",
    "Domestic or Foreign Entity": "domestic_or_foreign_entity",
    "Domestic or Foreign Entity Description Tag": "",
    "DOD Acquisition Program": "program_system_or_equipmen",
    "DOD Acquisition Program Description Tag": "",
    "Information Technology Commercial Item Category": "information_technology_com",
    "Information Technology Commercial Item Category Description Tag": "",
    "EPA-Designated Product": "epa_designated_product",
    "EPA-Designated Product Description Tag": "",
    "Country of Product or Service Origin": "country_of_product_or_serv",
    "Country of Product or Service Origin Description Tag": "",
    "Place of Manufacture": "place_of_manufacture",
    "Place of Manufacture Description Tag": "",
    "Subcontracting Plan": "subcontracting_plan",
    "Subcontracting Plan Description Tag": "",
    "Extent Competed": "extent_competed",
    "Extent Competed Description Tag": "",
    "Solicitation Procedures": "solicitation_procedures",
    "Solicitation Procedures Description Tag": "",
    "Type Set Aside": "type_set_aside",
    "Type Set Aside Description Tag": "",
    "Evaluated Preference": "evaluated_preference",
    "Evaluated Preference Description Tag": "",
    "Research": "research",
    "Research Description Tag": "",
    "Fair Opportunity Limited Sources": "fair_opportunity_limited_s",
    "Fair Opportunity Limited Sources Description Tag": "",
    "Other than Full and Open Competition": "other_than_full_and_open_c",
    "Other than Full and Open Competition Description Tag": "",
    "Number of Offers Received": "number_of_offers_received",
    "Commercial Item Acquisition Procedures": "commercial_item_acquisitio",
    "Commercial Item Acquisition Procedures Description Tag": "",
    "Small Business Competitiveness Demonstration Program": "small_business_competitive",
    "Commercial Item Test Program": "commercial_item_test_progr",
    "Commercial Item Test Program Description Tag": "",
    "A-76 FAIR Act Action": "a_76_fair_act_action",
    "A-76 FAIR Act Action Description Tag": "",
    "FedBizOpps": "fed_biz_opps",
    "FedBizOpps Description Tag": "",
    "Local Area Set Aside": "local_area_set_aside",
    "Local Area Set Aside Description Tag": "",
    "Clinger-Cohen Act Planning Compliance": "clinger_cohen_act_planning",
    "Clinger-Cohen Act Planning Compliance Description Tag": "",
    "Walsh Healey Act": "walsh_healey_act",
    "Walsh Healey Act Description Tag": "",
    "Service Contract Act": "service_contract_act",
    "Service Contract Act Description Tag": "",
    "Davis Bacon Act": "davis_bacon_act",
    "Davis Bacon Act Description Tag": "",
    "Interagency Contracting Authority": "interagency_contracting_au",
    "Interagency Contracting Authority Description Tag": "",
    "Other Statutory Authority": "other_statutory_authority",
    "Program Acronym": "program_acronym",
    "Referenced_IDV_Type": "referenced_idv_type",
    "Referenced_IDV_Type Description Tag": "",
    "Referenced IDV Multiple or Single": "referenced_mult_or_single",
    "Referenced IDV Multiple or Single Description Tag": "",
    "Major program": "major_program",
    "National Interest Action": "national_interest_action",
    "National Interest Action Description Tag": "",
    "Cost or Pricing Data": "cost_or_pricing_data",
    "Cost or Pricing Data Description Tag": "",
    "Cost Accounting Standards Clause": "cost_accounting_standards",
    "Cost Accounting Standards Clause Description Tag": "",
    "Government Furnished Equipment GFE and Government Furnished Property GFP": "government_furnished_equip",
    "Government Furnished Equipment GFE and Government Furnished Property GFP Description Tag": "",
    "Sea Transportation": "sea_transportation",
    "Sea Transportation Description Tag": "",
    "Consolidated Contract": "consolidated_contract",
    "Consolidated Contract Description Tag": "",
    "Performance-Based Service Acquisition": "performance_based_service",
    "Performance-Based Service Acquisition Description Tag": "",
    "Multi Year Contract": "multi_year_contract",
    "Multi Year Contract Description Tag": "",
    "Contract Financing": "contract_financing",
    "Contract Financing Description Tag": "",
    "Purchase Card as Payment Method": "purchase_card_as_payment_m",
    "Purchase Card as Payment Method Description Tag": "",
    "Contingency Humanitarian or Peacekeeping Operation": "contingency_humanitarian_o",
    "Contingency Humanitarian or Peacekeeping Operation Description Tag": "",
    "Alaskan Native Owned Corporation or Firm": "alaskan_native_owned_corpo",
    "American Indian Owned Business": "american_indian_owned_busi",
    "Indian Tribe Federally Recognized": "indian_tribe_federally_rec",
    "Native Hawaiian Owned Business": "native_hawaiian_owned_busi",
    "Tribally Owned Business": "tribally_owned_business",
    "Veteran Owned Business": "veteran_owned_business",
    "Service Disabled Veteran Owned Business": "service_disabled_veteran_o",
    "Woman Owned business": "woman_owned_business",
    "Women Owned Small Business": "women_owned_small_business",
    "Economically Disadvantaged Women Owned Small Business": "economically_disadvantaged",
    "Joint Venture Women Owned Small Business": "joint_venture_women_owned",
    "Joint Venture Economically Disadvantaged Women Owned Small Business": "joint_venture_economically",
    "Minority Owned Business": "minority_owned_business",
    "Subcontinent Asian Asian - Indian American Owned Business": "subcontinent_asian_asian_i",
    "Asian Pacific American Owned business": "asian_pacific_american_own",
    "Black American Owned Business": "black_american_owned_busin",
    "Hispanic American Owned Business": "hispanic_american_owned_bu",
    "Native American Owned Business": "native_american_owned_busi",
    "Other Minority Owned Business": "other_minority_owned_busin",
    "Contracting Officer's Determination of Business Size": "contracting_officers_deter",
    "Contracting Officer's Determination of Business Size Description Tag": "",
    "Emerging Small business": "emerging_small_business",
    "Community Developed Corporation Owned Firm": "community_developed_corpor",
    "Labor Surplus Area Firm": "labor_surplus_area_firm",
    "U.S. Federal Government": "us_federal_government",
    "Federally Funded Research and Development Corp": "federally_funded_research",
    "Federal Agency": "federal_agency",
    "U.S. State Government": "us_state_government",
    "U.S. Local Government": "us_local_government",
    "City Local Government": "city_local_government",
    "County Local Government": "county_local_government",
    "Inter-Municipal Local Government": "inter_municipal_local_gove",
    "Local Government Owned": "local_government_owned",
    "Municipality Local Government": "municipality_local_governm",
    "School District Local Government": "school_district_local_gove",
    "Township Local Government": "township_local_government",
    "U.S. Tribal Government": "us_tribal_government",
    "Foreign Government": "foreign_government",
    "Corporate Entity Not Tax Exempt": "corporate_entity_not_tax_e",
    "Corporate Entity tax Exempt": "corporate_entity_tax_exemp",
    "Partnership or Limited Liability Partnership": "partnership_or_limited_lia",
    "Sole Proprietorship": "sole_proprietorship",
    "Small Agricultural Cooperative": "small_agricultural_coopera",
    "International Organization": "international_organization",
    "U.S. Government Entity": "us_government_entity",
    "Community Development Corporation": "community_development_corp",
    "Domestic Shelter": "domestic_shelter",
    "Educational Institution": "educational_institution",
    "Foundation": "foundation",
    "Hospital Flag": "hospital_flag",
    "Manufacturer of Goods": "manufacturer_of_goods",
    "Veterinary Hospital": "veterinary_hospital",
    "Hispanic Servicing Institution": "hispanic_servicing_institu",
    "Contracts": "contracts",
    "Grants": "grants",
    "Receives Contracts and Grants": "receives_contracts_and_gra",
    "Airport Authority": "airport_authority",
    "Council of Governments": "council_of_governments",
    "Housing Authorities Public/Tribal": "housing_authorities_public",
    "Interstate Entity": "interstate_entity",
    "Planning Commission": "planning_commission",
    "Port Authority": "port_authority",
    "Transit Authority": "transit_authority",
    "Subchapter S Corporation": "subchapter_s_corporation",
    "Limited Liability Corporation": "limited_liability_corporat",
    "Foreign Owned and Located": "foreign_owned_and_located",
    "For Profit Organization": "for_profit_organization",
    "Nonprofit Organization": "nonprofit_organization",
    "Other Not For Profit Organization": "other_not_for_profit_organ",
    "The AbilityOne Program": "the_ability_one_program",
    "Private University or College ": "private_university_or_coll",
    "State Controlled Institution of Higher Learning": "state_controlled_instituti",
    "1862 Land grant College": "c1862_land_grant_college",
    "1890 land grant College": "c1890_land_grant_college",
    "1994 Land Grant College": "c1994_land_grant_college",
    "Minority Institution": "minority_institution",
    "Historically Black College or University": "historically_black_college",
    "Tribal College": "tribal_college",
    "Alaskan Native Servicing Institution": "alaskan_native_servicing_i",
    "Native Hawaiian Servicing Institution": "native_hawaiian_servicing",
    "School of Forestry": "school_of_forestry",
    "Veterinary College": "veterinary_college",
    "DoT Certified Disadvantaged Business Enterprise": "dot_certified_disadvantage",
    "Self-Certified Small Disadvantaged Business": "self_certified_small_disad",
    "Small Disadvantaged Business": "small_disadvantaged_busine",
    "8a Program Participant": "c8a_program_participant",
    "Historically Underutilized Business Zone HUBZone Firm": "historically_underutilized",
    "SBA Certified 8 a Joint Venture": "sba_certified_8_a_joint_ve",

"""

transaction_columns = [
    "Award ID",
    "Modification Number",
    "Transaction Number",
    "Parent Award Agency ID",
    # "Parent Award Agency Name",  not in spreadsheet
    "Parent Award ID",
    "Parent Award Modification Number",
    "Federal Action Obligation",
    "Total Dollars Obligated",
    "Change in Current Award Amount",
    "Change in Potential Award Amount",
    "Current Total Value Of Award",
    "Potential Total Value Of Award",
    "Action Date",
    "Period of Performance Start Date",
    "Period of Performance Current End Date",
    "Period of Performance Potential End Date",
    "Ordering Period End Date",
    "Awarding Agency Code",
    "Awarding Agency Name",
    "Awarding Sub Agency Code",
    "Awarding Sub Agency Name",
    "Awarding Office Code",
    "Awarding Office Name",
    "Funding Agency Code",
    "Funding Agency Name",
    "Funding Sub Agency Code",
    "Funding Sub Agency Name",
    "Funding Office Code",
    "Funding Office Name",
    "Foreign Funding",
    "SAM Exception",
    "Recipient DUNS",
    "Recipient Name",
    "Recipient Doing Business As Name",
    "Recipient Parent Name",
    "Recipient Parent DUNS",
    "Recipient Country Code",
    "Recipient Country Name",
    "Recipient Address Line 1",
    "Recipient Address Line 2",
    "Recipient Address Line 3",
    "Recipient City Name",
    "Recipient State Code",
    "Recipient State Name",
    "Recipient Foreign State Name",
    "Recipient Zip+4 Code",
    "Recipient Congressional District",
    "Recipient Phone Number",
    "Recipient Fax Number",
    "Primary Place of Performance City Name",
    "Primary Place of Performance County Name",
    "Primary Place of Performance State Code",
    "Primary Place of Performance State Name",
    "Primary Place of Performance Zip+4",
    "Primary Place of Performance Congressional District",
    "Primary Place of Performance Country Code",
    "Primary Place of Performance Country Name",
    "Primary Place of Performance Location Code",
    "Award or Parent Award Flag",
    "Award Type Code",
    "Award Type",
    "IDV Type Code",
    "IDV Type",
    "Multiple or Single Award IDV Code",
    "Multiple or Single Award IDV",
    "Type of IDC Code",
    "Type of IDC",
    "Type of Contract Pricing Code",
    "Type of Contract Pricing",
    "Award Description",
    "Action Type Code",
    "Action Type",
    "Solicitation Identifier",
    "Number of Actions",
    "Product or Service Code (PSC)",
    "Product or Service Code (PSC) Description",
    "Contract Bundling Code",
    "Contract Bundling",
    "DoD Claimant Program Code",
    "DoD Claimant Program Description",
    "NAICS Code",
    "NAICS Description",
    "Recovered Materials/Sustainability Code",
    "Recovered Materials/Sustainability",
    "Domestic or Foreign Entity Code",
    "Domestic or Foreign Entity",
    "DoD Acquisition Program Code",
    "DoD Acquisition Program Description",
    "Information Technology Commercial Item Category Code",
    "Information Technology Commercial Item Category",
    "EPA-Designated Product Code",
    "EPA-Designated Product",
    "Country of Product or Service Origin Code",
    "Country of Product or Service Origin",
    "Place of Manufacture Code",
    "Place of Manufacture",
    "Subcontracting Plan Code",
    "Subcontracting Plan",
    "Extent Competed Code",
    "Extent Competed",
    "Solicitation Procedures Code",
    "Solicitation Procedures",
    "Type of Set Aside Code",
    "Type of Set Aside",
    "Evaluated Preference Code",
    "Evaluated Preference",
    "Research Code",
    "Research",
    "Fair Opportunity Limited Sources Code",
    "Fair Opportunity Limited Sources",
    "Other than Full and Open Competition Code",
    "Other than Full and Open Competition",
    "Number of Offers Received",
    "Commercial Item Acquisition Procedures Code",
    "Commercial Item Acquisition Procedures",
    "Small Business Competitiveness Demonstration Program",
    "Commercial Item Test Program Code",
    "Commercial Item Test Program",
    "A-76 FAIR Act Action Code",
    "A-76 FAIR Act Action",
    "FedBizOpps Code",
    "FedBizOpps",
    "Local Area Set Aside Code",
    "Local Area Set Aside",
    "Price Evaluation Adjustment Preference Percent Difference",
    "Clinger-Cohen Act Planning Compliance Code",
    "Clinger-Cohen Act Planning Compliance",
    "Walsh Healey Act Code",
    "Walsh Healey Act",
    "Service Contract Act Code",
    "Service Contract Act",
    "Davis Bacon Act Code",
    "Davis Bacon Act",
    "Interagency Contracting Authority Code",
    "Interagency Contracting Authority",
    "Other Statutory Authority",
    "Program Acronym",
    "Parent Award Type Code",
    "Parent Award Type",
    "Parent Award Single or Multiple Code",
    "Parent Award Single or Multiple",
    "Major Program",
    "National Interest Action Code",
    "National Interest Action",
    "Cost or Pricing Data Code",
    "Cost or Pricing Data",
    "Cost Accounting Standards Clause Code",
    "Cost Accounting Standards Clause",
    "GFE and GFP Code",
    "GFE and GFP",
    "Sea Transportation Code",
    "Sea Transportation",
    "Undefinitized Action Code",
    "Undefinitized Action",
    "Consolidated Contract Code",
    "Consolidated Contract",
    "Performance-Based Service Acquisition Code",
    "Performance-Based Service Acquisition",
    "Multi Year Contract Code",
    "Multi Year Contract",
    "Contract Financing Code",
    "Contract Financing",
    "Purchase Card as Payment Method Code",
    "Purchase Card as Payment Method",
    "Contingency Humanitarian or Peacekeeping Operation Code",
    "Contingency Humanitarian or Peacekeeping Operation",
    "Alaskan Native Owned Corporation or Firm",
    "American Indian Owned Business",
    "Indian Tribe Federally Recognized",
    "Native Hawaiian Owned Business",
    "Tribally Owned Business",
    "Veteran Owned Business",
    "Service Disabled Veteran Owned Business",
    "Woman Owned Business",
    "Women Owned Small Business",
    "Economically Disadvantaged Women Owned Small Business",
    "Joint Venture Women Owned Small Business",
    "Joint Venture Economically Disadvantaged Women Owned Small Business",
    "Minority Owned Business",
    "Subcontinent Asian Asian - Indian American Owned Business",
    "Asian Pacific American Owned Business",
    "Black American Owned Business",
    "Hispanic American Owned Business",
    "Native American Owned Business",
    "Other Minority Owned Business",
    "Contracting Officer's Determination of Business Size",
    "Contracting Officer's Determination of Business Size Code",
    "Emerging Small Business",
    "Community Developed Corporation Owned Firm",
    "Labor Surplus Area Firm",
    "U.S. Federal Government",
    "Federally Funded Research and Development Corp",
    "Federal Agency",
    "U.S. State Government",
    "U.S. Local Government",
    "City Local Government",
    "County Local Government",
    "Inter-Municipal Local Government",
    "Local Government Owned",
    "Municipality Local Government",
    "School District Local Government",
    "Township Local Government",
    "U.S. Tribal Government",
    "Foreign Government",
    "Corporate Entity Not Tax Exempt",
    "Corporate Entity Tax Exempt",
    "Partnership or Limited Liability Partnership",
    "Sole Proprietorship",
    "Small Agricultural Cooperative",
    "International Organization",
    "U.S. Government Entity",
    "Community Development Corporation",
    "Domestic Shelter",
    "Educational Institution",
    "Foundation",
    "Hospital Flag",
    "Manufacturer of Goods",
    "Veterinary Hospital",
    "Hispanic Servicing Institution",
    "Receives Contracts",
    "Receives Grants",
    "Receives Contracts and Grants",
    "Airport Authority",
    "Council of Governments",
    "Housing Authorities Public/Tribal",
    "Interstate Entity",
    "Planning Commission",
    "Port Authority",
    "Transit Authority",
    "Subchapter S Corporation",
    "Limited Liability Corporation",
    "Foreign Owned and Located",
    "For Profit Organization",
    "Nonprofit Organization",
    "Other Not For Profit Organization",
    "The AbilityOne Program",
    "Private University or College ",
    "State Controlled Institution of Higher Learning",
    "1862 Land Grant College",
    "1890 Land Grant College",
    "1994 Land Grant College",
    "Minority Institution",
    "Historically Black College or University",
    "Tribal College",
    "Alaskan Native Servicing Institution",
    "Native Hawaiian Servicing Institution",
    "School of Forestry",
    "Veterinary College",
    "DoT Certified Disadvantaged Business Enterprise",
    "Self-Certified Small Disadvantaged Business",
    "Small Disadvantaged Business",
    "8a Program Participant",
    "Historically Underutilized Business Zone HUBZone Firm",
    "SBA Certified 8a Joint Venture",
    "Last Modified Date",

    "URI",
    "SAI Number",
    "Federal Action Obligation",
    "Non Federal Funding Amount",
    "Total Funding Amount",
    "Face Value of Loan",
    "Original Subsidy Cost",
    "Action Date",
    "Period of Performance Start Date",
    "Period of Performance Current End Date",
    "Awarding Agency Code",
    "Awarding Agency Name",
    "Awarding Sub Agency Code",
    "Awarding Sub Agency Name",
    "Awarding Office Code",
    "Awarding Office Name",
    "Funding Agency Code",
    "Funding Agency Name",
    "Funding Sub Agency Code",
    "Funding Sub Agency Name",
    "Funding Office Code",
    "Funding Office Name",
    "Recipient DUNS",
    "Recipient Name",
    "Recipient Country Code",
    "Recipient Country Name",
    "Recipient Address Line 1",
    "Recipient Address Line 2",
    "Recipient Address Line 3",
    "Recipient City Code",
    "Recipient City Name",
    "Recipient Country Code",
    "Recipient Country Name",
    "Recipient State Code",
    "Recipient State Name",
    "Recipient Zip Code",
    "Recipient Zip Last 4 Code",
    "Recipient Congressional District",
    "Recipient Foreign City Name",
    "Recipient Foreign Province Name",
    "Recipient Foreign Postal Code",
    "Primary Place of Performance Country Code",
    "Primary Place of Performance Country Name",
    "Primary Place of Performance Code",
    "Primary Place of Performance City Name",
    "Primary Place of Performance County Code",
    "Primary Place of Performance County Name",
    "Primary Place of Performance State Name",
    "Primary Place of Performance Zip+4",
    "Primary Place of Performance Congressional District",
    "Primary Place of Performance Foreign Location",
    "CFDA Number",
    "CFDA Title",
    "Assistance Type Code",
    "Assistance Type",
    "Award Description",
    "Business Funds Indicator Code",
    "Business Funds Indicator",
    "Business Types Code",
    "Business Types",
    "Action Type Code",
    "Action Type",
    "Record Type Code",
    "Record Type",
    "Submitted Type",
    "Fiscal Year and Quarter Correction",
    "Last Modified Date"
]

award_columns = [
    c for c in transaction_columns
    if c in award_column_map
    ]

# Temporarily omit columns that we have not yet worked out the mappings for.
transaction_columns = [c for c in transaction_columns if c in transaction_d1_columns or c in transaction_d2_columns]

transaction_columns_unique = [
    "1862 Land Grant College",
    "1890 Land Grant College",
    "1994 Land Grant College",
    "8a Program Participant",
    "A-76 FAIR Act Action Code",
    "A-76 FAIR Act Action",
    "Action Date",
    "Action Type Code",
    "Action Type",
    "Airport Authority",
    "Alaskan Native Owned Corporation or Firm",
    "Alaskan Native Servicing Institution",
    "American Indian Owned Business",
    "Asian Pacific American Owned Business",
    "Assistance Type Code",
    "Assistance Type",
    "Award Description",
    "Award ID",
    "Award Type Code",
    "Award Type",
    "Award or Parent Award Flag",
    "Awarding Agency Code",
    "Awarding Agency Name",
    "Awarding Office Code",
    "Awarding Office Name",
    "Awarding Sub Agency Code",
    "Awarding Sub Agency Name",
    "Black American Owned Business",
    "Business Funds Indicator Code",
    "Business Funds Indicator",
    "Business Types Code",
    "Business Types",
    "CFDA Number",
    "CFDA Title",
    "Change in Current Award Amount",
    "Change in Potential Award Amount",
    "City Local Government",
    "Clinger-Cohen Act Planning Compliance Code",
    "Clinger-Cohen Act Planning Compliance",
    "Commercial Item Acquisition Procedures Code",
    "Commercial Item Acquisition Procedures",
    "Commercial Item Test Program Code",
    "Commercial Item Test Program",
    "Community Developed Corporation Owned Firm",
    "Community Development Corporation",
    "Consolidated Contract Code",
    "Consolidated Contract",
    "Contingency Humanitarian or Peacekeeping Operation Code",
    "Contingency Humanitarian or Peacekeeping Operation",
    "Contract Bundling Code",
    "Contract Bundling",
    "Contract Financing Code",
    "Contract Financing",
    "Contracting Officer's Determination of Business Size Code",
    "Contracting Officer's Determination of Business Size",
    "Corporate Entity Not Tax Exempt",
    "Corporate Entity Tax Exempt",
    "Cost Accounting Standards Clause Code",
    "Cost Accounting Standards Clause",
    "Cost or Pricing Data Code",
    "Cost or Pricing Data",
    "Council of Governments",
    "Country of Product or Service Origin Code",
    "Country of Product or Service Origin",
    "County Local Government",
    "Davis Bacon Act Code",
    "Davis Bacon Act",
    "DoD Acquisition Program Code",
    "DoD Acquisition Program Description",
    "DoD Claimant Program Code",
    "DoD Claimant Program Description",
    "DoT Certified Disadvantaged Business Enterprise",
    "Domestic Shelter",
    "Domestic or Foreign Entity Code",
    "Domestic or Foreign Entity",
    "EPA-Designated Product Code",
    "EPA-Designated Product",
    "Economically Disadvantaged Women Owned Small Business",
    "Educational Institution",
    "Emerging Small Business",
    "Evaluated Preference Code",
    "Evaluated Preference",
    "Extent Competed Code",
    "Extent Competed",
    "Face Value of Loan",
    "Fair Opportunity Limited Sources Code",
    "Fair Opportunity Limited Sources",
    "FedBizOpps Code",
    "FedBizOpps",
    "Federal Action Obligation",
    "Federal Agency",
    "Federally Funded Research and Development Corp",
    "Fiscal Year and Quarter Correction",
    "For Profit Organization",
    "Foreign Funding",
    "Foreign Government",
    "Foreign Owned and Located",
    "Foundation",
    "Funding Agency Code",
    "Funding Agency Name",
    "Funding Office Code",
    "Funding Office Name",
    "Funding Sub Agency Code",
    "Funding Sub Agency Name",
    "GFE and GFP Code",
    "GFE and GFP",
    "Hispanic American Owned Business",
    "Hispanic Servicing Institution",
    "Historically Black College or University",
    "Historically Underutilized Business Zone HUBZone Firm",
    "Hospital Flag",
    "Housing Authorities Public/Tribal",
    "IDV Type Code",
    "IDV Type",
    "Indian Tribe Federally Recognized",
    "Information Technology Commercial Item Category Code",
    "Information Technology Commercial Item Category",
    "Inter-Municipal Local Government",
    "Interagency Contracting Authority Code",
    "Interagency Contracting Authority",
    "International Organization",
    "Interstate Entity",
    "Joint Venture Economically Disadvantaged Women Owned Small Business",
    "Joint Venture Women Owned Small Business",
    "Labor Surplus Area Firm",
    "Last Modified Date",
    "Limited Liability Corporation",
    "Local Area Set Aside Code",
    "Local Area Set Aside",
    "Local Government Owned",
    "Major Program",
    "Manufacturer of Goods",
    "Minority Institution",
    "Minority Owned Business",
    "Modification Number",
    "Multi Year Contract Code",
    "Multi Year Contract",
    "Multiple or Single Award IDV Code",
    "Multiple or Single Award IDV",
    "Municipality Local Government",
    "NAICS Code",
    "NAICS Description",
    "National Interest Action Code",
    "National Interest Action",
    "Native American Owned Business",
    "Native Hawaiian Owned Business",
    "Native Hawaiian Servicing Institution",
    "Non Federal Funding Amount",
    "Nonprofit Organization",
    "Number of Actions",
    "Number of Offers Received",
    "Ordering Period End Date",
    "Original Subsidy Cost",
    "Other Minority Owned Business",
    "Other Not For Profit Organization",
    "Other Statutory Authority",
    "Other than Full and Open Competition Code",
    "Other than Full and Open Competition",
    "Parent Award Agency ID",
    "Parent Award Agency Name",
    "Parent Award ID",
    "Parent Award Modification Number",
    "Parent Award Single or Multiple Code",
    "Parent Award Single or Multiple",
    "Parent Award Type Code",
    "Parent Award Type",
    "Partnership or Limited Liability Partnership",
    "Performance-Based Service Acquisition Code",
    "Performance-Based Service Acquisition",
    "Period of Performance Current End Date",
    "Period of Performance Potential End Date",
    "Period of Performance Start Date",
    "Place of Manufacture Code",
    "Place of Manufacture",
    "Planning Commission",
    "Port Authority",
    "Price Evaluation Adjustment Preference Percent Difference",
    "Primary Place of Performance City Name",
    "Primary Place of Performance Code",
    "Primary Place of Performance Congressional District",
    "Primary Place of Performance Country Code",
    "Primary Place of Performance Country Name",
    "Primary Place of Performance County Code",
    "Primary Place of Performance County Name",
    "Primary Place of Performance Foreign Location",
    "Primary Place of Performance Location Code",
    "Primary Place of Performance State Code",
    "Primary Place of Performance State Name",
    "Primary Place of Performance Zip+4",
    "Private University or College ",
    "Product or Service Code (PSC) Description",
    "Product or Service Code (PSC)",
    "Program Acronym",
    "Purchase Card as Payment Method Code",
    "Purchase Card as Payment Method",
    "Receives Contracts and Grants",
    "Receives Contracts",
    "Receives Grants",
    "Recipient Address Line 1",
    "Recipient Address Line 2",
    "Recipient Address Line 3",
    "Recipient City Code",
    "Recipient City Name",
    "Recipient Congressional District",
    "Recipient Country Code",
    "Recipient Country Name",
    "Recipient DUNS",
    "Recipient Doing Business As Name",
    "Recipient Fax Number",
    "Recipient Foreign City Name",
    "Recipient Foreign Postal Code",
    "Recipient Foreign Province Name",
    "Recipient Foreign State Name",
    "Recipient Name",
    "Recipient Parent DUNS",
    "Recipient Parent Name",
    "Recipient Phone Number",
    "Recipient State Code",
    "Recipient State Name",
    "Recipient Zip Code",
    "Recipient Zip Last 4 Code",
    "Recipient Zip+4 Code",
    "Record Type Code",
    "Record Type",
    "Recovered Materials/Sustainability Code",
    "Recovered Materials/Sustainability",
    "Research Code",
    "Research",
    "SAI Number",
    "SAM Exception",
    "SBA Certified 8a Joint Venture",
    "School District Local Government",
    "School of Forestry",
    "Sea Transportation Code",
    "Sea Transportation",
    "Self-Certified Small Disadvantaged Business",
    "Service Contract Act Code",
    "Service Contract Act",
    "Service Disabled Veteran Owned Business",
    "Small Agricultural Cooperative",
    "Small Business Competitiveness Demonstration Program",
    "Small Disadvantaged Business",
    "Sole Proprietorship",
    "Solicitation Identifier",
    "Solicitation Procedures Code",
    "Solicitation Procedures",
    "State Controlled Institution of Higher Learning",
    "Subchapter S Corporation",
    "Subcontinent Asian Asian - Indian American Owned Business",
    "Subcontracting Plan Code",
    "Subcontracting Plan",
    "Submitted Type",
    "The AbilityOne Program",
    "Total Funding Amount",
    "Township Local Government",
    "Transaction Number",
    "Transit Authority",
    "Tribal College",
    "Tribally Owned Business",
    "Type of Contract Pricing Code",
    "Type of Contract Pricing",
    "Type of IDC Code",
    "Type of IDC",
    "Type of Set Aside Code",
    "Type of Set Aside",
    "U.S. Federal Government",
    "U.S. Government Entity",
    "U.S. Local Government",
    "U.S. State Government",
    "U.S. Tribal Government",
    "URI",
    "Undefinitized Action Code",
    "Undefinitized Action",
    "Veteran Owned Business",
    "Veterinary College",
    "Veterinary Hospital",
    "Walsh Healey Act Code",
    "Walsh Healey Act",
    "Woman Owned Business",
    "Women Owned Small Business"
]
