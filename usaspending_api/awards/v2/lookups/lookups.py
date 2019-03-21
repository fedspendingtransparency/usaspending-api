award_contracts_mapping = {
    'Award ID': 'piid',
    'Recipient Name': 'recipient__recipient_name',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Funding Agency': 'funding_agency__toptier_agency__name',
    'Funding Sub Agency': 'funding_agency__subtier_agency__name',
    'Contract Award Type': 'type_description',
    'Contract Description': 'description',
    'Signed Date': 'date_signed',
    'Potential Award Amount': 'base_and_all_options_value',
    'Awarding Agency': 'awarding_agency__toptier_agency__name',
    'Awarding Sub Agency': 'awarding_agency__subtier_agency__name',
    'Awarding Office': 'awarding_agency__office_agency__name',
    'Funding Office': 'funding_agency__office_agency__name',
    'Recipient Address Line 1': 'recipient__location__address_line1',
    'Recipient Address Line 2': 'recipient__location__address_line2',
    'Recipient Address Line 3': 'recipient__location__address_line3',
    'Recipient Country': 'recipient__location__country_name',
    'Recipient State': 'recipient__location__state_code',
    'Recipient Province': 'recipient__location__foreign_province',
    'Recipient County': 'recipient__location__county_name',
    'Recipient City': 'recipient__location__city_name',
    'Recipient Zip Code': 'recipient__location__zip5',
    'Place of Performance City': 'place_of_performance__city_name',
    'Place of Performance Zip Code': 'place_of_performance__zip5',
    'Place of Performance Country': 'place_of_performance__country_name',
    'Place of Performance State': 'place_of_performance__state_name',
    'Place of Performance Province': 'place_of_performance__foreign_province',
    'Recipient DUNS Number': 'recipient__recipient_unique_id',
    'Recipient Ultimate DUNS Number': 'recipient__parent_recipient_unique_id',
    'Contract Pricing Type': 'latest_transaction__contract_data__type_of_contract_pricing',
    'Recipient Congressional District': 'recipient__location__congressional_code',
    'Recipient Phone Number': 'recipient__vendor_phone_number',
    'Recipient Fax Number': 'recipient__vendor_fax_number',
    'Place of Performance Congressional District': 'place_of_performance__congressional_code',
    'Place of Performance County': 'place_of_performance__county_name',
    'Parent Award ID': 'latest_transaction__contract_data__parent_award_id',
    'IDV Type': 'latest_transaction__contract_data__idv_type',
    'IDC Type': 'latest_transaction__contract_data__type_of_idc',
    'IDV Agency Identifier': 'latest_transaction__contract_data__referenced_idv_agency_iden',
    'Multiple or Single Award IDV': 'latest_transaction__contract_data__multiple_or_single_award_i',
    'Solicitation ID': 'latest_transaction__contract_data__solicitation_identifier',
    'Solicitation Procedures': 'latest_transaction__contract_data__solicitation_procedures',
    'Number of Offers Received': 'latest_transaction__contract_data__number_of_offers_received',
    'Extent Competed': 'latest_transaction__contract_data__extent_compete_description',
    'Set-Aside Type': 'latest_transaction__contract_data__type_set_aside_description',
    'Commercial Item Acquisition Procedures': 'latest_transaction__contract_data__commercial_item_acqui_desc',
    'Commercial Item Test Program': 'latest_transaction__contract_data__commercial_item_test_progr',
    'Evaluated Preference': 'latest_transaction__contract_data__evaluated_preference_desc',
    'FedBizOpps': 'latest_transaction__contract_data__fed_biz_opps_description',
    'Small Business Competitiveness Demonstration Program':
        'latest_transaction__contract_data__small_business_competitive',
    'PSC Code': 'latest_transaction__contract_data__product_or_service_code',
    'NAICS Code': 'latest_transaction__contract_data__naics',
    'NAICS Description': 'latest_transaction__contract_data__naics_description',
    'DoD Claimant Program Code': 'latest_transaction__contract_data__dod_claimant_program_code',
    'Program, System, or Equipment Code': 'latest_transaction__contract_data__program_system_or_equipmen',
    'Information Technology Commercial Item Category': 'latest_transaction__contract_data__information_technolog_desc',
    'Sea Transportation': 'latest_transaction__contract_data__sea_transportation_desc',
    'Clinger-Cohen Act Compliant': 'latest_transaction__contract_data__clinger_cohen_act_planning',
    'Subject To Construction Wage Rate Requirements': 'latest_transaction__contract_data__construction_wage_rat_desc',
    'Subject To Labor Standards': 'latest_transaction__contract_data__labor_standards_descrip',
    'Subject To Materials Supplies Articles Equipment': 'latest_transaction__contract_data__materials_supplies_descrip',
    'Consolidated Contract': 'latest_transaction__contract_data__consolidated_contract',
    'Cost or Pricing Data': 'latest_transaction__contract_data__cost_or_pricing_data_desc',
    'Fair Opportunity Limited Sources': 'latest_transaction__contract_data__fair_opportunity_limi_desc',
    'Foreign Funding': 'latest_transaction__contract_data__foreign_funding_desc',
    'Interagency Contracting Authority': 'latest_transaction__contract_data__interagency_contract_desc',
    'Major program': 'latest_transaction__contract_data__major_program',
    'Multi Year Contract': 'latest_transaction__contract_data__multi_year_contract',
    'Price Evaluation Adjustment Preference Percent Difference':
        'latest_transaction__contract_data__price_evaluation_adjustmen',
    'Program Acronym': 'latest_transaction__contract_data__program_acronym',
    'Purchase Card as Payment Method': 'latest_transaction__contract_data__purchase_card_as_payment_m',
    'Subcontracting Plan': 'latest_transaction__contract_data__subcontracting_plan_desc'
}

grant_award_mapping = {
    'Award ID': 'fain',
    'Recipient Name': 'recipient__recipient_name',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Awarding Agency': 'awarding_agency__toptier_agency__name',
    'Awarding Sub Agency': 'awarding_agency__subtier_agency__name',
    'Award Type': 'type_description',
    'Funding Agency': 'funding_agency__toptier_agency__name',
    'Funding Sub Agency': 'funding_agency__subtier_agency__name'
}

loan_award_mapping = {
    'Award ID': 'fain',
    'Recipient Name': 'recipient__recipient_name',
    'Issued Date': 'latest_transaction__action_date',
    'Loan Value': 'latest_transaction__assistance_data__face_value_loan_guarantee',
    'Subsidy Cost': 'latest_transaction__assistance_data__original_loan_subsidy_cost',
    'Awarding Agency': 'awarding_agency__toptier_agency__name',
    'Awarding Sub Agency': 'awarding_agency__subtier_agency__name',
    'Funding Agency': 'funding_agency__toptier_agency__name',
    'Funding Sub Agency': 'funding_agency__subtier_agency__name'
}

direct_payment_award_mapping = {
    'Award ID': 'fain',
    'Recipient Name': 'recipient__recipient_name',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Awarding Agency': 'awarding_agency__toptier_agency__name',
    'Awarding Sub Agency': 'awarding_agency__subtier_agency__name',
    'Award Type': 'type_description',
    'Funding Agency': 'funding_agency__toptier_agency__name',
    'Funding Sub Agency': 'funding_agency__subtier_agency__name'
}

other_award_mapping = {
    'Award ID': 'fain',
    'Recipient Name': 'recipient__recipient_name',
    'Start Date': 'period_of_performance_start_date',
    'End Date': 'period_of_performance_current_end_date',
    'Award Amount': 'total_obligation',
    'Awarding Agency': 'awarding_agency__toptier_agency__name',
    'Awarding Sub Agency': 'awarding_agency__subtier_agency__name',
    'Award Type': 'type_description',
    'Funding Agency': 'funding_agency__toptier_agency__name',
    'Funding Sub Agency': 'funding_agency__subtier_agency__name'
}

contract_subaward_mapping = {
    "Sub-Award ID": "subaward_number",
    "Sub-Award Type": "award_type",
    "Sub-Awardee Name": "recipient_name",
    "Sub-Award Date": "action_date",
    "Sub-Award Amount": "amount",
    "Awarding Agency": "awarding_toptier_agency_name",
    "Awarding Sub Agency": "awarding_subtier_agency_name",
    "Prime Award ID": "piid",
    "Prime Recipient Name": "prime_recipient_name"
}

grant_subaward_mapping = {
    "Sub-Award ID": "subaward_number",
    "Sub-Award Type": "award_type",
    "Sub-Awardee Name": "recipient_name",
    "Sub-Award Date": "action_date",
    "Sub-Award Amount": "amount",
    "Awarding Agency": "awarding_toptier_agency_name",
    "Awarding Sub Agency": "awarding_subtier_agency_name",
    "Prime Award ID": "fain",
    "Prime Recipient Name": "prime_recipient_name"
}

award_assistance_mapping = {**grant_award_mapping, **loan_award_mapping, **direct_payment_award_mapping,
                            **other_award_mapping}
non_loan_assistance_award_mapping = assistance_award_mapping = {**grant_award_mapping, **direct_payment_award_mapping,
                                                                **other_award_mapping}

# TODO: include IDV mappings in the award_type_mapping and update award_filter.py
award_type_mapping = {
    'IDV_A': 'GWAC Government Wide Acquisition Contract',
    'IDV_B': 'IDC Multi-Agency Contract, Other Indefinite Delivery Contract',
    'IDV_B_A': 'IDC Indefinite Delivery Contract / Requirements',
    'IDV_B_B': 'IDC Indefinite Delivery Contract / Indefinite Quantity',
    'IDV_B_C': 'IDC Indefinite Delivery Contract / Definite Quantity',
    'IDV_C': 'FSS Federal Supply Schedule',
    'IDV_D': 'BOA Basic Ordering Agreement',
    'IDV_E': 'BPA Blanket Purchase Agreement',
    '02': 'Block Grant',
    '03': 'Formula Grant',
    '04': 'Project Grant',
    '05': 'Cooperative Agreement',
    '06': 'Direct Payment for Specified Use',
    '07': 'Direct Loan',
    '08': 'Guaranteed/Insured Loan',
    '09': 'Insurance',
    '10': 'Direct Payment with Unrestricted Use',
    '11': 'Other Financial Assistance',
    'A': 'BPA Call',
    'B': 'Purchase Order',
    'C': 'Delivery Order',
    'D': 'Definitive Contract',
    # While these types exist, they should not be included when returning all award types
    # 'E': 'Unknown Type',
    # 'F': 'Cooperative Agreement',
    # 'G': 'Grant for Research',
    # 'S': 'Funded Space Act Agreement',
    # 'T': 'Training Grant'
}
contract_type_mapping = {
    'A': 'BPA Call',
    'B': 'Purchase Order',
    'C': 'Delivery Order',
    'D': 'Definitive Contract'
}
idv_type_mapping = {
    'IDV_A': 'GWAC Government Wide Acquisition Contract',
    'IDV_B': 'IDC Multi-Agency Contract, Other Indefinite Delivery Contract',
    'IDV_B_A': 'IDC Indefinite Delivery Contract / Requirements',
    'IDV_B_B': 'IDC Indefinite Delivery Contract / Indefinite Quantity',
    'IDV_B_C': 'IDC Indefinite Delivery Contract / Definite Quantity',
    'IDV_C': 'FSS Federal Supply Schedule',
    'IDV_D': 'BOA Basic Ordering Agreement',
    'IDV_E': 'BPA Blanket Purchase Agreement'
}
grant_type_mapping = {
    '02': 'Block Grant',
    '03': 'Formula Grant',
    '04': 'Project Grant',
    '05': 'Cooperative Agreement'
}
direct_payment_type_mapping = {
    '06': 'Direct Payment for Specified Use',
    '10': 'Direct Payment with Unrestricted Use'
}
loan_type_mapping = {
    '07': 'Direct Loan',
    '08': 'Guaranteed/Insured Loan'
}
other_type_mapping = {
    '09': 'Insurance',
    '11': 'Other Financial Assistance'
}
assistance_type_mapping = {**grant_type_mapping, **direct_payment_type_mapping, **loan_type_mapping,
                           **other_type_mapping}
non_loan_assistance_type_mapping = {**grant_type_mapping, **direct_payment_type_mapping, **other_type_mapping}
all_award_types_mappings = {
    'contracts': list(contract_type_mapping),
    'direct_payments': list(direct_payment_type_mapping),
    'grants': list(grant_type_mapping),
    'idvs': list(idv_type_mapping),
    'loans': list(loan_type_mapping),
    'other_financial_assistance': list(other_type_mapping)
}
