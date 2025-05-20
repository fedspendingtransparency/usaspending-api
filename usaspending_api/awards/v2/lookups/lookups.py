grant_award_mapping = {
    "Award ID": "fain",
    "Recipient Name": "latest_transaction__assistance_data__awardee_or_recipient_legal",
    "Start Date": "period_of_performance_start_date",
    "End Date": "period_of_performance_current_end_date",
    "Award Amount": "total_obligation",
    "Awarding Agency": "awarding_agency__toptier_agency__name",
    "Awarding Sub Agency": "awarding_agency__subtier_agency__name",
    "Award Type": "type_description",
    "Funding Agency": "funding_agency__toptier_agency__name",
    "Funding Sub Agency": "funding_agency__subtier_agency__name",
}

loan_award_mapping = {
    "Award ID": "fain",
    "Recipient Name": "latest_transaction__assistance_data__awardee_or_recipient_legal",
    "Issued Date": "latest_transaction__action_date",
    "Loan Value": "latest_transaction__assistance_data__face_value_loan_guarantee",
    "Subsidy Cost": "latest_transaction__assistance_data__original_loan_subsidy_cost",
    "Awarding Agency": "awarding_agency__toptier_agency__name",
    "Awarding Sub Agency": "awarding_agency__subtier_agency__name",
    "Funding Agency": "funding_agency__toptier_agency__name",
    "Funding Sub Agency": "funding_agency__subtier_agency__name",
}

direct_payment_award_mapping = {
    "Award ID": "fain",
    "Recipient Name": "latest_transaction__assistance_data__awardee_or_recipient_legal",
    "Start Date": "period_of_performance_start_date",
    "End Date": "period_of_performance_current_end_date",
    "Award Amount": "total_obligation",
    "Awarding Agency": "awarding_agency__toptier_agency__name",
    "Awarding Sub Agency": "awarding_agency__subtier_agency__name",
    "Award Type": "type_description",
    "Funding Agency": "funding_agency__toptier_agency__name",
    "Funding Sub Agency": "funding_agency__subtier_agency__name",
}

other_award_mapping = {
    "Award ID": "fain",
    "Recipient Name": "latest_transaction__assistance_data__awardee_or_recipient_legal",
    "Start Date": "period_of_performance_start_date",
    "End Date": "period_of_performance_current_end_date",
    "Award Amount": "total_obligation",
    "Awarding Agency": "awarding_agency__toptier_agency__name",
    "Awarding Sub Agency": "awarding_agency__subtier_agency__name",
    "Award Type": "type_description",
    "Funding Agency": "funding_agency__toptier_agency__name",
    "Funding Sub Agency": "funding_agency__subtier_agency__name",
}

subaward_mapping = {
    "Sub-Award ID": "subaward_number",
    "Sub-Award Type": "subaward_type.keyword",
    "Sub-Awardee Name": "sub_awardee_or_recipient_legal.keyword",
    "Sub-Award Date": "sub_action_date",
    "Sub-Award Amount": "subaward_amount",
    "Awarding Agency": "awarding_toptier_agency_name.keyword",
    "Awarding Sub Agency": "awarding_subtier_agency_name.keyword",
    "Prime Award ID": "award_piid_fain",
    "Prime Recipient Name": "awardee_or_recipient_legal.keyword",
    "prime_award_recipient_id": "prime_award_recipient_id",
    "Sub-Award Description": "subaward_description",
    "subaward_description_sorted": "subaward_description_sort",
    "Sub-Recipient UEI": "sub_awardee_or_recipient_uei",
    "Prime Award Recipient UEI": "awardee_or_recipient_uei.keyword",
}

award_assistance_mapping = {
    **grant_award_mapping,
    **loan_award_mapping,
    **direct_payment_award_mapping,
    **other_award_mapping,
}
non_loan_assistance_award_mapping = {**grant_award_mapping, **direct_payment_award_mapping, **other_award_mapping}

# TODO: include IDV mappings in the award_type_mapping and update award_filter.py
award_type_mapping = {
    "IDV_A": "GWAC Government Wide Acquisition Contract",
    "IDV_B": "IDC Multi-Agency Contract, Other Indefinite Delivery Contract",
    "IDV_B_A": "IDC Indefinite Delivery Contract / Requirements",
    "IDV_B_B": "IDC Indefinite Delivery Contract / Indefinite Quantity",
    "IDV_B_C": "IDC Indefinite Delivery Contract / Definite Quantity",
    "IDV_C": "FSS Federal Supply Schedule",
    "IDV_D": "BOA Basic Ordering Agreement",
    "IDV_E": "BPA Blanket Purchase Agreement",
    "02": "Block Grant",
    "03": "Formula Grant",
    "04": "Project Grant",
    "05": "Cooperative Agreement",
    "06": "Direct Payment for Specified Use",
    "07": "Direct Loan",
    "08": "Guaranteed/Insured Loan",
    "09": "Insurance",
    "10": "Direct Payment with Unrestricted Use",
    "11": "Other Financial Assistance",
    "A": "BPA Call",
    "B": "Purchase Order",
    "C": "Delivery Order",
    "D": "Definitive Contract",
    # -1 is a derived type that we added as a "catch-all" for any invalid `type` values
    "-1": "Not Specified",
    # While these types exist, they should not be included when returning all award types
    # 'E': 'Unknown Type',
    # 'F': 'Cooperative Agreement',
    # 'G': 'Grant for Research',
    # 'S': 'Funded Space Act Agreement',
    # 'T': 'Training Grant'
}
contract_type_mapping = {"A": "BPA Call", "B": "Purchase Order", "C": "Delivery Order", "D": "Definitive Contract"}
idv_type_mapping = {
    "IDV_A": "GWAC Government Wide Acquisition Contract",
    "IDV_B": "IDC Multi-Agency Contract, Other Indefinite Delivery Contract",
    "IDV_B_A": "IDC Indefinite Delivery Contract / Requirements",
    "IDV_B_B": "IDC Indefinite Delivery Contract / Indefinite Quantity",
    "IDV_B_C": "IDC Indefinite Delivery Contract / Definite Quantity",
    "IDV_C": "FSS Federal Supply Schedule",
    "IDV_D": "BOA Basic Ordering Agreement",
    "IDV_E": "BPA Blanket Purchase Agreement",
}
grant_type_mapping = {"02": "Block Grant", "03": "Formula Grant", "04": "Project Grant", "05": "Cooperative Agreement"}
direct_payment_type_mapping = {"06": "Direct Payment for Specified Use", "10": "Direct Payment with Unrestricted Use"}
loan_type_mapping = {"07": "Direct Loan", "08": "Guaranteed/Insured Loan"}
# -1 is a derived type that we added as a "catch-all" for any invalid `type` values
other_type_mapping = {"09": "Insurance", "11": "Other Financial Assistance", "-1": "Not Specified"}
assistance_type_mapping = {
    **grant_type_mapping,
    **direct_payment_type_mapping,
    **loan_type_mapping,
    **other_type_mapping,
}
non_loan_assistance_type_mapping = {**grant_type_mapping, **direct_payment_type_mapping, **other_type_mapping}
procurement_type_mapping = {**contract_type_mapping, **idv_type_mapping}
all_award_types_mappings = {
    "contracts": list(contract_type_mapping),
    "direct_payments": list(direct_payment_type_mapping),
    "grants": list(grant_type_mapping),
    "idvs": list(idv_type_mapping),
    "loans": list(loan_type_mapping),
    "other_financial_assistance": list(other_type_mapping),
}

all_awards_types_to_category = {
    type_code: category for category, type_codes in all_award_types_mappings.items() for type_code in type_codes
}

all_subaward_types = ["grant", "procurement"]

SUBAWARD_MAPPING_LOOKUP = {key: value.replace(".keyword", "") for key, value in subaward_mapping.items()}
