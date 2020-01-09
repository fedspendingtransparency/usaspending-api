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
other_type_mapping = {"09": "Insurance", "11": "Other Financial Assistance"}
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
