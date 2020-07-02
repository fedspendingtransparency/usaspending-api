"""
Look ups for elasticsearch fields to be displayed for the front end
"""
from copy import deepcopy
from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings


TRANSACTIONS_LOOKUP = {
    "Recipient Name": "recipient_name.keyword",
    "Action Date": "action_date",
    "Transaction Amount": "transaction_amount",
    "Award Type": "type_description.keyword",
    "Awarding Agency": "awarding_toptier_agency_name.keyword",
    "Awarding Sub Agency": "awarding_subtier_agency_name.keyword",
    "Funding Agency": "funding_toptier_agency_name",
    "Funding Sub Agency": "funding_subtier_agency_name",
    "Issued Date": "period_of_performance_start_date",
    "Loan Value": "face_value_loan_guarantee",
    "Subsidy Cost": "original_loan_subsidy_cost",
    "Mod": "modification_number.keyword",
    "Award ID": "display_award_id",
    "awarding_agency_id": "awarding_agency_id",
    "internal_id": "award_id",
    "generated_internal_id": "generated_unique_award_id",
    "Last Date to Order": "ordering_period_end_date",
    "def_codes": "disaster_emergency_fund_codes",
}

base_mapping = {
    "Award ID": "display_award_id",
    "Recipient Name": "recipient_name.keyword",
    "Recipient DUNS Number": "recipient_unique_id.keyword",
    "recipient_id": "recipient_unique_id.keyword",
    "Awarding Agency": "awarding_toptier_agency_name.keyword",
    "Awarding Agency Code": "awarding_toptier_agency_code.keyword",
    "Awarding Sub Agency": "awarding_subtier_agency_name.keyword",
    "Awarding Sub Agency Code": "awarding_subtier_agency_code.keyword",
    "Funding Agency": "funding_toptier_agency_name.keyword",
    "Funding Agency Code": "funding_toptier_agency_code.keyword",
    "Funding Sub Agency": "funding_subtier_agency_name.keyword",
    "Funding Sub Agency Code": "funding_subtier_agency_code.keyword",
    "Place of Performance City Code": "pop_city_code.keyword",
    "Place of Performance State Code": "pop_state_code",
    "Place of Performance Country Code": "pop_country_code",
    "Place of Performance Zip5": "pop_zip5.keyword",
    "Description": "description.keyword",
    "Last Modified Date": "last_modified_date",
    "Base Obligation Date": "date_signed",
    "prime_award_recipient_id": "prime_award_recipient_id",
    "generated_internal_id": "generated_unique_award_id",
    "def_codes": "disaster_emergency_fund_codes",
    "COVID-19 Obligations": "total_covid_obligation",
    "COVID-19 Outlays": "total_covid_outlay",
}
contracts_mapping = {
    **base_mapping,
    **{
        "Start Date": "period_of_performance_start_date",
        "End Date": "period_of_performance_current_end_date",
        "Award Amount": "total_obligation",
        "Contract Award Type": "type_description",
    },
}
idv_mapping = {
    **base_mapping,
    **{
        "Start Date": "period_of_performance_start_date",
        "Award Amount": "total_obligation",
        "Contract Award Type": "type_description",
        "Last Date to Order": "ordering_period_end_date",
    },
}
loan_mapping = {
    **base_mapping,
    **{
        "Issued Date": "action_date",
        "Loan Value": "total_loan_value",
        "Subsidy Cost": "total_subsidy_cost",
        "SAI Number": "sai_number.keyword",
        "CFDA Number": "cfda_number.keyword",
    },
}
non_loan_assist_mapping = {
    **base_mapping,
    **{
        "Start Date": "period_of_performance_start_date",
        "End Date": "period_of_performance_current_end_date",
        "Award Amount": "total_obligation",
        "Award Type": "type_description",
        "SAI Number": "sai_number.keyword",
        "CFDA Number": "cfda_number.keyword",
    },
}

TRANSACTIONS_SOURCE_LOOKUP = {key: value.replace(".keyword", "") for key, value in TRANSACTIONS_LOOKUP.items()}

CONTRACT_SOURCE_LOOKUP = {key: value.replace(".keyword", "") for key, value in contracts_mapping.items()}
IDV_SOURCE_LOOKUP = {key: value.replace(".keyword", "") for key, value in idv_mapping.items()}
NON_LOAN_ASST_SOURCE_LOOKUP = {key: value.replace(".keyword", "") for key, value in non_loan_assist_mapping.items()}
LOAN_SOURCE_LOOKUP = {key: value.replace(".keyword", "") for key, value in loan_mapping.items()}

INDEX_ALIASES_TO_AWARD_TYPES = deepcopy(all_award_types_mappings)
INDEX_ALIASES_TO_AWARD_TYPES["directpayments"] = INDEX_ALIASES_TO_AWARD_TYPES.pop("direct_payments")
INDEX_ALIASES_TO_AWARD_TYPES["other"] = INDEX_ALIASES_TO_AWARD_TYPES.pop("other_financial_assistance")

KEYWORD_DATATYPE_FIELDS = [
    "recipient_name.keyword",
    "awarding_toptier_agency_name.keyword",
    "awarding_subtier_agency_name.keyword",
    "type_description.keyword",
]
