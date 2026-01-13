"""
Lookups for elasticsearch fields to be displayed for the front end
"""

from copy import deepcopy
from dataclasses import dataclass
from enum import Enum

from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings


@dataclass
class ElasticsearchField:
    """
    Represents a field that is searchable by an API endpoint and pairs it with the corresponding elasticsearch field.

    Args:
        field_name: The name of the field provided by the user when selecting fields and returned by the API
        full_path: A complete path that may include additional field types such as ".keyword"
        short_path: The full_path with any additional field types removed; may be 1:1 with full_path
    """

    field_name: str
    full_path: str
    short_path: str


class TransactionField(str, Enum):
    ACTION_DATE = ("Action Date", "action_date")
    ACTION_TYPE = ("Action Type", "action_type")
    AWARD_ID = ("Award ID", "display_award_id")
    AWARD_TYPE = ("Award Type", "type_description.keyword")
    AWARDING_AGENCY = ("Awarding Agency", "awarding_toptier_agency_name.keyword")
    AWARDING_AGENCY_ID = ("awarding_agency_id", "awarding_agency_id")
    AWARDING_AGENCY_SLUG = ("awarding_agency_slug", "awarding_toptier_agency_name.keyword")
    AWARDING_SUB_AGENCY = ("Awarding Sub Agency", "awarding_subtier_agency_name.keyword")
    CFDA_NUMBER = ("cfda_number", "cfda_number.keyword")
    CFDA_TITLE = ("cfda_title", "cfda_title.keyword")
    DEF_CODES = ("def_codes", "disaster_emergency_fund_codes")
    FUNDING_AGENCY = ("Funding Agency", "funding_toptier_agency_name.keyword")
    FUNDING_AGENCY_SLUG = ("funding_agency_slug", "funding_toptier_agency_name.keyword")
    FUNDING_SUB_AGENCY = ("Funding Sub Agency", "funding_subtier_agency_name.keyword")
    GENERATED_INTERNAL_ID = ("generated_internal_id", "generated_unique_award_id")
    INTERNAL_ID = ("internal_id", "award_id")
    ISSUED_DATE = ("Issued Date", "period_of_performance_start_date")
    LAST_DATE_TO_ORDER = ("Last Date to Order", "ordering_period_end_date")
    LOAN_VALUE = ("Loan Value", "face_value_loan_guarantee")
    MOD = ("Mod", "modification_number.keyword")
    NAICS_CODE = ("naics_code", "naics_code.keyword")
    NAICS_DESCRIPTION = ("naics_description", "naics_description.keyword")
    POP_CITY_NAME = ("pop_city_name", "pop_city_name.keyword")
    POP_COUNTRY_NAME = ("pop_country_name", "pop_country_name.keyword")
    POP_STATE_CODE = ("pop_state_code", "pop_state_code")
    PSC_CODE = ("product_or_service_code", "product_or_service_code.keyword")
    PSC_DESCRIPTION = ("product_or_service_description", "product_or_service_description.keyword")
    RECIPIENT_ID = ("recipient_id", "recipient_agg_key")
    RECIPIENT_LOCATION_ADDRESS_LINE_1 = ("recipient_location_address_line1", "recipient_location_address_line1.keyword")
    RECIPIENT_LOCATION_ADDRESS_LINE_2 = ("recipient_location_address_line2", "recipient_location_address_line2.keyword")
    RECIPIENT_LOCATION_ADDRESS_LINE_3 = ("recipient_location_address_line3", "recipient_location_address_line3.keyword")
    RECIPIENT_LOCATION_CITY_NAME = ("recipient_location_city_name", "recipient_location_city_name.keyword")
    RECIPIENT_LOCATION_COUNTRY_NAME = ("recipient_location_country_name", "recipient_location_country_name.keyword")
    RECIPIENT_LOCATION_STATE_CODE = ("recipient_location_state_code", "recipient_location_state_code")
    RECIPIENT_NAME = ("Recipient Name", "recipient_name.keyword")
    RECIPIENT_UEI = ("Recipient UEI", "recipient_uei.keyword")
    SUBSIDY_COST = ("Subsidy Cost", "original_loan_subsidy_cost")
    TRANSACTION_AMOUNT = ("Transaction Amount", "federal_action_obligation")
    TRANSACTION_DESCRIPTION = ("Transaction Description", "transaction_description.keyword")

    def __new__(cls, field_name: str, full_path: str) -> "str":
        obj = str.__new__(cls, field_name)
        obj._value_ = field_name
        short_path = full_path.split(".")[0]
        obj._es_field = ElasticsearchField(field_name, full_path, short_path)
        return obj

    @property
    def field_name(self) -> str:
        return self._es_field.field_name

    @property
    def full_path(self) -> str:
        return self._es_field.full_path

    @property
    def short_path(self) -> str:
        return self._es_field.short_path


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
    "COVID-19 Obligations": "spending_by_defc",
    "COVID-19 Outlays": "spending_by_defc",
    "Infrastructure Obligations": "spending_by_defc",
    "Infrastructure Outlays": "spending_by_defc",
    "Recipient UEI": "recipient_uei.keyword",
    "naics_code": "naics_code.keyword",
    "naics_description": "naics_description.keyword",
    "psc_code": "product_or_service_code.keyword",
    "psc_description": "product_or_service_description.keyword",
    "cfda_number": "cfda_number.keyword",
    "cfda_program_title": "cfda_title.keyword",
    "sub_cfda_program_titles": "cfda_titles.keyword",
    "recipient_location_city_name": "recipient_location_city_name.keyword",
    "recipient_location_state_code": "recipient_location_state_code",
    "recipient_location_country_name": "recipient_location_country_name.keyword",
    "recipient_location_address_line1": "recipient_location_address_line1.keyword",
    "recipient_location_address_line2": "recipient_location_address_line2.keyword",
    "recipient_location_address_line3": "recipient_location_address_line3.keyword",
    "sub_recipient_location_city_name": "sub_recipient_location_city_name.keyword",
    "sub_recipient_location_state_code": "sub_recipient_location_state_code",
    "sub_recipient_location_country_name": "sub_recipient_location_country_name.keyword",
    "sub_recipient_location_address_line1": "sub_recipient_location_address_line1.keyword",
    "pop_city_name": "pop_city_name.keyword",
    "pop_state_code": "pop_state_code",
    "pop_country_name": "pop_country_name.keyword",
    "sub_pop_city_name": "sub_pop_city_name.keyword",
    "sub_pop_state_code": "sub_pop_state_code",
    "sub_pop_country_name": "sub_pop_country_name.keyword",
}
contracts_mapping = {
    **base_mapping,
    **{
        "Start Date": "period_of_performance_start_date",
        "End Date": "period_of_performance_current_end_date",
        "Award Amount": "total_obligation",
        "Total Outlays": "total_outlays",
        "Contract Award Type": "type_description.keyword",
    },
}
idv_mapping = {
    **base_mapping,
    **{
        "Start Date": "period_of_performance_start_date",
        "Award Amount": "total_obligation",
        "Total Outlays": "total_outlays",
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
        "Assistance Listings": "cfdas",
    },
}
non_loan_assist_mapping = {
    **base_mapping,
    **{
        "Start Date": "period_of_performance_start_date",
        "End Date": "period_of_performance_current_end_date",
        "Award Amount": "total_obligation",
        "Total Outlays": "total_outlays",
        "Award Type": "type_description",
        "SAI Number": "sai_number.keyword",
        "CFDA Number": "cfda_number.keyword",
        "Assistance Listings": "cfdas",
    },
}

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
