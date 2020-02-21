"""
This file defines a series of constants that represent the values used in
the API's "helper" tables.

Rather than define the values in the db setup scripts and then make db calls to
lookup the surrogate keys, we'll define everything here, in a file that can be
used by the db setup scripts *and* the application code.
"""

from collections import namedtuple, OrderedDict

from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.accounts.v2.filters.account_download import account_download_filter
from usaspending_api.awards.models import Award, TransactionNormalized
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.download.helpers.elasticsearch_download_functions import (
    AwardsElasticsearchDownload,
    TransactionsElasticsearchDownload,
)
from usaspending_api.search.models import AwardSearchView, UniversalTransactionView, SubawardView
from usaspending_api.awards.v2.filters.idv_filters import (
    idv_order_filter,
    idv_transaction_filter,
    idv_treasury_account_funding_filter,
)
from usaspending_api.awards.v2.filters.award_filters import (
    awards_transaction_filter,
    awards_subaward_filter,
    awards_treasury_account_funding_filter,
)
from usaspending_api.awards.v2.filters.matview_filters import (
    universal_award_matview_filter,
    universal_transaction_matview_filter,
)
from usaspending_api.awards.v2.filters.sub_award import subaward_download
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping

from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.download.helpers.download_annotation_functions import (
    universal_transaction_matview_annotations,
    universal_award_matview_annotations,
    subaward_annotations,
    idv_order_annotations,
    idv_transaction_annotations,
)


LookupType = namedtuple("LookupType", ["id", "name", "desc"])

JOB_STATUS = [
    LookupType(1, "ready", "job is ready to be run"),
    LookupType(2, "running", "job is currently in progress"),
    LookupType(3, "finished", "job is complete"),
    LookupType(4, "failed", "job failed to complete"),
    LookupType(5, "queued", "job sent to queue for async processing"),
    LookupType(6, "resumed", "job is being reprocessed after a failure"),
    LookupType(7, "created", "job product has been created and stored locally"),
    LookupType(8, "uploading", "job is being uploaded to public storage"),
]

JOB_STATUS_DICT = {item.name: item.id for item in JOB_STATUS}

VALUE_MAPPINGS = {
    # Award Level
    "awards": {
        "source_type": "award",
        "table": AwardSearchView,
        "table_name": "award",
        "type_name": "PrimeAwardSummaries",
        "download_name": "{agency}{type}_PrimeAwardSummaries_{timestamp}",
        "contract_data": "award__latest_transaction__contract_data",
        "assistance_data": "award__latest_transaction__assistance_data",
        "filter_function": universal_award_matview_filter,
        "annotations_function": universal_award_matview_annotations,
    },
    # Elasticsearch Award Level
    # Lives alongside Postgres functionality for /api/v2/download/awards/ as of 1/17/2020
    "elasticsearch_awards": {
        "source_type": "award",
        "table": AwardSearchView,
        "table_name": "award",
        "type_name": "PrimeAwardSummaries",
        "download_name": "{agency}{type}_PrimeAwardSummaries_{timestamp}",
        "contract_data": "award__latest_transaction__contract_data",
        "assistance_data": "award__latest_transaction__assistance_data",
        "filter_function": AwardsElasticsearchDownload.query,
        "annotations_function": universal_award_matview_annotations,
    },
    # Transaction Level
    "transactions": {
        "source_type": "award",
        "table": UniversalTransactionView,
        "table_name": "transaction",
        "type_name": "PrimeTransactions",
        "download_name": "{agency}{type}_PrimeTransactions_{timestamp}",
        "contract_data": "transaction__contract_data",
        "assistance_data": "transaction__assistance_data",
        "filter_function": universal_transaction_matview_filter,
        "annotations_function": universal_transaction_matview_annotations,
    },
    # Elasticsearch Transaction Level
    # Lives alongside Postgres functionality for /api/v2/download/transactions/ as of 1/17/2020
    "elasticsearch_transactions": {
        "source_type": "award",
        "table": UniversalTransactionView,
        "table_name": "transaction",
        "type_name": "PrimeTransactions",
        "download_name": "{agency}{type}_PrimeTransactions_{timestamp}",
        "contract_data": "transaction__contract_data",
        "assistance_data": "transaction__assistance_data",
        "filter_function": TransactionsElasticsearchDownload.query,
        "annotations_function": universal_transaction_matview_annotations,
    },
    # SubAward Level
    "sub_awards": {
        "source_type": "award",
        "table": SubawardView,
        "table_name": "subaward",
        "type_name": "Subawards",
        "download_name": "{agency}{type}_Subawards_{timestamp}",
        "contract_data": "award__latest_transaction__contract_data",
        "assistance_data": "award__latest_transaction__assistance_data",
        "filter_function": subaward_download,
        "annotations_function": subaward_annotations,
    },
    # Appropriations Account Data
    "account_balances": {
        "source_type": "account",
        "table": AppropriationAccountBalances,
        "table_name": "account_balances",
        "download_name": "{data_quarters}_{agency}_{level}_AccountBalances_{timestamp}",
        "zipfile_template": "{data_quarters}_{agency}_{level}_AccountBalances_{timestamp}",
        "filter_function": account_download_filter,
    },
    # Object Class Program Activity Account Data
    "object_class_program_activity": {
        "source_type": "account",
        "table": FinancialAccountsByProgramActivityObjectClass,
        "table_name": "object_class_program_activity",
        "download_name": "{data_quarters}_{agency}_{level}_AccountBreakdownByPA-OC_{timestamp}",
        "zipfile_template": "{data_quarters}_{agency}_{level}_AccountBreakdownByPA-OC_{timestamp}",
        "filter_function": account_download_filter,
    },
    "award_financial": {
        "source_type": "account",
        "table": FinancialAccountsByAwards,
        "table_name": "award_financial",
        "download_name": "{data_quarters}_{agency}_{level}_AccountBreakdownByAward_{timestamp}",
        "zipfile_template": "{data_quarters}_{agency}_{level}_AccountBreakdownByAward_{timestamp}",
        "filter_function": account_download_filter,
    },
    "idv_orders": {
        "source_type": "award",
        "table": Award,
        "table_name": "idv_orders",
        "download_name": "IDV_{piid}_Orders",
        "contract_data": "latest_transaction__contract_data",
        "filter_function": idv_order_filter,
        "is_for_idv": True,
        "annotations_function": idv_order_annotations,
    },
    "idv_federal_account_funding": {
        "source_type": "account",
        "table": FinancialAccountsByAwards,
        "table_name": "award_financial",
        "download_name": "IDV_{piid}_FederalAccountFunding",
        "filter_function": idv_treasury_account_funding_filter,
        "is_for_idv": True,
    },
    "idv_transaction_history": {
        "source_type": "award",
        "table": TransactionNormalized,
        "table_name": "idv_transaction_history",
        "download_name": "IDV_{piid}_TransactionHistory",
        "contract_data": "contract_data",
        "filter_function": idv_transaction_filter,
        "is_for_idv": True,
        "annotations_function": idv_transaction_annotations,
    },
    "contract_federal_account_funding": {
        "source_type": "account",
        "table": FinancialAccountsByAwards,
        "table_name": "award_financial",
        "download_name": "Contract_{piid}_FederalAccountFunding",
        "filter_function": awards_treasury_account_funding_filter,
        "is_for_contract": True,
    },
    "assistance_federal_account_funding": {
        "source_type": "account",
        "table": FinancialAccountsByAwards,
        "table_name": "award_financial",
        "download_name": "Assistance_{assistance_id}_FederalAccountFunding",
        "filter_function": awards_treasury_account_funding_filter,
        "is_for_assistance": True,
    },
    "sub_contracts": {
        "source_type": "award",
        "table": SubawardView,
        "table_name": "subaward",
        "download_name": "Contract_{piid}_Sub-Awards",
        "contract_data": "award__latest_transaction__contract_data",
        "filter_function": awards_subaward_filter,
        "is_for_contract": True,
        "annotations_function": subaward_annotations,
    },
    "sub_grants": {
        "source_type": "award",
        "table": SubawardView,
        "table_name": "subaward",
        "download_name": "Assistance_{assistance_id}_Sub-Awards",
        "assistance_data": "award__latest_transaction__assistance_data",
        "filter_function": awards_subaward_filter,
        "is_for_assistance": True,
        "annotations_function": subaward_annotations,
    },
    "contract_transactions": {
        "source_type": "award",
        "table": TransactionNormalized,
        "table_name": "idv_transaction_history",
        "download_name": "Contract_{piid}_TransactionHistory",
        "contract_data": "contract_data",
        "filter_function": awards_transaction_filter,
        "is_for_contract": True,
        "annotations_function": idv_transaction_annotations,
    },
    "assistance_transactions": {
        "source_type": "award",
        "table": TransactionNormalized,
        "table_name": "assistance_transaction_history",
        "download_name": "Assistance_{assistance_id}_TransactionHistory",
        "assistance_data": "assistance_data",
        "filter_function": awards_transaction_filter,
        "is_for_assistance": True,
        "annotations_function": idv_transaction_annotations,
    },
}

# Bulk Download still uses "prime awards" instead of "transactions"
VALUE_MAPPINGS["prime_awards"] = VALUE_MAPPINGS["transactions"]

SHARED_AWARD_FILTER_DEFAULTS = {
    "award_type_codes": list(award_type_mapping.keys()),
    "agencies": [],
    "time_period": [],
    "place_of_performance_locations": [],
    "recipient_locations": [],
}
YEAR_CONSTRAINT_FILTER_DEFAULTS = {"elasticsearch_keyword": ""}
ROW_CONSTRAINT_FILTER_DEFAULTS = {
    "keywords": [],
    "legal_entities": [],
    "recipient_search_text": [],
    "recipient_scope": "",
    "recipient_type_names": [],
    "place_of_performance_scope": "",
    "award_amounts": [],
    "award_ids": [],
    "program_numbers": [],
    "naics_codes": [],
    "psc_codes": [],
    "contract_pricing_type_codes": [],
    "set_aside_type_codes": [],
    "extent_competed_type_codes": [],
    "federal_account_ids": [],
    "object_class_ids": [],
    "program_activity_ids": [],
}
ACCOUNT_FILTER_DEFAULTS = {
    "agency": "all",
    "federal_account": "all",
    "budget_function": "all",
    "budget_subfunction": "all",
}

# List of CFO CGACS for list agencies viewset in the correct order, names included for reference
# TODO: Find a solution that marks the CFO agencies in the database AND have the correct order
CFO_CGACS_MAPPING = OrderedDict(
    [
        ("012", "Department of Agriculture"),
        ("013", "Department of Commerce"),
        ("097", "Department of Defense"),
        ("091", "Department of Education"),
        ("089", "Department of Energy"),
        ("075", "Department of Health and Human Services"),
        ("070", "Department of Homeland Security"),
        ("086", "Department of Housing and Urban Development"),
        ("015", "Department of Justice"),
        ("1601", "Department of Labor"),
        ("019", "Department of State"),
        ("014", "Department of the Interior"),
        ("020", "Department of the Treasury"),
        ("069", "Department of Transportation"),
        ("036", "Department of Veterans Affairs"),
        ("068", "Environmental Protection Agency"),
        ("047", "General Services Administration"),
        ("080", "National Aeronautics and Space Administration"),
        ("049", "National Science Foundation"),
        ("031", "Nuclear Regulatory Commission"),
        ("024", "Office of Personnel Management"),
        ("073", "Small Business Administration"),
        ("028", "Social Security Administration"),
        ("072", "Agency for International Development"),
    ]
)
CFO_CGACS = list(CFO_CGACS_MAPPING.keys())

FILE_FORMATS = {
    "csv": {"delimiter": ",", "extension": "csv", "options": "WITH CSV HEADER"},
    "tsv": {"delimiter": "\t", "extension": "tsv", "options": r"WITH CSV DELIMITER E'\t' HEADER"},
    "pstxt": {"delimiter": "|", "extension": "txt", "options": "WITH CSV DELIMITER '|' HEADER"},
}

VALID_ACCOUNT_SUBMISSION_TYPES = ("account_balances", "object_class_program_activity", "award_financial")
