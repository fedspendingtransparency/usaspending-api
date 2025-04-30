"""
This file defines a series of constants that represent the values used in
the API's "helper" tables.

Rather than define the values in the db setup scripts and then make db calls to
lookup the surrogate keys, we'll define everything here, in a file that can be
used by the db setup scripts *and* the application code.
"""

from collections import namedtuple, OrderedDict

from usaspending_api.accounts.v2.filters.account_download import account_download_filter
from usaspending_api.download.helpers.elasticsearch_download_functions import (
    AwardsElasticsearchDownload,
    SubawardsElasticsearchDownload,
    TransactionsElasticsearchDownload,
)
from usaspending_api.download.helpers.disaster_filter_functions import disaster_filter_function
from usaspending_api.download.models import (
    AppropriationAccountBalancesDownloadView,
    FinancialAccountsByAwardsDownloadView,
    FinancialAccountsByProgramActivityObjectClassDownloadView,
)
from usaspending_api.references.models import GTASSF133Balances
from usaspending_api.search.models import AwardSearch, SubawardSearch, TransactionSearch
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
from usaspending_api.awards.v2.filters.search import (
    transaction_search_filter,
)
from usaspending_api.awards.v2.filters.sub_award import subaward_download
from usaspending_api.download.helpers.download_annotation_functions import (
    award_annotations,
    subaward_annotations,
    idv_order_annotations,
    idv_transaction_annotations,
    transaction_search_annotations,
    object_class_program_activity_annotations,
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
    # Elasticsearch Award Level
    "elasticsearch_awards": {
        "source_type": "award",
        "table": AwardSearch,
        "table_name": "award",
        "type_name": "PrimeAwardSummaries",
        "download_name": "{agency}{type}_PrimeAwardSummaries_{timestamp}",
        "is_fpds_join": "",
        "filter_function": AwardsElasticsearchDownload.query,
        "annotations_function": award_annotations,
    },
    # Transaction Level
    "transactions": {
        "source_type": "award",
        "table": TransactionSearch,
        "table_name": "transaction_search",
        "type_name": "PrimeTransactions",
        "download_name": "{agency}{type}_PrimeTransactions_{timestamp}",
        "is_fpds_join": "",
        "filter_function": transaction_search_filter,
        "annotations_function": transaction_search_annotations,
    },
    # Elasticsearch Transaction Level
    "elasticsearch_transactions": {
        "source_type": "award",
        "table": TransactionSearch,
        "table_name": "transaction_search",
        "type_name": "PrimeTransactions",
        "download_name": "{agency}{type}_PrimeTransactions_{timestamp}",
        "is_fpds_join": "",
        "filter_function": TransactionsElasticsearchDownload.query,
        "annotations_function": transaction_search_annotations,
    },
    # SubAward Level
    "sub_awards": {
        "source_type": "award",
        "table": SubawardSearch,
        "table_name": "subaward_search",
        "type_name": "Subawards",
        "download_name": "{agency}{type}_Subawards_{timestamp}",
        "is_fpds_join": "latest_transaction__",
        "filter_function": subaward_download,
        "annotations_function": subaward_annotations,
    },
    "elasticsearch_sub_awards": {
        "source_type": "award",
        "table": SubawardSearch,
        "table_name": "subaward_search",
        "type_name": "Subawards",
        "download_name": "{agency}{type}_Subawards_{timestamp}",
        "is_fpds_join": "latest_transaction__",
        "filter_function": SubawardsElasticsearchDownload.query,
        "annotations_function": subaward_annotations,
    },
    # Appropriations Account Data
    "account_balances": {
        "source_type": "account",
        "table": AppropriationAccountBalancesDownloadView,
        "table_name": "account_balances",
        "download_name": "{data_quarters}_{agency}_{level}_AccountBalances_{timestamp}",
        "zipfile_template": "{data_quarters}_{agency}_{level}_AccountBalances_{timestamp}",
        "filter_function": account_download_filter,
    },
    "gtas_balances": {
        "source_type": "account",
        "table": GTASSF133Balances,
        "table_name": "gtas_balances",
        "download_name": "{data_quarters}_{agency}_{level}_AccountBalances_{timestamp}",
        "zipfile_template": "{data_quarters}_{agency}_{level}_AccountBalances_{timestamp}",
        "filter_function": account_download_filter,
    },
    # Object Class Program Activity Account Data
    "object_class_program_activity": {
        "source_type": "account",
        "table": FinancialAccountsByProgramActivityObjectClassDownloadView,
        "table_name": "object_class_program_activity",
        "download_name": "{data_quarters}_{agency}_{level}_AccountBreakdownByPA-OC_{timestamp}",
        "zipfile_template": "{data_quarters}_{agency}_{level}_AccountBreakdownByPA-OC_{timestamp}",
        "filter_function": account_download_filter,
        "annotations_function": object_class_program_activity_annotations,
    },
    "award_financial": {
        "source_type": "account",
        "table": FinancialAccountsByAwardsDownloadView,
        "table_name": "award_financial",
        "download_name": "{data_quarters}_{agency}_{level}_{extra_file_type}AccountBreakdownByAward_{timestamp}",
        "zipfile_template": "{data_quarters}_{agency}_{level}_AccountBreakdownByAward_{timestamp}",
        "is_fpds_join": "award__latest_transaction_search__",
        "filter_function": account_download_filter,
    },
    "idv_orders": {
        "source_type": "award",
        "table": AwardSearch,
        "table_name": "idv_orders",
        "download_name": "IDV_{piid}_Orders",
        "is_fpds_join": "",
        "filter_function": idv_order_filter,
        "is_for_idv": True,
        "annotations_function": idv_order_annotations,
    },
    "idv_federal_account_funding": {
        "source_type": "account",
        "table": FinancialAccountsByAwardsDownloadView,
        "table_name": "award_financial",
        "download_name": "IDV_{piid}_FederalAccountFunding",
        "filter_function": idv_treasury_account_funding_filter,
        "is_for_idv": True,
    },
    "idv_transaction_history": {
        "source_type": "award",
        "table": TransactionSearch,
        "table_name": "idv_transaction_history",
        "download_name": "IDV_{piid}_TransactionHistory",
        "is_fpds_join": "",
        "filter_function": idv_transaction_filter,
        "is_for_idv": True,
        "annotations_function": idv_transaction_annotations,
    },
    "contract_federal_account_funding": {
        "source_type": "account",
        "table": FinancialAccountsByAwardsDownloadView,
        "table_name": "award_financial",
        "download_name": "Contract_{piid}_FederalAccountFunding",
        "filter_function": awards_treasury_account_funding_filter,
        "is_for_contract": True,
    },
    "assistance_federal_account_funding": {
        "source_type": "account",
        "table": FinancialAccountsByAwardsDownloadView,
        "table_name": "award_financial",
        "download_name": "Assistance_{assistance_id}_FederalAccountFunding",
        "filter_function": awards_treasury_account_funding_filter,
        "is_for_assistance": True,
    },
    "sub_contracts": {
        "source_type": "award",
        "table": SubawardSearch,
        "table_name": "subaward_search",
        "download_name": "Contract_{piid}_Sub-Awards",
        "is_fpds_join": "latest_transaction__",
        "filter_function": awards_subaward_filter,
        "is_for_contract": True,
        "annotations_function": subaward_annotations,
    },
    "sub_grants": {
        "source_type": "award",
        "table": SubawardSearch,
        "table_name": "subaward_search",
        "download_name": "Assistance_{assistance_id}_Sub-Awards",
        "is_fpds_join": "latest_transaction__",
        "filter_function": awards_subaward_filter,
        "is_for_assistance": True,
        "annotations_function": subaward_annotations,
    },
    "contract_transactions": {
        "source_type": "award",
        "table": TransactionSearch,
        "table_name": "idv_transaction_history",
        "download_name": "Contract_{piid}_TransactionHistory",
        "is_fpds_join": "",
        "filter_function": awards_transaction_filter,
        "is_for_contract": True,
        "annotations_function": idv_transaction_annotations,
    },
    "assistance_transactions": {
        "source_type": "award",
        "table": TransactionSearch,
        "table_name": "assistance_transaction_history",
        "download_name": "Assistance_{assistance_id}_TransactionHistory",
        "is_fpds_join": "",
        "filter_function": awards_transaction_filter,
        "is_for_assistance": True,
        "annotations_function": idv_transaction_annotations,
    },
    "disaster_recipient": {
        "source_type": "disaster",
        "table": AwardSearch,
        "table_name": "recipient",
        "download_name": "COVID-19_Recipients_{award_category}_{timestamp}",
        "filter_function": disaster_filter_function,
        "base_fields": ["recipient_name", "recipient_unique_id"],
    },
}

# Bulk Download still uses "prime awards" instead of "transactions"
VALUE_MAPPINGS["prime_awards"] = VALUE_MAPPINGS["transactions"]

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
