# This file defines a series of constants that represent the values used in the API's "helper" tables. Rather than
# define the values in the db setup scripts and then make db calls to lookup the surrogate keys, we'll define everything
# here, in a file that can be used by the db setup scripts *and* the application code.

from collections import namedtuple, OrderedDict

from usaspending_api.awards.models_matviews import UniversalAwardView, UniversalTransactionView
from usaspending_api.awards.models import Subaward
from usaspending_api.awards.v2.filters.matview_filters import universal_award_matview_filter, \
    universal_transaction_matview_filter
from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping

LookupType = namedtuple('LookupType', ['id', 'name', 'desc'])

JOB_STATUS = [
    LookupType(1, 'ready', 'job is ready to be run'),
    LookupType(2, 'running', 'job is currently in progress'),
    LookupType(3, 'finished', 'job is complete'),
    LookupType(4, 'failed', 'job failed to complete')
]
JOB_STATUS_DICT = {item.name: item.id for item in JOB_STATUS}
JOB_STATUS_DICT_ID = {item.id: item.name for item in JOB_STATUS}

VALUE_MAPPINGS = {
    # Award Level
    'awards': {
        'table': UniversalAwardView,
        'table_name': 'award',
        'download_name': 'prime_awards',
        'contract_data': 'award__latest_transaction__contract_data',
        'assistance_data': 'award__latest_transaction__assistance_data',
        'filter_function': universal_award_matview_filter
    },
    # Transaction Level
    'transactions': {
        'table': UniversalTransactionView,
        'table_name': 'transaction',
        'download_name': 'prime_transactions',
        'contract_data': 'transaction__contract_data',
        'assistance_data': 'transaction__assistance_data',
        'filter_function': universal_transaction_matview_filter
    },
    # SubAward Level
    'sub_awards': {
        'table': Subaward,
        'table_name': 'subaward',
        'download_name': 'subawards',
        'contract_data': 'award__latest_transaction__contract_data',
        'assistance_data': 'award__latest_transaction__assistance_data',
        'filter_function': subaward_filter
    }
}
# Bulk Download still uses "prime awards" instead of "transactions"
VALUE_MAPPINGS['prime_awards'] = VALUE_MAPPINGS['transactions']

SHARED_FILTER_DEFAULTS = {
    'award_type_codes': list(award_type_mapping.keys()),
    'agencies': [],
    'time_period': [],
    'place_of_performance_locations': [],
    'recipient_locations': []
}
YEAR_CONSTRAINT_FILTER_DEFAULTS = {'elasticsearch_keyword': ''}
ROW_CONSTRAINT_FILTER_DEFAULTS = {
    'keyword': '',
    'legal_entities': [],
    'recipient_search_text': [],
    'recipient_scope': '',
    'recipient_type_names': [],
    'place_of_performance_scope': '',
    'award_amounts': [],
    'award_ids': [],
    'program_numbers': [],
    'naics_codes': [],
    'psc_codes': [],
    'contract_pricing_type_codes': [],
    'set_aside_type_codes': [],
    'extent_competed_type_codes': [],
    'federal_account_ids': [],
    'object_class_ids': [],
    'program_activity_ids': []
}

# List of CFO CGACS for list agencies viewset in the correct order, names included for reference
# TODO: Find a solution that marks the CFO agencies in the database AND have the correct order
CFO_CGACS_MAPPING = OrderedDict([('012', 'Department of Agriculture'),
                                 ('013', 'Department of Commerce'),
                                 ('097', 'Department of Defense'),
                                 ('091', 'Department of Education'),
                                 ('089', 'Department of Energy'),
                                 ('075', 'Department of Health and Human Services'),
                                 ('070', 'Department of Homeland Security'),
                                 ('086', 'Department of Housing and Urban Development'),
                                 ('015', 'Department of Justice'),
                                 ('1601', 'Department of Labor'),
                                 ('019', 'Department of State'),
                                 ('014', 'Department of the Interior'),
                                 ('020', 'Department of the Treasury'),
                                 ('069', 'Department of Transportation'),
                                 ('036', 'Department of Veterans Affairs'),
                                 ('068', 'Environmental Protection Agency'),
                                 ('047', 'General Services Administration'),
                                 ('080', 'National Aeronautics and Space Administration'),
                                 ('049', 'National Science Foundation'),
                                 ('031', 'Nuclear Regulatory Commission'),
                                 ('024', 'Office of Personnel Management'),
                                 ('073', 'Small Business Administration'),
                                 ('028', 'Social Security Administration'),
                                 ('072', 'Agency for International Development')])
CFO_CGACS = list(CFO_CGACS_MAPPING.keys())
