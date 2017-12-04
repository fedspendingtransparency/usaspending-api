from usaspending_api.awards.models_matviews import SumaryCfdaNumbersView
from usaspending_api.awards.models_matviews import SumaryNaicsCodesView
from usaspending_api.awards.models_matviews import SumaryPscCodesView
from usaspending_api.awards.models_matviews import SummaryAwardView
from usaspending_api.awards.models_matviews import SummaryTransactionView
from usaspending_api.awards.models_matviews import SummaryView
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.v2.filters.matview_transaction import transaction_filter
from usaspending_api.awards.v2.filters.matview_award import award_filter
import logging

logger = logging.getLogger(__name__)

MATVIEW_SELECTOR = {
    'SummaryView': {
        'allowed_filters': ['time_period', 'award_type_codes', 'agencies'],
        'model': SummaryView,
        'base_model': 'transaction'
    },
    'SummaryAwardView': {
        'allowed_filters': ['time_period', 'award_type_codes', 'agencies'],
        'model': SummaryAwardView,
        'base_model': 'award'
    },
    'SumaryPscCodesView': {
        'allowed_filters': ['time_period', 'award_type_codes'],
        'model': SumaryPscCodesView,
        'base_model': 'transaction'
    },
    'SumaryCfdaNumbersView': {
        'allowed_filters': ['time_period', 'award_type_codes'],
        'model': SumaryCfdaNumbersView,
        'base_model': 'transaction'
    },
    'SumaryNaicsCodesView': {
        'allowed_filters': ['time_period', 'award_type_codes'],
        'model': SumaryNaicsCodesView,
        'base_model': 'transaction'
    },
    'SummaryTransactionView': {
        'allowed_filters': [
            'time_period',
            'award_type_codes',
            'legal_entities',
            'recipient_scope',
            'recipient_locations',
            'recipient_type_names',
            'place_of_performance_scope',
            'place_of_performance_locations',
            'naics_codes',
            'psc_codes',
            'contract_pricing_type_codes',
            'set_aside_type_codes',
            'extent_competed_type_codes'],
        'model': SummaryTransactionView,
        'base_model': 'transaction'
    }
}


def get_view_queryset(filters, view_name):
    try:
        view_model = MATVIEW_SELECTOR[view_name]['model']
    except Exception:
        raise InvalidParameterException('Invalid view: ' + view_name + ' does not exist.')

    if MATVIEW_SELECTOR[view_name]['base_model'] == 'award':
        queryset = award_filter(filters, view_model)
    else:
        queryset = transaction_filter(filters, view_model)

    return queryset


def can_use_view(filters, view_name):
    try:
        key_list = MATVIEW_SELECTOR[view_name]['allowed_filters']
    except KeyError:
        return False

    # Make sure *only* acceptable keys are in the filters for that view_name
    if not set(key_list).issuperset(set(filters.keys())):
        return False

    agencies = filters.get('agencies')
    if agencies:
        for v in agencies:
            if v["tier"] == "subtier":
                return False
    return True
