from usaspending_api.awards.models_matviews import SumaryCfdaNumbersView
from usaspending_api.awards.models_matviews import SumaryNaicsCodesView
from usaspending_api.awards.models_matviews import SumaryPscCodesView
from usaspending_api.awards.models_matviews import SummaryAwardView
from usaspending_api.awards.models_matviews import SummaryTransactionMonthView
from usaspending_api.awards.models_matviews import SummaryTransactionView
from usaspending_api.awards.models_matviews import SummaryView
from usaspending_api.awards.models_matviews import UniversalAwardView
from usaspending_api.awards.models_matviews import UniversalTransactionView
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.v2.filters.matview_transaction import transaction_filter
from usaspending_api.awards.v2.filters.matview_award import award_filter
import logging

logger = logging.getLogger(__name__)

MATVIEW_SELECTOR = {
    'SummaryView': {
        'allowed_filters': ['time_period', 'award_type_codes', 'agencies'],
        'prevent_values': {'agencies': {'type': 'list', 'key': 'tier', 'value': 'subtier'}},
        'model': SummaryView,
        'base_model': 'transaction'
    },
    'SummaryAwardView': {
        'allowed_filters': ['time_period', 'award_type_codes', 'agencies'],
        'prevent_values': {'agencies': {'type': 'list', 'key': 'tier', 'value': 'subtier'}},
        'model': SummaryAwardView,
        'base_model': 'award'
    },
    'SumaryPscCodesView': {
        'allowed_filters': ['time_period', 'award_type_codes'],
        'prevent_values': {},
        'model': SumaryPscCodesView,
        'base_model': 'transaction'
    },
    'SumaryCfdaNumbersView': {
        'allowed_filters': ['time_period', 'award_type_codes'],
        'prevent_values': {},
        'model': SumaryCfdaNumbersView,
        'base_model': 'transaction'
    },
    'SumaryNaicsCodesView': {
        'allowed_filters': ['time_period', 'award_type_codes'],
        'prevent_values': {},
        'model': SumaryNaicsCodesView,
        'base_model': 'transaction'
    },
    'SummaryTransactionView': {
        'allowed_filters': [
            'time_period',
            'award_type_codes',
            'agencies',
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
        'prevent_values': {'agencies': {'type': 'list', 'key': 'tier', 'value': 'subtier'}},
        'model': SummaryTransactionView,
        'base_model': 'transaction'
    },
    'SummaryTransactionMonthView': {
        'allowed_filters': [
            'time_period',
            'award_type_codes',
            'agencies',
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
        'prevent_values': {'agencies': {'type': 'list', 'key': 'tier', 'value': 'subtier'}},
        'model': SummaryTransactionMonthView,
        'base_model': 'transaction'
    },
    'UniversalTransactionView': {
        'allowed_filters': [
            'keyword',
            'time_period',
            'award_type_codes',
            'agencies',
            'legal_entities',
            'recipient_search_text',
            'recipient_scope',
            'recipient_locations',
            'recipient_type_names',
            'place_of_performance_scope',
            'place_of_performance_locations',
            'award_amounts',
            'award_ids',
            'program_numbers',
            'naics_codes',
            'psc_codes',
            'contract_pricing_type_codes',
            'set_aside_type_codes',
            'extent_competed_type_codes'],
        'prevent_values': {},
        'model': UniversalTransactionView,
        'base_model': 'transaction'
    },
    'UniversalAwardView': {
        'allowed_filters': [
            'keyword',
            'time_period',
            'award_type_codes',
            'agencies',
            'legal_entities',
            'recipient_search_text',
            'recipient_scope',
            'recipient_locations',
            'recipient_type_names',
            'place_of_performance_scope',
            'place_of_performance_locations',
            'award_amounts',
            'award_ids',
            'program_numbers',
            'naics_codes',
            'psc_codes',
            'contract_pricing_type_codes',
            'set_aside_type_codes',
            'extent_competed_type_codes'],
        'prevent_values': {},
        'model': UniversalAwardView,
        'base_model': 'award'
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
        print('#1 killed for view {} with filters {}'.format(view_name, filters))
        return False

    # Make sure *only* acceptable keys are in the filters for that view_name
    if not set(key_list).issuperset(set(filters.keys())):
        print('#2 killed for view {} with filters {}'.format(view_name, filters))
        return False

    for key, rules in MATVIEW_SELECTOR[view_name]['prevent_values'].items():
        '''
            slightly counter-intuitive. The loop is necessary to ensure that
            allowed filters don't have sub-(tier|scope|child) filters which are
            not compatible with the materialized view.
        '''
        if rules['type'] == 'list':
            try:
                for field in filters[key]:
                    if field[rules['key']] == rules['value']:
                        print('#3 killed for view {} with filters {}'.format(view_name, filters))
                        return False
            except KeyError:
                # Since a postive equality test produces a False, a key error is acceptable
                pass
        elif rules['type'] == 'dict':
            raise NotImplementedError
    return True


def spending_over_time(filters):
    view_chain = ['SummaryView', 'SummaryTransactionMonthView', 'UniversalTransactionView']
    for view in view_chain:
        if can_use_view(filters, view):
                queryset = get_view_queryset(filters, view)
                break
    else:
        raise InvalidParameterException

    return queryset


def spending_by_geography(filters):
    view_chain = ['SummaryTransactionMonthView', 'UniversalTransactionView']
    model = None
    for view in view_chain:
        if can_use_view(filters, view):
                queryset = get_view_queryset(filters, view)
                model = view
                break
    else:
        raise InvalidParameterException

    return queryset, model


def spending_by_award_count(filters):
    view_chain = ['SummaryAwardView', 'UniversalAwardView']
    model = None
    for view in view_chain:
        if can_use_view(filters, view):
                queryset = get_view_queryset(filters, view)
                model = view
                break
    else:
        raise InvalidParameterException

    return queryset, model
