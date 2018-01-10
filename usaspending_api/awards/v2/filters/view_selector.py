from usaspending_api.awards.models_matviews import SummaryCfdaNumbersView
from usaspending_api.awards.models_matviews import SummaryNaicsCodesView
from usaspending_api.awards.models_matviews import SummaryPscCodesView
from usaspending_api.awards.models_matviews import SummaryAwardView
from usaspending_api.awards.models_matviews import SummaryTransactionMonthView
from usaspending_api.awards.models_matviews import SummaryTransactionView
from usaspending_api.awards.models_matviews import SummaryView
from usaspending_api.awards.models_matviews import UniversalAwardView
from usaspending_api.awards.models_matviews import UniversalTransactionView
from usaspending_api.awards.v2.filters.filter_helpers import can_use_month_aggregation, can_use_total_obligation_enum
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.common.exceptions import InvalidParameterException
import logging

logger = logging.getLogger(__name__)

MATVIEW_SELECTOR = {
    'SummaryView': {
        'allowed_filters': ['time_period', 'award_type_codes', 'agencies'],
        'prevent_values': {},  # Example: 'agencies': {'type': 'list', 'key': 'tier', 'value': 'subtier'}
        'examine_values': {},
        'model': SummaryView,
    },
    'SummaryAwardView': {
        'allowed_filters': ['time_period', 'award_type_codes', 'agencies'],
        'prevent_values': {},
        'examine_values': {},
        'model': SummaryAwardView,
    },
    'SummaryPscCodesView': {
        'allowed_filters': ['time_period', 'award_type_codes'],
        'prevent_values': {},
        'examine_values': {},
        'model': SummaryPscCodesView,
    },
    'SummaryCfdaNumbersView': {
        'allowed_filters': ['time_period', 'award_type_codes'],
        'prevent_values': {},
        'examine_values': {},
        'model': SummaryCfdaNumbersView,
    },
    'SummaryNaicsCodesView': {
        'allowed_filters': ['time_period', 'award_type_codes'],
        'prevent_values': {},
        'examine_values': {},
        'model': SummaryNaicsCodesView,
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
        'prevent_values': {},
        'examine_values': {},
        'model': SummaryTransactionView,
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
            'award_amounts',
            'naics_codes',
            'psc_codes',
            'contract_pricing_type_codes',
            'set_aside_type_codes',
            'extent_competed_type_codes'],
        'prevent_values': {},
        'examine_values': {
            'time_period': can_use_month_aggregation,
            'award_amounts': can_use_total_obligation_enum},
        'model': SummaryTransactionMonthView,
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
        'examine_values': {},
        'model': UniversalTransactionView,
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
        'examine_values': {},
        'model': UniversalAwardView,
    }
}


def get_view_queryset(filters, view_name):
    try:
        view_model = MATVIEW_SELECTOR[view_name]['model']
    except Exception:
        raise InvalidParameterException('Invalid view: ' + view_name + ' does not exist.')

    return matview_search_filter(filters, view_model)


def can_use_view(filters, view_name):
    try:
        key_list = MATVIEW_SELECTOR[view_name]['allowed_filters']
    except KeyError:
        return False

    # Make sure *only* acceptable keys are in the filters for that view_name
    if not set(key_list).issuperset(set(filters.keys())):
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
                        return False
            except KeyError:
                # Since a postive equality test produces a False, a key error is acceptable
                pass
        elif rules['type'] == 'dict':
            raise NotImplementedError

    for key, func in MATVIEW_SELECTOR[view_name]['examine_values'].items():
        try:
            if not func(filters[key]):
                return False
        except KeyError:
            pass
    return True


def spending_over_time(filters):
    view_chain = ['SummaryView', 'SummaryTransactionMonthView', 'SummaryTransactionView', 'UniversalTransactionView']
    for view in view_chain:
        if can_use_view(filters, view):
                queryset = get_view_queryset(filters, view)
                break
    else:
        raise InvalidParameterException

    return queryset


def spending_by_geography(filters):
    view_chain = ['SummaryTransactionMonthView', 'SummaryTransactionView', 'UniversalTransactionView']
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


def download_transaction_count(filters):
    view_chain = ['SummaryView', 'SummaryTransactionMonthView', 'SummaryTransactionView', 'UniversalTransactionView']
    model = None
    for view in view_chain:
        if can_use_view(filters, view):
                queryset = get_view_queryset(filters, view)
                model = view
                break
    else:
        raise InvalidParameterException

    return queryset, model
