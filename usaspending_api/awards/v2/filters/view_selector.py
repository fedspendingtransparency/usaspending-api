import logging

from usaspending_api.awards.models_matviews import AwardSummaryMatview
from usaspending_api.awards.models_matviews import SummaryCfdaNumbersView
from usaspending_api.awards.models_matviews import SummaryNaicsCodesView
from usaspending_api.awards.models_matviews import SummaryPscCodesView
from usaspending_api.awards.models_matviews import SummaryTransactionFedAcctView
from usaspending_api.awards.models_matviews import SummaryTransactionGeoView
from usaspending_api.awards.models_matviews import SummaryTransactionMonthView
from usaspending_api.awards.models_matviews import SummaryTransactionRecipientView
from usaspending_api.awards.models_matviews import SummaryTransactionView
from usaspending_api.awards.models_matviews import SummaryView
from usaspending_api.awards.models_matviews import UniversalTransactionView
from usaspending_api.awards.v2.filters.filter_helpers import can_use_month_aggregation
from usaspending_api.awards.v2.filters.filter_helpers import can_use_total_obligation_enum
from usaspending_api.awards.v2.filters.filter_helpers import only_action_date_type
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.common.exceptions import InvalidParameterException


logger = logging.getLogger(__name__)

MATVIEW_SELECTOR = {
    "AwardSummaryMatview": {
        "allowed_filters": ["time_period", "award_type_codes", "agencies"],
        "prevent_values": {},
        "examine_values": {"time_period": [only_action_date_type]},
        "model": AwardSummaryMatview,
    },
    "SummaryView": {
        "allowed_filters": ["time_period", "award_type_codes", "agencies"],
        "prevent_values": {},  # Example: 'agencies': {'type': 'list', 'key': 'tier', 'value': 'subtier'}
        "examine_values": {"time_period": [only_action_date_type]},
        "model": SummaryView,
    },
    "SummaryPscCodesView": {
        "allowed_filters": ["time_period", "award_type_codes"],
        "prevent_values": {},
        "examine_values": {"time_period": [only_action_date_type]},
        "model": SummaryPscCodesView,
    },
    "SummaryCfdaNumbersView": {
        "allowed_filters": ["time_period", "award_type_codes"],
        "prevent_values": {},
        "examine_values": {"time_period": [only_action_date_type]},
        "model": SummaryCfdaNumbersView,
    },
    "SummaryNaicsCodesView": {
        "allowed_filters": ["time_period", "award_type_codes"],
        "prevent_values": {},
        "examine_values": {"time_period": [only_action_date_type]},
        "model": SummaryNaicsCodesView,
    },
    "SummaryTransactionGeoView": {
        "allowed_filters": [
            "time_period",
            "award_type_codes",
            "agencies",
            "recipient_scope",
            "recipient_locations",
            "place_of_performance_scope",
            "place_of_performance_locations",
        ],
        "prevent_values": {},
        "examine_values": {"time_period": [can_use_month_aggregation, only_action_date_type]},
        "model": SummaryTransactionGeoView,
    },
    "SummaryTransactionView": {
        "allowed_filters": [
            "time_period",
            "award_type_codes",
            "agencies",
            "recipient_id",
            "recipient_scope",
            "recipient_locations",
            "recipient_type_names",
            "place_of_performance_scope",
            "place_of_performance_locations",
            "naics_codes",
            "psc_codes",
            "contract_pricing_type_codes",
            "set_aside_type_codes",
            "extent_competed_type_codes",
        ],
        "prevent_values": {},
        "examine_values": {"time_period": [only_action_date_type]},
        "model": SummaryTransactionView,
    },
    "SummaryTransactionMonthView": {
        "allowed_filters": [
            "time_period",
            "award_type_codes",
            "agencies",
            "recipient_scope",
            "recipient_locations",
            "recipient_type_names",
            "recipient_id",
            "place_of_performance_scope",
            "place_of_performance_locations",
            "award_amounts",
            "naics_codes",
            "psc_codes",
            "contract_pricing_type_codes",
            "set_aside_type_codes",
            "extent_competed_type_codes",
        ],
        "prevent_values": {},
        "examine_values": {
            "time_period": [can_use_month_aggregation, only_action_date_type],
            "award_amounts": [can_use_total_obligation_enum],
        },
        "model": SummaryTransactionMonthView,
    },
    "SummaryTransactionRecipientView": {
        "allowed_filters": ["time_period", "award_type_codes", "recipient_id"],
        "prevent_values": {},
        "examine_values": {"time_period": [only_action_date_type]},
        "model": SummaryTransactionRecipientView,
    },
    "SummaryTransactionFedAcctView": {
        "allowed_filters": ["time_period", "award_type_codes", "recipient_id"],
        "prevent_values": {},
        "examine_values": {"time_period": [only_action_date_type]},
        "model": SummaryTransactionFedAcctView,
    },
    "UniversalTransactionView": {
        "allowed_filters": [
            "keywords",
            "time_period",
            "award_type_codes",
            "agencies",
            "legal_entities",
            "recipient_id",
            "recipient_search_text",
            "recipient_scope",
            "recipient_locations",
            "recipient_type_names",
            "place_of_performance_scope",
            "place_of_performance_locations",
            "object_class",
            "program_activity",
            "award_amounts",
            "award_ids",
            "program_numbers",
            "naics_codes",
            "psc_codes",
            "contract_pricing_type_codes",
            "set_aside_type_codes",
            "extent_competed_type_codes",
            "tas_codes",
        ],
        "prevent_values": {},
        "examine_values": {},
        "model": UniversalTransactionView,
    },
}


def get_view_queryset(filters, view_name):
    try:
        view_model = MATVIEW_SELECTOR[view_name]["model"]
    except Exception:
        raise InvalidParameterException("Invalid view: " + view_name + " does not exist.")
    return matview_search_filter(filters, view_model)


def can_use_view(filters, view_name):
    try:
        key_list = MATVIEW_SELECTOR[view_name]["allowed_filters"]
    except KeyError:
        return False

    # Make sure *only* acceptable keys are in the filters for that view_name
    if not set(key_list).issuperset(set(filters.keys())):
        return False

    for key, rules in MATVIEW_SELECTOR[view_name]["prevent_values"].items():
        """
            slightly counter-intuitive. The loop is necessary to ensure that
            allowed filters don't have sub-(tier|scope|child) filters which are
            not compatible with the materialized view.
        """
        if rules["type"] == "list":
            if rules.get("is_present") and key in filters:
                for field in filters[key]:
                    if field.get(rules.get("key")):
                        return False
            try:
                for field in filters[key]:
                    if field[rules["key"]] == rules["value"]:
                        return False
            except KeyError:
                # Since a postive equality test produces a False, a key error is acceptable
                pass
        elif rules["type"] == "dict":
            raise NotImplementedError

    for key, func_list in MATVIEW_SELECTOR[view_name]["examine_values"].items():
        for func in func_list:
            try:
                if not func(filters[key]):
                    return False
            except KeyError:
                pass
    return True


def spending_over_time(filters):
    view_chain = [
        "SummaryView",
        "SummaryTransactionGeoView",
        "SummaryTransactionMonthView",
        "SummaryTransactionView",
        "UniversalTransactionView",
    ]
    for view in view_chain:
        if can_use_view(filters, view):
            queryset = get_view_queryset(filters, view)
            break
    else:
        raise InvalidParameterException

    return queryset


def spending_by_geography(filters):
    view_chain = [
        "SummaryTransactionGeoView",
        "SummaryTransactionMonthView",
        "SummaryTransactionView",
        "UniversalTransactionView",
    ]
    for view in view_chain:
        if can_use_view(filters, view):
            queryset = get_view_queryset(filters, view)
            model = view
            break
    else:
        raise InvalidParameterException

    return queryset, model


def spending_by_award_count(filters):
    if can_use_view(filters, "AwardSummaryMatview"):
        queryset = get_view_queryset(filters, "AwardSummaryMatview")
        return queryset, "AwardSummaryMatview"
    else:
        return None, None


def download_transaction_count(filters):
    view_chain = [
        "SummaryView",
        "SummaryTransactionGeoView",
        "SummaryTransactionMonthView",
        "SummaryTransactionView",
        "UniversalTransactionView",
    ]
    for view in view_chain:
        if can_use_view(filters, view):
            queryset = get_view_queryset(filters, view)
            model = view
            break
    else:
        raise InvalidParameterException

    return queryset, model


def transaction_spending_summary(filters):
    view_chain = [
        "SummaryView",
        "SummaryTransactionGeoView",
        "SummaryTransactionMonthView",
        "SummaryTransactionView",
        "UniversalTransactionView",
    ]
    for view in view_chain:
        if can_use_view(filters, view):
            queryset = get_view_queryset(filters, view)
            model = view
            break
    else:
        raise InvalidParameterException

    return queryset, model


def recipient_totals(filters):
    view_chain = ["SummaryTransactionMonthView", "SummaryTransactionView", "UniversalTransactionView"]
    for view in view_chain:
        if can_use_view(filters, view):
            queryset = get_view_queryset(filters, view)
            model = view
            break
    else:
        raise InvalidParameterException

    return queryset, model


def spending_by_category(category, filters):
    # category is a string of <category>.
    view_chain = []
    if category in ["awarding_agency", "funding_agency", "awarding_subagency", "funding_subagency"]:
        view_chain = ["SummaryView"]
    elif category == "psc":
        view_chain = ["SummaryPscCodesView"]
    elif category == "naics":
        view_chain = ["SummaryNaicsCodesView"]
    elif category == "cfda":
        view_chain = ["SummaryCfdaNumbersView"]
    elif category in ["county", "district", "state_territory", "country"]:
        view_chain = ["SummaryTransactionGeoView"]
    elif category in ["recipient_duns"]:
        view_chain = ["SummaryTransactionRecipientView"]

    # All of these category/scope combinations can use the following:
    view_chain.extend(["SummaryTransactionMonthView", "SummaryTransactionView", "UniversalTransactionView"])

    if category in ["federal_account"]:
        view_chain = ["SummaryTransactionFedAcctView"]

    for view in view_chain:
        if can_use_view(filters, view):
            queryset = get_view_queryset(filters, view)
            break
    else:
        raise InvalidParameterException

    return queryset
