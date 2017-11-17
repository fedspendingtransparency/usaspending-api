from usaspending_api.awards.models_matviews import SummaryAwardView, SummaryView, SumaryPscCodesView, SumaryCfdaNumbersView
from usaspending_api.common.exceptions import InvalidParameterException

import logging

logger = logging.getLogger(__name__)

MATVIEW_SELECTOR = {
    'SummaryView': {
        'allowed_filters': ['time_period', 'award_type_codes', 'agencies'],
        'model': SummaryView.objects
    },
    'SummaryAwardView': {
        'allowed_filters': ['time_period', 'award_type_codes', 'agencies'],
        'model': SummaryAwardView.objects
    },
    'SumaryPscCodesView': {
        'allowed_filters': ['time_period', 'award_type_codes'],
        'model': SumaryPscCodesView.objects
    },
    'SumaryCfdaNumbersView': {
        'allowed_filters': ['time_period', 'award_type_codes'],
        'model': SumaryCfdaNumbersView.objects
    }
}


def view_filter(filters, view_name):

    try:
        view_objects = MATVIEW_SELECTOR[view_name]['model']
    except Exception as e:
        print(e)
        raise InvalidParameterException('Invalid view: ' + view_name + ' does not exist.')

    queryset = view_objects.all()

    for key, value in filters.items():
        # check for valid key
        if value is None:
            raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')

        # time_period
        if key == "time_period":
            or_queryset = None
            queryset_init = False
            for v in value:
                kwargs = {}
                if v.get("start_date") is not None:
                    kwargs["action_date__gte"] = v.get("start_date")
                if v.get("end_date") is not None:
                    kwargs["action_date__lte"] = v.get("end_date")
                # (may have to cast to date) (oct 1 to sept 30)
                if queryset_init:
                    or_queryset |= view_objects.filter(**kwargs)
                else:
                    queryset_init = True
                    or_queryset = view_objects.filter(**kwargs)
            if queryset_init:
                queryset &= or_queryset

        # award_type_codes
        elif key == "award_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= view_objects.filter(type__in=or_queryset)

        # agencies
        elif key == "agencies":
            or_queryset = None
            funding_toptier = []
            funding_subtier = []
            awarding_toptier = []
            awarding_subtier = []
            for v in value:
                type = v["type"]
                tier = v["tier"]
                name = v["name"]
                if type == "funding":
                    if tier == "toptier":
                        funding_toptier.append(name)
                    elif tier == "subtier":
                        funding_subtier.append(name)
                    else:
                        raise InvalidParameterException('Invalid filter: agencies ' + tier + ' tier is invalid.')
                elif type == "awarding":
                    if tier == "toptier":
                        awarding_toptier.append(name)
                    elif tier == "subtier":
                        awarding_subtier.append(name)
                    else:
                        raise InvalidParameterException('Invalid filter: agencies ' + tier + ' tier is invalid.')
                else:
                    raise InvalidParameterException('Invalid filter: agencies ' + type + ' type is invalid.')
            if len(funding_toptier) != 0:
                queryset &= view_objects.filter(funding_agency_name__in=funding_toptier)
            # if len(funding_subtier) != 0:
            #     queryset &= viewObjects.filter(funding_agency__subtier_agency__name__in=funding_subtier)
            if len(awarding_toptier) != 0:
                queryset &= view_objects.filter(awarding_agency_name__in=awarding_toptier)
            # if len(awarding_subtier) != 0:
            #     queryset &= viewObjects.filter(awarding_agency__subtier_agency__name__in=awarding_subtier)

    return queryset


def can_use_view(filters, view_name):
    try:
        key_list = MATVIEW_SELECTOR[view_name]['allowed_filters']
    except Exception as e:
        print(e)
        return False

    # Make sure only the keys in the key list are in the fileters
    if len(filters.keys() - key_list) > 0:
        return False

    agencies = filters.get('agencies')
    if agencies is not None:
        for v in agencies:
            if v["tier"] == "subtier":
                return False
    return True
