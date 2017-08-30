from usaspending_api.awards.models import Award
from usaspending_api.common.exceptions import InvalidParameterException

import logging

logger = logging.getLogger(__name__)


# TODO: Performance when multiple false values are initially provided
def award_filter(filters):

    queryset = Award.objects.all()
    for key, value in filters.items():
        # check for valid key
        if value is None:
            raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')

        key_list = ['keyword',
                    'time_period',
                    'award_type_codes',
                    'agencies',
                    'legal_entities',
                    'recipient_scope',
                    'recipient_locations',
                    'recipient_type_names',
                    'place_of_performance_scope',
                    'place_of_performance_locations',
                    'award_amounts',
                    'award_ids'
                    ]

        if key not in key_list:
            raise InvalidParameterException('Invalid filter: ' + key + ' does not exist.')

        # keyword
        if key == "keyword":
            queryset = queryset.filter(description=value)

        # time_period
        elif key == "time_period":
            or_queryset = None
            for v in value:
                kwargs = {}
                if v.get("start_date") is not None:
                    kwargs["period_of_performance_start_date__gte"] = v.get("start_date")
                if v.get("end_date") is not None:
                    kwargs["period_of_performance_start_date__lte"] = v.get("end_date")
                # (may have to cast to date) (oct 1 to sept 30)
                if or_queryset:
                    or_queryset |= or_queryset.filter(**kwargs)
                else:
                    or_queryset = Award.objects.filter(**kwargs)
            if or_queryset is not None:
                queryset &= or_queryset

        # award_type_codes
        elif key == "award_type_codes":
            or_queryset = None
            for v in value:
                if or_queryset:
                    or_queryset |= or_queryset.filter(type=v)
                else:
                    or_queryset = Award.objects.filter(type=v)
            if or_queryset is not None:
                queryset &= or_queryset

        # agencies
        elif key == "agencies":
            or_queryset = None
            for v in value:
                type = v["type"]
                tier = v["tier"]
                name = v["name"]
                if type == "funding":
                    if tier == "toptier":
                        if or_queryset:
                            or_queryset |= or_queryset.filter(funding_agency__toptier_agency__name=name)
                        else:
                            or_queryset = Award.objects.filter(funding_agency__toptier_agency__name=name)
                    elif tier == "subtier":
                        if or_queryset:
                            or_queryset |= or_queryset.filter(funding_agency__subtier_agency__name=name)
                        else:
                            or_queryset = Award.objects.filter(funding_agency__subtier_agency__name=name)
                    else:
                        raise InvalidParameterException('Invalid filter: agencies ' + tier + ' tier is invalid.')
                elif type == "awarding":
                    if tier == "toptier":
                        if or_queryset:
                            or_queryset |= or_queryset.filter(awarding_agency__toptier_agency__name=name)
                        else:
                            or_queryset = Award.objects.filter(awarding_agency__toptier_agency__name=name)
                    elif tier == "subtier":
                        if or_queryset:
                            or_queryset |= or_queryset.filter(awarding_agency__subtier_agency__name=name)
                        else:
                            or_queryset = Award.objects.filter(awarding_agency__subtier_agency__name=name)
                    else:
                        raise InvalidParameterException('Invalid filter: agencies ' + tier + ' tier is invalid.')
                else:
                    raise InvalidParameterException('Invalid filter: agencies ' + type + ' type is invalid.')
            if or_queryset is not None:
                queryset &= or_queryset

        # legal_entities
        elif key == "legal_entities":
            or_queryset = None
            for v in value:
                if or_queryset:
                    or_queryset |= or_queryset.filter(recipient__legal_entity_id=v)
                else:
                    or_queryset = Award.objects.filter(recipient__legal_entity_id=v)
            if or_queryset is not None:
                queryset &= or_queryset

        # recipient_location_scope (broken till data reload)
        elif key == "recipient_scope":
            if value == "domestic":
                queryset = queryset.filter(recipient__location__country_name="UNITED STATES")
            elif value == "foreign":
                queryset = queryset.exclude(recipient__location__country_name="UNITED STATES")
            else:
                raise InvalidParameterException('Invalid filter: recipient_scope type is invalid.')

        # recipient_location
        elif key == "recipient_locations":
            or_queryset = None
            for v in value:
                if or_queryset:
                    or_queryset |= or_queryset.filter(recipient__location__location_id=v)
                else:
                    or_queryset = Award.objects.filter(recipient__location__location_id=v)
            if or_queryset is not None:
                queryset &= or_queryset

        # recipient_type_names
        elif key == "recipient_type_names":
            or_queryset = None
            for v in value:
                if or_queryset:
                    or_queryset |= or_queryset.filter(recipient__business_types_description=v)
                else:
                    or_queryset = Award.objects.filter(recipient__business_types_description=v)
            if or_queryset is not None:
                queryset &= or_queryset

        # place_of_performance_scope (broken till data reload
        elif key == "place_of_performance_scope":
            if value == "domestic":
                queryset = queryset.filter(place_of_performance__country_name="UNITED STATES")
            elif value == "foreign":
                queryset = queryset.exclude(place_of_performance__country_name="UNITED STATES")
            else:
                raise InvalidParameterException('Invalid filter: place_of_performance_scope is invalid.')

        # place_of_performance
        elif key == "place_of_performance_locations":
            or_queryset = None
            for v in value:
                if or_queryset:
                    or_queryset |= or_queryset.filter(place_of_performance__location_id=v)
                else:
                    or_queryset = Award.objects.filter(place_of_performance__location_id=v)
            if or_queryset is not None:
                queryset &= or_queryset

        # award_amounts
        elif key == "award_amounts":
            or_queryset = None
            for v in value:
                if v.get("lower_bound") is not None and v.get("upper_bound") is not None:
                    if or_queryset:
                        or_queryset |= or_queryset.filter(total_obligation__gt=v["lower_bound"],
                                                          total_obligation__lt=v["upper_bound"])
                    else:
                        or_queryset = Award.objects.filter(total_obligation__gt=v["lower_bound"],
                                                                 total_obligation__lt=v["upper_bound"])
                elif v.get("lower_bound") is not None:
                    if or_queryset:
                        or_queryset |= or_queryset.filter(total_obligation__gt=v["lower_bound"])
                    else:
                        or_queryset = Award.objects.filter(total_obligation__gt=v["lower_bound"])
                elif v.get("upper_bound") is not None:
                    if or_queryset:
                        or_queryset |= or_queryset.filter(total_obligation__lt=v["upper_bound"])
                    else:
                        or_queryset = Award.objects.filter(total_obligation__lt=v["upper_bound"])
                else:
                    raise InvalidParameterException('Invalid filter: award amount has incorrect object.')
            if or_queryset is not None:
                queryset &= or_queryset

        # award_ids
        elif key == "award_ids":
            or_queryset = None
            for v in value:
                if or_queryset:
                    or_queryset |= or_queryset.filter(id=v)
                else:
                    or_queryset = Award.objects.filter(id=v)
            if or_queryset is not None:
                queryset &= or_queryset

    return queryset
