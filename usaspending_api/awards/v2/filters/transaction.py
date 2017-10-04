from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.common.exceptions import InvalidParameterException

import logging
logger = logging.getLogger(__name__)


# TODO: Performance when multiple false values are initially provided
def transaction_filter(filters):

    queryset = TransactionNormalized.objects.all()
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
                    'award_ids',
                    'program_numbers',
                    'naics_codes',
                    'psc_codes',
                    'contract_pricing_type_codes',
                    'set_aside_type_codes',
                    'extent_competed_type_codes']

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
                    kwargs["action_date__gte"] = v.get("start_date")
                if v.get("end_date") is not None:
                    kwargs["action_date__lte"] = v.get("end_date")
                # (may have to cast to date) (oct 1 to sept 30)
                if or_queryset:
                    or_queryset |= TransactionNormalized.objects.filter(**kwargs)
                else:
                    or_queryset = TransactionNormalized.objects.filter(**kwargs)
            if or_queryset is not None:
                queryset &= or_queryset

        # award_type_codes
        elif key == "award_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= TransactionNormalized.objects.filter(award__type__in=or_queryset)

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
                queryset &= TransactionNormalized.objects.filter(funding_agency__toptier_agency__name__in=funding_toptier)
            if len(funding_subtier) != 0:
                queryset &= TransactionNormalized.objects.filter(funding_agency__subtier_agency__name__in=funding_subtier)
            if len(awarding_toptier) != 0:
                queryset &= TransactionNormalized.objects.filter(awarding_agency__toptier_agency__name__in=awarding_toptier)
            if len(awarding_subtier) != 0:
                queryset &= TransactionNormalized.objects.filter(awarding_agency__subtier_agency__name__in=awarding_subtier)

        # legal_entities
        elif key == "legal_entities":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= TransactionNormalized.objects.filter(recipient__legal_entity_id__in=or_queryset)

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
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= TransactionNormalized.objects.filter(recipient__location__location_id__in=or_queryset)

        # recipient_type_names
        elif key == "recipient_type_names":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= TransactionNormalized.objects.filter(recipient__business_types_description__in=or_queryset)

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
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= TransactionNormalized.objects.filter(place_of_performance__location_id__in=or_queryset)

        # award_amounts
        elif key == "award_amounts":
            or_queryset = None
            for v in value:
                if v.get("lower_bound") is not None and v.get("upper_bound") is not None:
                    if or_queryset:
                        or_queryset |= TransactionNormalized.objects.filter(award__total_obligation__gt=v["lower_bound"],
                                                                            award__total_obligation__lt=v["upper_bound"])
                    else:
                        or_queryset = TransactionNormalized.objects.filter(award__total_obligation__gt=v["lower_bound"],
                                                                           award__total_obligation__lt=v["upper_bound"])
                elif v.get("lower_bound") is not None:
                    if or_queryset:
                        or_queryset |= TransactionNormalized.objects.filter(award__total_obligation__gt=v["lower_bound"])
                    else:
                        or_queryset = TransactionNormalized.objects.filter(award__total_obligation__gt=v["lower_bound"])
                elif v.get("upper_bound") is not None:
                    if or_queryset:
                        or_queryset |= TransactionNormalized.objects.filter(award__total_obligation__lt=v["upper_bound"])
                    else:
                        or_queryset = TransactionNormalized.objects.filter(award__total_obligation__lt=v["upper_bound"])
                else:
                    raise InvalidParameterException('Invalid filter: award amount has incorrect object.')
            if or_queryset is not None:
                queryset &= or_queryset

        # award_ids
        elif key == "award_ids":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= TransactionNormalized.objects.filter(award__id__in=or_queryset)

        # program_numbers
        elif key == "program_numbers":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= TransactionNormalized.objects.filter(
                        assistance_data__cfda_number__in=or_queryset)

        # naics_codes
        elif key == "naics_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= TransactionNormalized.objects.filter(
                        contract_data__naics__in=or_queryset)

        # psc_codes
        elif key == "psc_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= TransactionNormalized.objects.filter(
                        contract_data__product_or_service_code__in=or_queryset)

        # contract_pricing_type_codes
        elif key == "contract_pricing_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= TransactionNormalized.objects.filter(
                        contract_data__type_of_contract_pricing__in=or_queryset)

        # set_aside_type_codes
        elif key == "set_aside_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= TransactionNormalized.objects.filter(
                        contract_data__type_set_aside__in=or_queryset)

        # extent_competed_type_codes
        elif key == "extent_competed_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= TransactionNormalized.objects.filter(
                        contract_data__extent_competed__in=or_queryset)

    return queryset
