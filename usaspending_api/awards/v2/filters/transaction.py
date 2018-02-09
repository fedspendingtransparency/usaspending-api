from django.db.models import Q

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.awards.models import LegalEntity
from usaspending_api.references.models import NAICS, PSC
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations

import logging
logger = logging.getLogger(__name__)


# TODO: Performance when multiple false values are initially provided
def transaction_filter(filters):

    queryset = TransactionNormalized.objects.all()
    for key, value in filters.items():
        # check for valid key
        if value is None:
            raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')

        key_list = [
            # 'keyword',
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
            'extent_competed_type_codes'
        ]

        if key not in key_list:
            raise InvalidParameterException('Invalid filter: ' + key + ' does not exist.')

        # if key == "keyword":
        #     keyword = value  # alias
        #
        #     # description_match = False
        #     # description_qs = queryset.filter(description__icontains=keyword)
        #     # if description_qs.exists():
        #     #     description_match = True
        #
        #     recipient_match = False
        #     recipient_list = LegalEntity.objects.all().values('legal_entity_id').filter(
        #         recipient_name__icontains=keyword)
        #     if recipient_list.exists():
        #         recipient_match = True
        #         recipient_qs = queryset.filter(recipient__in=recipient_list)
        #
        #     naics_match = False
        #     if keyword.isnumeric():
        #         naics_list = NAICS.objects.all().filter(code__icontains=keyword).values('code')
        #     else:
        #         naics_list = NAICS.objects.all().filter(
        #             description__icontains=keyword).values('code')
        #     if naics_list.exists():
        #         naics_match = True
        #         naics_qs = queryset.filter(contract_data__naics__in=naics_list)
        #
        #     psc_match = False
        #     if len(keyword) == 4 and PSC.objects.all().filter(code=keyword).exists():
        #         psc_list = PSC.objects.all().filter(code=keyword).values('code')
        #     else:
        #         psc_list = PSC.objects.all().filter(description__icontains=keyword).values('code')
        #     if psc_list.exists():
        #         psc_match = True
        #         psc_qs = queryset.filter(contract_data__product_or_service_code__in=psc_list)
        #
        #     duns_match = False
        #     non_parent_duns_list = LegalEntity.objects.all().values('legal_entity_id').filter(
        #         recipient_unique_id=keyword)
        #     parent_duns_list = LegalEntity.objects.all().values('legal_entity_id').filter(
        #         parent_recipient_unique_id=keyword)
        #     duns_list = non_parent_duns_list | parent_duns_list
        #     if duns_list.exists():
        #         duns_match = True
        #         duns_qs = queryset.filter(recipient__in=duns_list)
        #
        #     piid_qs = queryset.filter(contract_data__piid=keyword)
        #     fain_qs = queryset.filter(assistance_data__fain=keyword)
        #
        #     # Always filter on fain/piid because fast:
        #     queryset = piid_qs
        #     queryset |= fain_qs
        #     # if description_match:
        #     #     queryset |= description_qs
        #     if recipient_match:
        #         queryset |= recipient_qs
        #     if naics_match:
        #         queryset |= naics_qs
        #     if psc_match:
        #         queryset |= psc_qs
        #     if duns_match:
        #         queryset |= duns_qs

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
                    or_queryset |= TransactionNormalized.objects.filter(**kwargs)
                else:
                    queryset_init = True
                    or_queryset = TransactionNormalized.objects.filter(**kwargs)
            if queryset_init:
                queryset &= or_queryset

        # award_type_codes
        elif key == "award_type_codes":
            or_queryset = []

            idv_flag = all(i in value for i in ['A', 'B', 'C', 'D'])

            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                filter_obj = Q(type__in=or_queryset)
                if idv_flag:
                    filter_obj |= Q(contract_data__pulled_from='IDV')
                queryset &= TransactionNormalized.objects.filter(filter_obj)

        # agencies
        elif key == "agencies":
            # TODO: Make function to match agencies in award filter throwing dupe error
            funding_toptier = Q()
            funding_subtier = Q()
            awarding_toptier = Q()
            awarding_subtier = Q()
            for v in value:
                type = v["type"]
                tier = v["tier"]
                name = v["name"]
                if type == "funding":
                    if tier == "toptier":
                        funding_toptier |= Q(funding_agency__toptier_agency__name=name)
                    elif tier == "subtier":
                        funding_subtier |= Q(funding_agency__subtier_agency__name=name)
                    else:
                        raise InvalidParameterException('Invalid filter: agencies ' + tier + ' tier is invalid.')
                elif type == "awarding":
                    if tier == "toptier":
                        awarding_toptier |= Q(awarding_agency__toptier_agency__name=name)
                    elif tier == "subtier":
                        awarding_subtier |= Q(awarding_agency__subtier_agency__name=name)
                    else:
                        raise InvalidParameterException('Invalid filter: agencies ' + tier + ' tier is invalid.')
                else:
                    raise InvalidParameterException('Invalid filter: agencies ' + type + ' type is invalid.')

            awarding_queryfilter = Q()
            funding_queryfilter = Q()

            # Since these are Q filters, no DB hits for boolean checks
            if funding_toptier:
                funding_queryfilter |= funding_toptier
            if funding_subtier:
                funding_queryfilter |= funding_subtier
            if awarding_toptier:
                awarding_queryfilter |= awarding_toptier
            if awarding_subtier:
                awarding_queryfilter |= awarding_subtier

            queryset = queryset.filter(funding_queryfilter & awarding_queryfilter)

        elif key == "legal_entities":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= TransactionNormalized.objects.filter(
                    recipient__legal_entity_id__in=or_queryset
                )

        elif key == "recipient_search_text":
            if len(value) != 1:
                raise InvalidParameterException(
                    'Invalid filter: recipient_search_text must have exactly one value.')
            recipient_string = str(value[0])

            filter_obj = Q(recipient__recipient_name__icontains=recipient_string)

            if len(recipient_string) == 9:
                filter_obj |= Q(recipient__recipient_unique_id__iexact=recipient_string)

            queryset &= TransactionNormalized.objects.filter(filter_obj)

        # recipient_location_scope (broken till data reload)
        elif key == "recipient_scope":
            if value == "domestic":
                queryset = queryset.filter(
                    recipient__location__country_name="UNITED STATES"
                )
            elif value == "foreign":
                queryset = queryset.exclude(
                    recipient__location__country_name="UNITED STATES"
                )
            else:
                raise InvalidParameterException(
                    'Invalid filter: recipient_scope type is invalid.')

        # recipient_location
        elif key == "recipient_locations":
            or_queryset = geocode_filter_locations(
                'recipient__location', value, 'TransactionNormalized'
            )

            queryset &= or_queryset

        # recipient_type_names
        elif key == "recipient_type_names":
            if len(value) != 0:
                queryset &= TransactionNormalized.objects.filter(
                    recipient__business_categories__overlap=value
                )

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
            or_queryset = geocode_filter_locations(
                'place_of_performance', value, 'TransactionNormalized'
            )

            queryset &= or_queryset

        # award_amounts
        elif key == "award_amounts":
            or_queryset = None
            queryset_init = False
            for v in value:
                if v.get("lower_bound") is not None and v.get("upper_bound") is not None:
                    if queryset_init:
                        or_queryset |= TransactionNormalized.objects.filter(
                            award__total_obligation__gt=v["lower_bound"],
                            award__total_obligation__lt=v["upper_bound"]
                        )
                    else:
                        queryset_init = True
                        or_queryset = TransactionNormalized.objects.filter(award__total_obligation__gt=v["lower_bound"],
                                                                           award__total_obligation__lt=v["upper_bound"])
                elif v.get("lower_bound") is not None:
                    if queryset_init:
                        or_queryset |= TransactionNormalized.objects.filter(
                            award__total_obligation__gt=v["lower_bound"]
                        )
                    else:
                        queryset_init = True
                        or_queryset = TransactionNormalized.objects.filter(award__total_obligation__gt=v["lower_bound"])
                elif v.get("upper_bound") is not None:
                    if queryset_init:
                        or_queryset |= TransactionNormalized.objects.filter(
                            award__total_obligation__lt=v["upper_bound"]
                        )
                    else:
                        queryset_init = True
                        or_queryset = TransactionNormalized.objects.filter(award__total_obligation__lt=v["upper_bound"])
                else:
                    raise InvalidParameterException('Invalid filter: award amount has incorrect object.')
            if queryset_init:
                queryset &= or_queryset

        # award_ids
        elif key == "award_ids":
            if len(value) != 0:
                filter_obj = Q()
                for val in value:
                    filter_obj |= Q(award__piid__icontains=val) | Q(award__fain__icontains=val) | \
                                  Q(award__uri__icontains=val)

                queryset &= TransactionNormalized.objects.filter(filter_obj)

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
