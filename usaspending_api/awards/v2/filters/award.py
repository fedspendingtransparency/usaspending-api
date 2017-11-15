from usaspending_api.awards.models import Award, LegalEntity
from usaspending_api.references.models import NAICS, PSC
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations

import logging

logger = logging.getLogger(__name__)


# TODO: Performance when multiple false values are initially provided
def award_filter(filters):

    queryset = Award.objects.filter(latest_transaction_id__isnull=False, category__isnull=False)
    for key, value in filters.items():

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
                    'extent_competed_type_codes'
                    ]

        if key not in key_list:
            raise InvalidParameterException('Invalid filter: ' + key + ' does not exist.')

        if key == "keyword":
            keyword = value  # alias

            # description_match = False
            # description_qs = queryset.filter(description__icontains=keyword)
            # if description_qs.exists():
            #     description_match = True

            recipient_match = False
            recipient_list = LegalEntity.objects.all().values('legal_entity_id').filter(
                recipient_name__icontains=keyword)
            if recipient_list.exists():
                recipient_match = True
                recipient_qs = queryset.filter(recipient__in=recipient_list)

            naics_match = False
            if keyword.isnumeric():
                naics_list = NAICS.objects.all().filter(code__icontains=keyword).values('code')
            else:
                naics_list = NAICS.objects.all().filter(
                    description__icontains=keyword).values('code')
            if naics_list.exists():
                naics_match = True
                naics_qs = queryset.filter(latest_transaction__contract_data__naics__in=naics_list)

            psc_match = False
            if len(keyword) == 4 and PSC.objects.all().filter(code=keyword).exists():
                psc_list = PSC.objects.all().filter(code=keyword).values('code')
            else:
                psc_list = PSC.objects.all().filter(description__icontains=keyword).values('code')
            if psc_list.exists():
                psc_match = True
                psc_qs = queryset.filter(
                        latest_transaction__contract_data__product_or_service_code__in=psc_list)

            duns_match = False
            non_parent_duns_list = LegalEntity.objects.all().values('legal_entity_id').filter(
                recipient_unique_id=keyword)
            parent_duns_list = LegalEntity.objects.all().values('legal_entity_id').filter(
                parent_recipient_unique_id=keyword)
            duns_list = non_parent_duns_list | parent_duns_list
            if duns_list.exists():
                duns_match = True
                duns_qs = queryset.filter(recipient__in=duns_list)

            piid_qs = queryset.filter(piid=keyword)
            fain_qs = queryset.filter(fain=keyword)

            # Always filter on fain/piid because fast:
            queryset = piid_qs
            queryset |= fain_qs
            # if description_match:
            #     queryset |= description_qs
            if recipient_match:
                queryset |= recipient_qs
            if naics_match:
                queryset |= naics_qs
            if psc_match:
                queryset |= psc_qs
            if duns_match:
                queryset |= duns_qs

        elif key == "time_period":
            or_queryset = None
            queryset_init = False
            for v in value:
                kwargs = {}
                if v.get("start_date") is not None:
                    kwargs["latest_transaction__action_date__gte"] = v.get("start_date")
                if v.get("end_date") is not None:
                    kwargs["latest_transaction__action_date__lte"] = v.get("end_date")
                # (may have to cast to date) (oct 1 to sept 30)
                if queryset_init:
                    or_queryset |= Award.objects.filter(**kwargs)
                else:
                    queryset_init = True
                    or_queryset = Award.objects.filter(**kwargs)
            if queryset_init:
                queryset &= or_queryset

        elif key == "award_type_codes":
            or_queryset = []

            idv_flag = all(i in value for i in ['A', 'B', 'C', 'D'])

            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Award.objects.filter(type__in=or_queryset)
            if idv_flag:
                queryset |= Award.objects.filter(type__isnull=True, latest_transaction__contract_data__pulled_from='IDV')

        elif key == "agencies":
            # TODO: Make function to match agencies in award filter throwing dupe error
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
                queryset &= Award.objects.filter(funding_agency__toptier_agency__name__in=funding_toptier)
            if len(funding_subtier) != 0:
                queryset &= Award.objects.filter(funding_agency__subtier_agency__name__in=funding_subtier)
            if len(awarding_toptier) != 0:
                queryset &= Award.objects.filter(awarding_agency__toptier_agency__name__in=awarding_toptier)
            if len(awarding_subtier) != 0:
                queryset &= Award.objects.filter(awarding_agency__subtier_agency__name__in=awarding_subtier)

        elif key == "legal_entities":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Award.objects.filter(recipient__legal_entity_id__in=or_queryset)

        elif key == "recipient_scope":
            if value == "domestic":
                queryset = queryset.filter(recipient__location__country_name="UNITED STATES")
            elif value == "foreign":
                queryset = queryset.exclude(recipient__location__country_name="UNITED STATES")
            else:
                raise InvalidParameterException('Invalid filter: recipient_scope type is invalid.')

        elif key == "recipient_locations":
            or_queryset = geocode_filter_locations('recipient__location', value, 'Award')
            queryset &= or_queryset

        elif key == "recipient_type_names":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Award.objects.filter(recipient__business_types_description__in=or_queryset)

        elif key == "place_of_performance_scope":
            if value == "domestic":
                queryset = queryset.filter(place_of_performance__country_name="UNITED STATES")
            elif value == "foreign":
                queryset = queryset.exclude(place_of_performance__country_name="UNITED STATES")
            else:
                raise InvalidParameterException('Invalid filter: place_of_performance_scope is invalid.')

        elif key == "place_of_performance_locations":
            or_queryset = geocode_filter_locations('place_of_performance', value, 'Award')

            queryset &= or_queryset

        elif key == "award_amounts":
            or_queryset = None
            queryset_init = False
            for v in value:
                if v.get("lower_bound") is not None and v.get("upper_bound") is not None:
                    if queryset_init:
                        or_queryset |= Award.objects.filter(total_obligation__gt=v["lower_bound"],
                                                            total_obligation__lt=v["upper_bound"])
                    else:
                        queryset_init = True
                        or_queryset = Award.objects.filter(total_obligation__gt=v["lower_bound"],
                                                           total_obligation__lt=v["upper_bound"])
                elif v.get("lower_bound") is not None:
                    if queryset_init:
                        or_queryset |= Award.objects.filter(total_obligation__gt=v["lower_bound"])
                    else:
                        queryset_init = True
                        or_queryset = Award.objects.filter(total_obligation__gt=v["lower_bound"])
                elif v.get("upper_bound") is not None:
                    if queryset_init:
                        or_queryset |= Award.objects.filter(total_obligation__lt=v["upper_bound"])
                    else:
                        queryset_init = True
                        or_queryset = Award.objects.filter(total_obligation__lt=v["upper_bound"])
                else:
                    raise InvalidParameterException('Invalid filter: award amount has incorrect object.')
            if queryset_init:
                queryset &= or_queryset

        elif key == "award_ids":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Award.objects.filter(id__in=or_queryset)

        elif key == "program_numbers":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Award.objects.filter(
                        latest_transaction__assistance_data__cfda_number__in=or_queryset)

        elif key == "naics_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Award.objects.filter(
                        latest_transaction__contract_data__naics__in=or_queryset)

        elif key == "psc_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Award.objects.filter(
                        latest_transaction__contract_data__product_or_service_code__in=or_queryset)

        elif key == "contract_pricing_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Award.objects.filter(
                        latest_transaction__contract_data__type_of_contract_pricing__in=or_queryset)

        elif key == "set_aside_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Award.objects.filter(
                        latest_transaction__contract_data__type_set_aside__in=or_queryset)

        elif key == "extent_competed_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Award.objects.filter(
                        latest_transaction__contract_data__extent_competed__in=or_queryset)

    return queryset
