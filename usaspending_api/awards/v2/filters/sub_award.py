import itertools
import logging

from django.db.models import Q

from usaspending_api.awards.models import Subaward, LegalEntity
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.common.exceptions import InvalidParameterException
# Commenting out currently knowing NAICS will be included later
# from usaspending_api.references.models import NAICS
from usaspending_api.references.models import PSC
from usaspending_api.search.v2 import elasticsearch_helper

logger = logging.getLogger(__name__)


# TODO: Performance when multiple false values are initially provided
def subaward_filter(filters):

    queryset = Subaward.objects.filter(award_id__isnull=False, award__latest_transaction_id__isnull=False)
    for key, value in filters.items():

        if value is None:
            raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')

        key_list = [
            'keyword',
            'elasticsearch_keyword',
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

            # Commenting out until NAICS is associated with subawards
            # naics_match = False
            # if keyword.isnumeric():
            #     naics_list = NAICS.objects.all().filter(code__icontains=keyword).values('code')
            # else:
            #     naics_list = NAICS.objects.all().filter(
            #         description__icontains=keyword).values('code')
            # if naics_list.exists():
            #     naics_match = True
            #     naics_qs = queryset.filter(award__latest_transaction__contract_data__naics__in=naics_list)

            psc_match = False
            if len(keyword) == 4 and PSC.objects.all().filter(code=keyword).exists():
                psc_list = PSC.objects.all().filter(code=keyword).values('code')
            else:
                psc_list = PSC.objects.all().filter(description__icontains=keyword).values('code')
            if psc_list.exists():
                psc_match = True
                psc_qs = queryset.filter(
                    award__latest_transaction__contract_data__product_or_service_code__in=psc_list)

            duns_match = False
            non_parent_duns_list = LegalEntity.objects.all().values('legal_entity_id').filter(
                recipient_unique_id=keyword)
            parent_duns_list = LegalEntity.objects.all().values('legal_entity_id').filter(
                parent_recipient_unique_id=keyword)
            duns_list = non_parent_duns_list | parent_duns_list
            if duns_list.exists():
                duns_match = True
                duns_qs = queryset.filter(recipient__in=duns_list)

            piid_qs = queryset.filter(award__piid=keyword)
            fain_qs = queryset.filter(award__fain=keyword)
            subaward_num_qs = queryset.filter(subaward_number=keyword)

            # Always filter on fain/piid because fast:
            queryset = piid_qs
            queryset |= fain_qs
            queryset |= subaward_num_qs
            # if description_match:
            #     queryset |= description_qs
            if recipient_match:
                queryset |= recipient_qs
            # if naics_match:
            #     queryset |= naics_qs
            if psc_match:
                queryset |= psc_qs
            if duns_match:
                queryset |= duns_qs

        elif key == "elasticsearch_keyword":
            keyword = value
            transaction_ids = elasticsearch_helper.get_download_ids(keyword=keyword, field='transaction_id')
            # flatten IDs
            transaction_ids = list(itertools.chain.from_iterable(transaction_ids))
            logger.info('Found {} transactions based on keyword: {}'.format(len(transaction_ids), keyword))
            transaction_ids = [str(transaction_id) for transaction_id in transaction_ids]
            queryset = queryset.filter(award__latest_transaction__id__isnull=False)
            queryset &= queryset.extra(
                where=['"transaction_normalized"."id" = ANY(\'{{{}}}\'::int[])'.format(','.join(transaction_ids))])

        elif key == "time_period":
            or_queryset = None
            queryset_init = False
            for v in value:
                date_type = v.get("date_type", "action_date")
                if date_type not in ["action_date", "last_modified_date"]:
                    raise InvalidParameterException('Invalid date_type: {}'.format(date_type))
                date_field = (date_type if date_type == 'action_date'
                              else 'award__latest_transaction__{}'.format(date_type))
                kwargs = {}
                if v.get("start_date") is not None:
                    kwargs["{}__gte".format(date_field)] = v.get("start_date")
                if v.get("end_date") is not None:
                    kwargs["{}__lte".format(date_field)] = v.get("end_date")
                # (may have to cast to date) (oct 1 to sept 30)
                if queryset_init:
                    or_queryset |= Subaward.objects.filter(**kwargs)
                else:
                    queryset_init = True
                    or_queryset = Subaward.objects.filter(**kwargs)
            if queryset_init:
                queryset &= or_queryset

        elif key == "award_type_codes":
            or_queryset = []

            idv_flag = all(i in value for i in ['A', 'B', 'C', 'D'])

            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                filter_obj = Q(award__type__in=or_queryset)
                if idv_flag:
                    filter_obj |= Q(award__latest_transaction__contract_data__pulled_from='IDV')
                queryset &= Subaward.objects.filter(filter_obj)

        elif key == "agencies":
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
                        if 'toptier_name' in v:
                            funding_subtier |= (Q(funding_agency__subtier_agency__name=name) &
                                                Q(funding_agency__toptier_agency__name=v["toptier_name"]))
                        else:
                            funding_subtier |= Q(funding_agency__subtier_agency__name=name)
                    else:
                        raise InvalidParameterException('Invalid filter: agencies ' + tier + ' tier is invalid.')
                elif type == "awarding":
                    if tier == "toptier":
                        awarding_toptier |= Q(awarding_agency__toptier_agency__name=name)
                    elif tier == "subtier":
                        if 'toptier_name' in v:
                            awarding_subtier |= (Q(awarding_agency__subtier_agency__name=name) &
                                                 Q(awarding_agency__toptier_agency__name=v['toptier_name']))
                        else:
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
                queryset &= Subaward.objects.filter(recipient__legal_entity_id__in=or_queryset)

        elif key == "recipient_search_text":
            if len(value) != 1:
                raise InvalidParameterException('Invalid filter: recipient_search_text must have exactly one value.')
            recipient_string = str(value[0])

            filter_obj = Q(recipient__recipient_name__icontains=recipient_string)

            if len(recipient_string) == 9:
                filter_obj |= Q(recipient__recipient_unique_id__iexact=recipient_string)

            queryset &= Subaward.objects.filter(filter_obj)

        elif key == "recipient_scope":
            if value == "domestic":
                queryset = queryset.filter(recipient__location__country_name="UNITED STATES")
            elif value == "foreign":
                queryset = queryset.exclude(recipient__location__country_name="UNITED STATES")
            else:
                raise InvalidParameterException('Invalid filter: recipient_scope type is invalid.')

        elif key == "recipient_locations":
            or_queryset = geocode_filter_locations('recipient__location', value, 'Subaward')
            queryset &= or_queryset

        elif key == "recipient_type_names":
            if len(value) != 0:
                queryset &= Subaward.objects.filter(
                    recipient__business_categories__overlap=value
                )

        elif key == "place_of_performance_scope":
            if value == "domestic":
                queryset = queryset.filter(place_of_performance__country_name="UNITED STATES")
            elif value == "foreign":
                queryset = queryset.exclude(place_of_performance__country_name="UNITED STATES")
            else:
                raise InvalidParameterException('Invalid filter: place_of_performance_scope is invalid.')

        elif key == "place_of_performance_locations":
            or_queryset = geocode_filter_locations('place_of_performance', value, 'Subaward')

            queryset &= or_queryset

        elif key == "award_amounts":
            or_queryset = None
            queryset_init = False
            for v in value:
                if v.get("lower_bound") is not None and v.get("upper_bound") is not None:
                    if queryset_init:
                        or_queryset |= Subaward.objects.filter(amount__gte=v["lower_bound"],
                                                               amount__lte=v["upper_bound"])
                    else:
                        queryset_init = True
                        or_queryset = Subaward.objects.filter(amount__gte=v["lower_bound"],
                                                              amount__lte=v["upper_bound"])
                elif v.get("lower_bound") is not None:
                    if queryset_init:
                        or_queryset |= Subaward.objects.filter(amount__gte=v["lower_bound"])
                    else:
                        queryset_init = True
                        or_queryset = Subaward.objects.filter(amount__gte=v["lower_bound"])
                elif v.get("upper_bound") is not None:
                    if queryset_init:
                        or_queryset |= Subaward.objects.filter(amount__lte=v["upper_bound"])
                    else:
                        queryset_init = True
                        or_queryset = Subaward.objects.filter(amount__lte=v["upper_bound"])
                else:
                    raise InvalidParameterException('Invalid filter: award amount has incorrect object.')
            if queryset_init:
                queryset &= or_queryset

        elif key == "award_ids":
            if len(value) != 0:
                filter_obj = Q()
                for val in value:
                    filter_obj |= (Q(award__piid__icontains=val) | Q(award__fain__icontains=val) |
                                   Q(award__uri__icontains=val) | Q(subaward_number__icontains=val))

                queryset &= Subaward.objects.filter(filter_obj)

        elif key == "program_numbers":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Subaward.objects.filter(
                    cfda__program_number__in=or_queryset)

        # Commenting this out as NAICS isn't currently mapped to subawards
        # elif key == "naics_codes":
        #     or_queryset = []
        #     for v in value:
        #         or_queryset.append(v)
        #     if len(or_queryset) != 0:
        #         queryset &= Subaward.objects.filter(
        #             naics__in=or_queryset)

        elif key == "psc_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Subaward.objects.filter(
                    award__latest_transaction__contract_data__product_or_service_code__in=or_queryset)

        elif key == "contract_pricing_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Subaward.objects.filter(
                    award__latest_transaction__contract_data__type_of_contract_pricing__in=or_queryset)

        elif key == "set_aside_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Subaward.objects.filter(
                    award__latest_transaction__contract_data__type_set_aside__in=or_queryset)

        elif key == "extent_competed_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= Subaward.objects.filter(
                    award__latest_transaction__contract_data__extent_competed__in=or_queryset)

    return queryset
