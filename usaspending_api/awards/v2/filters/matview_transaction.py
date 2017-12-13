import logging
from django.db.models import Q

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.references.models import PSC
from .filter_helpers import date_or_fy_queryset, total_obligation_queryset

logger = logging.getLogger(__name__)


# TODO: Performance when multiple false values are initially provided
def transaction_filter(filters, model):
    queryset = model.objects.all()
    for key, value in filters.items():
        if value is None:
            raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')

        key_list = [
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
            'extent_competed_type_codes'
        ]

        if key not in key_list:
            raise InvalidParameterException('Invalid filter: ' + key + ' does not exist.')

        if key == "keyword":
            keyword = value

            compound_or = Q(recipient_name__icontains=keyword) | \
                Q(piid=keyword) | \
                Q(fain=keyword) | \
                Q(recipient_unique_id=keyword) | \
                Q(parent_recipient_unique_id=keyword)

            if keyword.isnumeric():
                compound_or |= Q(naics_code__contains=keyword)
            else:
                compound_or |= Q(naics_description__icontains=keyword)

            if len(keyword) == 4 and PSC.objects.all().filter(code__iexact=keyword).exists():
                compound_or |= Q(product_or_service_code__iexact=keyword)
            else:
                compound_or |= Q(product_or_service_description__icontains=keyword)

            queryset = queryset.filter(compound_or)

        elif key == "time_period":
            success, or_queryset = date_or_fy_queryset(value, model, "fiscal_year",
                                                       "action_date")
            if success:
                queryset &= or_queryset

        elif key == "award_type_codes":
            if len(value) != 0:
                queryset &= model.objects.filter(type__in=value)

        elif key == "agencies":
            # TODO: Make function to match agencies in award filter throwing dupe error
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

            if funding_toptier:
                or_queryset = Q()
                for name in funding_toptier:
                    or_queryset |= Q(funding_toptier_agency_name__icontains=name)
                queryset = queryset.filter(or_queryset)

            if funding_subtier:
                or_queryset = Q()
                for name in funding_subtier:
                    or_queryset |= Q(funding_subtier_agency_name__icontains=name)
                queryset = queryset.filter(or_queryset)

            if awarding_toptier:
                or_queryset = Q()
                for name in awarding_toptier:
                    or_queryset |= Q(awarding_toptier_agency_name__icontains=name)
                queryset = queryset.filter(or_queryset)

            if awarding_subtier:
                or_queryset = Q()
                for name in awarding_subtier:
                    or_queryset |= Q(awarding_subtier_agency_name__icontains=name)
                queryset = queryset.filter(or_queryset)

        elif key == "legal_entities":
            in_query = [v for v in value]
            if len(in_query) != 0:
                queryset &= model.objects.filter(recipient_id__in=in_query)

        elif key == "recipient_search_text":
            if len(value) != 1:
                raise InvalidParameterException('Invalid filter: recipient_search_text must have exactly one value.')
            recipient_string = str(value[0])

            filter_obj = Q(recipient_name__icontains=recipient_string)

            if len(recipient_string) == 9:
                filter_obj |= Q(recipient_unique_id__iexact=recipient_string)

            queryset &= model.objects.filter(filter_obj)

        elif key == "recipient_scope":
            if value == "domestic":
                queryset = queryset.filter(
                    recipient_location_country_name="UNITED STATES"
                )
            elif value == "foreign":
                queryset = queryset.exclude(
                    recipient_location_country_name="UNITED STATES"
                )
            else:
                raise InvalidParameterException(
                    'Invalid filter: recipient_scope type is invalid.')

        elif key == "recipient_locations":
            or_queryset = geocode_filter_locations(
                'recipient_location', value, model, True
            )

            queryset &= or_queryset

        elif key == "recipient_type_names":
            if len(value) != 0:
                queryset &= model.objects.filter(business_categories__overlap=value)

        elif key == "place_of_performance_scope":
            if value == "domestic":
                queryset = queryset.filter(pop_country_name="UNITED STATES")
            elif value == "foreign":
                queryset = queryset.exclude(pop_country_name="UNITED STATES")
            else:
                raise InvalidParameterException('Invalid filter: place_of_performance_scope is invalid.')

        elif key == "place_of_performance_locations":
            queryset &= geocode_filter_locations(
                'pop', value, model, True
            )

        elif key == "award_amounts":
            success, or_queryset = total_obligation_queryset(value, model)
            if success:
                queryset &= or_queryset

        elif key == "award_ids":
            if len(value) != 0:
                filter_obj = Q()
                for val in value:
                    filter_obj |= Q(piid__icontains=val) | Q(fain__icontains=val) | Q(uri__icontains=val)
                queryset &= model.objects.filter(filter_obj)

        elif key == "program_numbers":
            in_query = [v for v in value]
            if len(in_query) != 0:
                queryset &= model.objects.filter(
                    cfda_number__in=in_query)

        elif key == "naics_codes":
            in_query = [v for v in value]
            if len(in_query) != 0:
                queryset &= model.objects.filter(
                    naics_code__in=in_query)

        elif key == "psc_codes":
            in_query = [v for v in value]
            if len(in_query) != 0:
                queryset &= model.objects.filter(
                    product_or_service_code__in=in_query)

        elif key == "contract_pricing_type_codes":
            in_query = [v for v in value]
            if len(in_query) != 0:
                queryset &= model.objects.filter(
                    type_of_contract_pricing__in=in_query)

        elif key == "set_aside_type_codes":
            or_queryset = Q()
            for v in value:
                or_queryset |= Q(type_set_aside__exact=v)
            queryset = queryset.filter(or_queryset)

        elif key == "extent_competed_type_codes":
            or_queryset = Q()
            for v in value:
                or_queryset |= Q(extent_competed__exact=v)
            queryset = queryset.filter(or_queryset)

    return queryset
