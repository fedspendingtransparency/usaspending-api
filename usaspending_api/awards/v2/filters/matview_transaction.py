import logging
from django.db.models import Q

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.references.models import PSC, NAICS
from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping
from .filter_helpers import date_or_fy_queryset, total_obligation_queryset

logger = logging.getLogger(__name__)


# TODO: Performance when multiple false values are initially provided
def transaction_filter(filters, model):
    queryset = model.objects.all()
    for key, value in filters.items():
        # check for valid key
        if value is None:
            raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')

        key_list = ['keyword',
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
                    'extent_competed_type_codes']

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
                naics_list = NAICS.objects.all().filter(
                    code__icontains=keyword).values('code')
            else:
                naics_list = NAICS.objects.all().filter(
                    description__icontains=keyword).values('code')

            if naics_list:
                compound_or |= Q(naics_code__in=naics_list)

            if len(keyword) == 4 and PSC.objects.all().filter(code=keyword).exists():
                psc_list = PSC.objects.all().filter(code=keyword).values('code')
            else:
                psc_list = PSC.objects.all().filter(description__icontains=keyword).values('code')
            if psc_list.exists():
                compound_or |= Q(psc_code__in=psc_list)

            queryset = queryset.filter(compound_or)

        # time_period
        elif key == "time_period":
            success, or_queryset = date_or_fy_queryset(value, model, "fiscal_year",
                                                       "action_date")
            if success:
                queryset &= or_queryset

        # award_type_codes
        elif key == "award_type_codes":
            idv_flag = all(i in value for i in contract_type_mapping.keys())

            if len(value) != 0:
                filter_obj = Q(type__in=value)
                if idv_flag:
                    filter_obj |= Q(pulled_from='IDV')
                queryset &= model.objects.filter(filter_obj)

        # agencies
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
            if len(funding_toptier) != 0:
                queryset &= model.objects.filter(
                    funding_toptier_agency_name__in=funding_toptier
                )
            if len(funding_subtier) != 0:
                queryset &= model.objects.filter(
                    funding_subtier_agency_name__in=funding_subtier
                )
            if len(awarding_toptier) != 0:
                queryset &= model.objects.filter(
                    awarding_toptier_agency_name__in=awarding_toptier
                )
            if len(awarding_subtier) != 0:
                queryset &= model.objects.filter(
                    awarding_subtier_agency_name__in=awarding_subtier
                )

        elif key == "legal_entities":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= model.objects.filter(
                    recipient_id__in=or_queryset
                )

        # recipient_search_text
        elif key == "recipient_search_text":
            if len(value) != 1:
                raise InvalidParameterException('Invalid filter: recipient_search_text must have exactly one value.')
            recipient_string = str(value[0])

            filter_obj = Q(recipient_name__icontains=recipient_string)

            if len(recipient_string) == 9:
                filter_obj |= Q(recipient_unique_id__iexact=recipient_string)

            queryset &= model.objects.filter(filter_obj)

        # recipient_location_scope (broken till data reload)
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

        # recipient_location
        elif key == "recipient_locations":
            or_queryset = geocode_filter_locations(
                'recipient_location', value, model, True
            )

            queryset &= or_queryset

        # recipient_type_names
        elif key == "recipient_type_names":
            if len(value) != 0:
                queryset &= model.objects.filter(business_categories__overlap=value)

        # place_of_performance_scope
        elif key == "place_of_performance_scope":
            if value == "domestic":
                queryset = queryset.filter(pop_country_name="UNITED STATES")
            elif value == "foreign":
                queryset = queryset.exclude(pop_country_name="UNITED STATES")
            else:
                raise InvalidParameterException('Invalid filter: place_of_performance_scope is invalid.')

        # place_of_performance
        elif key == "place_of_performance_locations":
            or_queryset = geocode_filter_locations(
                'pop', value, model, True
            )
            queryset &= or_queryset

        # award_amounts
        elif key == "award_amounts":
            success, or_queryset = total_obligation_queryset(value, model)
            if success:
                queryset &= or_queryset

        # award_ids
        elif key == "award_ids":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= model.objects.filter(award_id__in=or_queryset)

        # program_numbers
        elif key == "program_numbers":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= model.objects.filter(
                    cfda_number__in=or_queryset)

        # naics_codes
        elif key == "naics_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= model.objects.filter(
                    naics_code__in=or_queryset)

        # psc_codes
        elif key == "psc_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= model.objects.filter(
                    psc_code__in=or_queryset)

        # contract_pricing_type_codes
        elif key == "contract_pricing_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= model.objects.filter(
                    type_of_contract_pricing__in=or_queryset)

        # set_aside_type_codes
        elif key == "set_aside_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= model.objects.filter(
                    type_set_aside__in=or_queryset)

        # extent_competed_type_codes
        elif key == "extent_competed_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= model.objects.filter(
                    extent_competed__in=or_queryset)

    return queryset
