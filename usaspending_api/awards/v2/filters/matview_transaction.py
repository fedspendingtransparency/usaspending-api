import logging
from django.db.models import Q

from usaspending_api.awards.models_matviews import UniversalTransactionView, UniversalAwardView
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations

from usaspending_api.awards.v2.lookups.lookups import contract_type_mapping
from usaspending_api.references.constants import WEBSITE_AWARD_BINS
from usaspending_api.common.helpers import dates_are_fiscal_year_bookends
from usaspending_api.common.helpers import generate_all_fiscal_years_in_range
from usaspending_api.common.helpers import generate_date_from_string

logger = logging.getLogger(__name__)


def date_or_fy_queryset(date_dict):
    full_fiscal_years = []
    for v in date_dict:
        s = generate_date_from_string(v.get("start_date"))
        e = generate_date_from_string(v.get("end_date"))
        if dates_are_fiscal_year_bookends(s, e):
            full_fiscal_years.append((s, e))

    if len(full_fiscal_years) == len(date_dict):
        fys = []
        for s, e in full_fiscal_years:
            fys.append(generate_all_fiscal_years_in_range(s, e))
        all_fiscal_years = set([x for sublist in fys for x in sublist])
        return True, UniversalTransactionView.objects.filter(fiscal_year__in=all_fiscal_years)

    or_queryset = None
    queryset_init = False

    for v in date_dict:
        kwargs = {}
        if v.get("start_date") is not None:
            kwargs["action_date__gte"] = v.get("start_date")
        if v.get("end_date") is not None:
            kwargs["action_date__lte"] = v.get("end_date")
        # (may have to cast to date) (oct 1 to sept 30)
        if queryset_init:
            or_queryset |= UniversalTransactionView.objects.filter(**kwargs)
        else:
            queryset_init = True
            or_queryset = UniversalTransactionView.objects.filter(**kwargs)
    if queryset_init:
        return True, or_queryset
    return False, None


def total_obligation_queryset(amount_obj):
    bins = []
    for v in amount_obj:
        lower_bound = v.get("lower_bound")
        upper_bound = v.get("upper_bound")
        for key, limits in WEBSITE_AWARD_BINS.items():
            if lower_bound == limits['lower'] and upper_bound == limits['upper']:
                bins.append(key)
                break

    if len(bins) == len(amount_obj):
        return True, UniversalTransactionView.objects.filter(total_obl_bin__in=bins)
    else:
        or_queryset = None
        queryset_init = False

        for v in amount_obj:
            if v.get("lower_bound") is not None and v.get("upper_bound") is not None:
                if queryset_init:
                    or_queryset |= UniversalTransactionView.objects.filter(
                        total_obligation__gt=v["lower_bound"],
                        total_obligation__lt=v["upper_bound"]
                    )
                else:
                    queryset_init = True
                    or_queryset = UniversalTransactionView.objects.filter(
                        total_obligation__gt=v["lower_bound"],
                        total_obligation__lt=v["upper_bound"])
            elif v.get("lower_bound") is not None:
                if queryset_init:
                    or_queryset |= UniversalTransactionView.objects.filter(
                        total_obligation__gt=v["lower_bound"]
                    )
                else:
                    queryset_init = True
                    or_queryset = UniversalTransactionView.objects.filter(
                        total_obligation__gt=v["lower_bound"])
            elif v.get("upper_bound") is not None:
                if queryset_init:
                    or_queryset |= UniversalTransactionView.objects.filter(
                        total_obligation__lt=v["upper_bound"]
                    )
                else:
                    queryset_init = True
                    or_queryset = UniversalTransactionView.objects.filter(
                        total_obligation__lt=v["upper_bound"])
            else:
                raise InvalidParameterException('Invalid filter: award amount has incorrect object.')
    if queryset_init:
        return True, or_queryset
    return False, None


# TODO: Performance when multiple false values are initially provided
def transaction_filter(filters):
    queryset = UniversalTransactionView.objects.all()
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

        if key == "keyword":
            keyword = value

            if keyword.isnumeric():
                naics_q = Q(naics_code__icontains=keyword)
            else:
                naics_q = Q(naics_description__icontains=keyword)

            if len(keyword) == 4:
                psc_q = Q(psc_code__in=keyword)
            else:
                psc_q = Q(psc_description__icontains=keyword)

            queryset = queryset.filter(
                Q(recipient_name__icontains=keyword) |
                Q(piid=keyword) |
                Q(fain=keyword) |
                naics_q |
                psc_q |
                Q(transaction_description__icontains=keyword) |
                Q(recipient_unique_id__in=keyword) |
                Q(parent_recipient_unique_id__in=keyword)
            )

        # time_period
        elif key == "time_period":
            success, or_queryset = date_or_fy_queryset(value)
            if success:
                queryset &= or_queryset

        # award_type_codes
        elif key == "award_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalTransactionView.objects.filter(type__in=or_queryset)

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
                queryset &= UniversalTransactionView.objects.filter(
                    funding_toptier_agency_name__in=funding_toptier
                )
            if len(funding_subtier) != 0:
                queryset &= UniversalTransactionView.objects.filter(
                    funding_subtier_agency_name__in=funding_subtier
                )
            if len(awarding_toptier) != 0:
                queryset &= UniversalTransactionView.objects.filter(
                    awarding_toptier_agency_name__in=awarding_toptier
                )
            if len(awarding_subtier) != 0:
                queryset &= UniversalTransactionView.objects.filter(
                    awarding_subtier_agency_name__in=awarding_subtier
                )

        # legal_entities
        elif key == "legal_entities":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalTransactionView.objects.filter(
                    recipient_id__in=or_queryset
                )

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
                'recipient_location', value, 'UniversalTransactionView', True
            )

            queryset &= or_queryset

        # recipient_type_names
        elif key == "recipient_type_names":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalTransactionView.objects.filter(
                    business_types_description__in=or_queryset
                )

        # place_of_performance_scope (broken till data reload
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
                'pop', value, 'UniversalTransactionView', True
            )

            queryset &= or_queryset

        # award_amounts
        elif key == "award_amounts":
            success, or_queryset = total_obligation_queryset(value)
            if success:
                queryset &= or_queryset

        # award_ids
        elif key == "award_ids":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalTransactionView.objects.filter(award_id__in=or_queryset)

        # program_numbers
        elif key == "program_numbers":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalTransactionView.objects.filter(
                    cfda_number__in=or_queryset)

        # naics_codes
        elif key == "naics_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalTransactionView.objects.filter(
                    naics_code__in=or_queryset)

        # psc_codes
        elif key == "psc_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalTransactionView.objects.filter(
                    psc_code__in=or_queryset)

        # contract_pricing_type_codes
        elif key == "contract_pricing_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalTransactionView.objects.filter(
                    type_of_contract_pricing__in=or_queryset)

        # set_aside_type_codes
        elif key == "set_aside_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalTransactionView.objects.filter(
                    type_set_aside__in=or_queryset)

        # extent_competed_type_codes
        elif key == "extent_competed_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalTransactionView.objects.filter(
                    extent_competed__in=or_queryset)

    return queryset


# TODO: Performance when multiple false values are initially provided
def award_filter(filters):

    queryset = UniversalAwardView.objects.filter()
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
            keyword = value

            if keyword.isnumeric():
                naics_q = Q(naics_code__icontains=keyword)
            else:
                naics_q = Q(naics_description__icontains=keyword)

            if len(keyword) == 4:
                psc_q = Q(psc_code__in=keyword)
            else:
                psc_q = Q(psc_description__icontains=keyword)

            queryset = queryset.filter(
                Q(recipient_name__icontains=keyword) |
                Q(piid=keyword) |
                Q(fain=keyword) |
                naics_q |
                psc_q |
                Q(description__icontains=keyword) |
                Q(recipient_unique_id__in=keyword) |
                Q(parent_recipient_unique_id__in=keyword)
            )

        elif key == "time_period":
            or_queryset = None
            queryset_init = False
            for v in value:
                kwargs = {}
                if v.get("start_date") is not None:
                    kwargs["issued_date__gte"] = v.get("start_date")
                if v.get("end_date") is not None:
                    kwargs["issued_date__lte"] = v.get("end_date")
                # (may have to cast to date) (oct 1 to sept 30)
                if queryset_init:
                    or_queryset |= UniversalAwardView.objects.filter(**kwargs)
                else:
                    queryset_init = True
                    or_queryset = UniversalAwardView.objects.filter(**kwargs)
            if queryset_init:
                queryset &= or_queryset

        elif key == "award_type_codes":
            or_queryset = []

            idv_flag = all(i in value for i in contract_type_mapping.keys())

            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalAwardView.objects.filter(type__in=or_queryset)
            if idv_flag:
                queryset |= UniversalAwardView.objects.filter(type__isnull=True, pulled_from='IDV')

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
                queryset &= UniversalAwardView.objects.filter(funding_toptier_agency_name__in=funding_toptier)
            if len(funding_subtier) != 0:
                queryset &= UniversalAwardView.objects.filter(funding_subtier_agency_name__in=funding_subtier)
            if len(awarding_toptier) != 0:
                queryset &= UniversalAwardView.objects.filter(awarding_toptier_agency_name__in=awarding_toptier)
            if len(awarding_subtier) != 0:
                queryset &= UniversalAwardView.objects.filter(awarding_subtier_agency_name__in=awarding_subtier)

        elif key == "legal_entities":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalAwardView.objects.filter(recipient_id__in=or_queryset)

        elif key == "recipient_scope":
            if value == "domestic":
                queryset = queryset.filter(recipient_location_country_name="UNITED STATES")
            elif value == "foreign":
                queryset = queryset.exclude(recipient_location_country_name="UNITED STATES")
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
                queryset &= UniversalAwardView.objects.filter(business_types_description__in=or_queryset)

        elif key == "place_of_performance_scope":
            if value == "domestic":
                queryset = queryset.filter(pop_country_name="UNITED STATES")
            elif value == "foreign":
                queryset = queryset.exclude(pop_country_name="UNITED STATES")
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
                        or_queryset |= UniversalAwardView.objects.filter(
                            total_obligation__gt=v["lower_bound"],
                            total_obligation__lt=v["upper_bound"])
                    else:
                        queryset_init = True
                        or_queryset = UniversalAwardView.objects.filter(
                            total_obligation__gt=v["lower_bound"],
                            total_obligation__lt=v["upper_bound"])
                elif v.get("lower_bound") is not None:
                    if queryset_init:
                        or_queryset |= UniversalAwardView.objects.filter(
                            total_obligation__gt=v["lower_bound"])
                    else:
                        queryset_init = True
                        or_queryset = UniversalAwardView.objects.filter(
                            total_obligation__gt=v["lower_bound"])
                elif v.get("upper_bound") is not None:
                    if queryset_init:
                        or_queryset |= UniversalAwardView.objects.filter(
                            total_obligation__lt=v["upper_bound"])
                    else:
                        queryset_init = True
                        or_queryset = UniversalAwardView.objects.filter(
                            total_obligation__lt=v["upper_bound"])
                else:
                    raise InvalidParameterException('Invalid filter: award amount has incorrect object.')
            if queryset_init:
                queryset &= or_queryset

        elif key == "award_ids":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalAwardView.objects.filter(award_id__in=or_queryset)

        elif key == "program_numbers":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalAwardView.objects.filter(
                    cfda_number__in=or_queryset)

        elif key == "naics_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalAwardView.objects.filter(
                    naics__in=or_queryset)

        elif key == "psc_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalAwardView.objects.filter(
                    product_or_service_code__in=or_queryset)

        elif key == "contract_pricing_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalAwardView.objects.filter(
                    type_of_contract_pricing__in=or_queryset)

        elif key == "set_aside_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalAwardView.objects.filter(
                    type_set_aside__in=or_queryset)

        elif key == "extent_competed_type_codes":
            or_queryset = []
            for v in value:
                or_queryset.append(v)
            if len(or_queryset) != 0:
                queryset &= UniversalAwardView.objects.filter(
                    extent_competed__in=or_queryset)

    return queryset
