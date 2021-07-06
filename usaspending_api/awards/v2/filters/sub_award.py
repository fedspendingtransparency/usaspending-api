import itertools
import logging

from django.db.models import Q, query
from django.db.models.expressions import OuterRef

from usaspending_api.awards.v2.filters.filter_helpers import combine_date_range_queryset, total_obligation_queryset
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import PSC
from usaspending_api.search.filters.postgres.defc import DefCodes
from usaspending_api.search.filters.postgres.psc import PSCCodes
from usaspending_api.search.filters.postgres.tas import TasCodes, TreasuryAccounts
from usaspending_api.search.helpers.matview_filter_helpers import build_award_ids_filter
from usaspending_api.search.models import SubawardView
from usaspending_api.search.models import TransactionSearch as TransactionSearchModel
from usaspending_api.search.v2 import elasticsearch_helper
from usaspending_api.settings import API_MAX_DATE, API_MIN_DATE, API_SEARCH_MIN_DATE


logger = logging.getLogger(__name__)


def subaward_download(filters):
    """ Used by the Custom download"""
    return subaward_filter(filters, for_downloads=True)


# TODO: Performance when multiple false values are initially provided
def subaward_filter(filters, for_downloads=False):
    queryset = SubawardView.objects.all()

    recipient_scope_q = Q(recipient_location_country_code="USA") | Q(recipient_location_country_name="UNITED STATES")
    pop_scope_q = Q(pop_country_code="USA") | Q(pop_country_name="UNITED STATES")

    for key, value in filters.items():

        if value is None:
            raise InvalidParameterException("Invalid filter: " + key + " has null as its value.")

        key_list = [
            "keywords",
            "elasticsearch_keyword",
            "time_period",
            "award_type_codes",
            "prime_and_sub_award_types",
            "agencies",
            "legal_entities",
            "recipient_search_text",
            "recipient_scope",
            "recipient_locations",
            "recipient_type_names",
            "place_of_performance_scope",
            "place_of_performance_locations",
            "award_amounts",
            "award_ids",
            "program_numbers",
            "naics_codes",
            PSCCodes.underscore_name,
            "contract_pricing_type_codes",
            "set_aside_type_codes",
            "extent_competed_type_codes",
            TasCodes.underscore_name,
            TreasuryAccounts.underscore_name,
            "def_codes",
        ]

        if key not in key_list:
            raise InvalidParameterException("Invalid filter: " + key + " does not exist.")

        if key == "keywords":

            def keyword_parse(keyword):
                # keyword_ts_vector & award_ts_vector are Postgres TS_vectors.
                # keyword_ts_vector = recipient_name + psc_description + subaward_description
                # award_ts_vector = piid + fain + uri + subaward_number
                filter_obj = Q(keyword_ts_vector=keyword) | Q(award_ts_vector=keyword)
                # Commenting out until NAICS is associated with subawards in DAIMS 1.3.1
                # if keyword.isnumeric():
                #     filter_obj |= Q(naics_code__contains=keyword)
                if len(keyword) == 4 and PSC.objects.filter(code__iexact=keyword).exists():
                    filter_obj |= Q(product_or_service_code__iexact=keyword)

                return filter_obj

            filter_obj = Q()
            for keyword in value:
                filter_obj |= keyword_parse(keyword)
            potential_duns = list(filter((lambda x: len(x) > 7 and len(x) < 10), value))
            if len(potential_duns) > 0:
                filter_obj |= Q(recipient_unique_id__in=potential_duns) | Q(
                    parent_recipient_unique_id__in=potential_duns
                )

            queryset = queryset.filter(filter_obj)

        elif key == "elasticsearch_keyword":
            keyword = value
            transaction_ids = elasticsearch_helper.get_download_ids(keyword=keyword, field="transaction_id")
            # flatten IDs
            transaction_ids = list(itertools.chain.from_iterable(transaction_ids))
            logger.info("Found {} transactions based on keyword: {}".format(len(transaction_ids), keyword))
            transaction_ids = [str(transaction_id) for transaction_id in transaction_ids]
            queryset = queryset.filter(latest_transaction_id__isnull=False)

            # Prepare a SQL snippet to include in the predicate for searching an array of transaction IDs
            sql_fragment = '"subaward_view"."latest_transaction_id" = ANY(\'{{{}}}\'::int[])'  # int[] -> int array type
            queryset = queryset.extra(where=[sql_fragment.format(",".join(transaction_ids))])

        elif key == "time_period":
            min_date = API_SEARCH_MIN_DATE
            if for_downloads:
                min_date = API_MIN_DATE
            queryset &= combine_date_range_queryset(value, SubawardView, min_date, API_MAX_DATE)

        elif key == "award_type_codes":
            queryset = queryset.filter(prime_award_type__in=value)

        elif key == "prime_and_sub_award_types":
            award_types = value.get("sub_awards")
            if award_types:
                queryset = queryset.filter(award_type__in=award_types)

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
                        funding_toptier |= Q(funding_toptier_agency_name=name)
                    elif tier == "subtier":
                        if "toptier_name" in v:
                            funding_subtier |= Q(funding_subtier_agency_name=name) & Q(
                                funding_toptier_agency_name=v["toptier_name"]
                            )
                        else:
                            funding_subtier |= Q(funding_subtier_agency_name=name)

                elif type == "awarding":
                    if tier == "toptier":
                        awarding_toptier |= Q(awarding_toptier_agency_name=name)
                    elif tier == "subtier":
                        if "toptier_name" in v:
                            awarding_subtier |= Q(awarding_subtier_agency_name=name) & Q(
                                awarding_toptier_agency_name=v["toptier_name"]
                            )
                        else:
                            awarding_subtier |= Q(awarding_subtier_agency_name=name)

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
            # This filter key has effectively become obsolete by recipient_search_text
            msg = 'API request included "{}" key. No filtering will occur with provided value "{}"'
            logger.info(msg.format(key, value))

        elif key == "recipient_search_text":

            def recip_string_parse(recipient_string):
                upper_recipient_string = recipient_string.upper()

                # recipient_name_ts_vector is a postgres TS_Vector
                filter_obj = Q(recipient_name_ts_vector=upper_recipient_string)
                if len(upper_recipient_string) == 9 and upper_recipient_string[:5].isnumeric():
                    filter_obj |= Q(recipient_unique_id=upper_recipient_string)
                return filter_obj

            filter_obj = Q()
            for recipient in value:
                filter_obj |= recip_string_parse(recipient)
            queryset = queryset.filter(filter_obj)

        elif key == "recipient_scope":
            if value == "domestic":
                queryset = queryset.filter(recipient_scope_q)
            elif value == "foreign":
                queryset = queryset.exclude(recipient_scope_q)
            else:
                raise InvalidParameterException("Invalid filter: recipient_scope type is invalid.")

        elif key == "recipient_locations":
            queryset = queryset.filter(geocode_filter_locations("recipient_location", value))

        elif key == "recipient_type_names":
            if len(value) != 0:
                queryset = queryset.filter(business_categories__overlap=value)

        elif key == "place_of_performance_scope":
            if value == "domestic":
                queryset = queryset.filter(pop_scope_q)
            elif value == "foreign":
                queryset = queryset.exclude(pop_scope_q)
            else:
                raise InvalidParameterException("Invalid filter: place_of_performance_scope is invalid.")

        elif key == "place_of_performance_locations":
            queryset = queryset.filter(geocode_filter_locations("pop", value))

        elif key == "award_amounts":
            queryset &= total_obligation_queryset(value, SubawardView, filters)

        elif key == "award_ids":
            queryset = build_award_ids_filter(queryset, value, ("piid", "fain"))

        elif key == PSCCodes.underscore_name:
            q = PSCCodes.build_tas_codes_filter(value)
            queryset = queryset.filter(q) if q else queryset

        # add "naics_codes" (column naics) after NAICS are mapped to subawards
        elif key in ("contract_pricing_type_codes"):
            if len(value) != 0:
                queryset &= SubawardView.objects.filter(type_of_contract_pricing__in=value)

        elif key == "program_numbers":
            if len(value) != 0:
                queryset &= SubawardView.objects.filter(award__transactions__cfda_number__in=value)

        elif key in ("set_aside_type_codes", "extent_competed_type_codes"):
            or_queryset = Q()
            filter_to_col = {"set_aside_type_codes": "type_set_aside", "extent_competed_type_codes": "extent_competed"}
            in_query = [v for v in value]
            for v in value:
                or_queryset |= Q(**{"{}__exact".format(filter_to_col[key]): in_query})
            queryset = queryset.filter(or_queryset)

        # Because these two filters OR with each other, we need to know about the presence of both filters to know what to do
        # This filter was picked arbitrarily to be the one that checks for the other
        elif key == TasCodes.underscore_name:
            q = TasCodes.build_tas_codes_filter(queryset, value)
            if TreasuryAccounts.underscore_name in filters.keys():
                q |= TreasuryAccounts.build_tas_codes_filter(queryset, filters[TreasuryAccounts.underscore_name])
            queryset = queryset.filter(q)

        elif key == TreasuryAccounts.underscore_name and TasCodes.underscore_name not in filters.keys():
            queryset = queryset.filter(TreasuryAccounts.build_tas_codes_filter(queryset, value))

        elif key == "def_codes":
            queryset = queryset.filter(DefCodes.build_def_codes_filter(queryset, value))
            queryset = queryset.filter(action_date__gte="2020-04-01")
    return queryset
