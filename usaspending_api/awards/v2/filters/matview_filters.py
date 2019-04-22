import logging
import itertools

from django.db.models import Q

from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import PSC
from usaspending_api.accounts.views.federal_accounts_v2 import filter_on
from .filter_helpers import combine_date_range_queryset, total_obligation_queryset
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.awards.models_matviews import UniversalAwardView, UniversalTransactionView
from usaspending_api.search.v2 import elasticsearch_helper
from usaspending_api.settings import API_MAX_DATE, API_MIN_DATE, API_SEARCH_MIN_DATE
from usaspending_api.recipient.models import RecipientProfile

logger = logging.getLogger(__name__)


def universal_award_matview_filter(filters):
    return matview_search_filter(filters, UniversalAwardView, for_downloads=True)


def universal_transaction_matview_filter(filters):
    return matview_search_filter(filters, UniversalTransactionView, for_downloads=True)


def matview_search_filter(filters, model, for_downloads=False):
    queryset = model.objects.all()

    recipient_scope_q = Q(recipient_location_country_code="USA") | Q(recipient_location_country_name="UNITED STATES")
    pop_scope_q = Q(pop_country_code="USA") | Q(pop_country_name="UNITED STATES")

    faba_flag = False
    faba_queryset = FinancialAccountsByAwards.objects.filter(award__isnull=False)

    for key, value in filters.items():
        if value is None:
            raise InvalidParameterException('Invalid filter: ' + key + ' has null as its value.')

        key_list = [
            'keywords',
            'elasticsearch_keyword',
            'time_period',
            'award_type_codes',
            'agencies',
            'legal_entities',
            'recipient_id',
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
            'extent_competed_type_codes',
            # next 3 keys used by federal account page
            'federal_account_ids',
            'object_class',
            'program_activity'
        ]

        if key not in key_list:
            raise InvalidParameterException('Invalid filter: ' + key + ' does not exist.')

        if key == "keywords":
            def keyword_parse(keyword):
                # keyword_ts_vector & award_ts_vector are Postgres TS_vectors.
                # keyword_ts_vector = recipient_name + naics_code + naics_description
                #     + psc_description + awards_description
                # award_ts_vector = piid + fain + uri
                filter_obj = Q(keyword_ts_vector=keyword) | Q(award_ts_vector=keyword)
                if keyword.isnumeric():
                    filter_obj |= Q(naics_code__contains=keyword)
                if len(keyword) == 4 and PSC.objects.all().filter(code__iexact=keyword).exists():
                    filter_obj |= Q(product_or_service_code__iexact=keyword)

                return filter_obj

            filter_obj = Q()
            for keyword in value:
                filter_obj |= keyword_parse(keyword)
            potential_duns = list(filter((lambda x: len(x) > 7 and len(x) < 10), value))
            if len(potential_duns) > 0:
                filter_obj |= Q(recipient_unique_id__in=potential_duns) | \
                    Q(parent_recipient_unique_id__in=potential_duns)

            queryset = queryset.filter(filter_obj)

        elif key == "elasticsearch_keyword":
            keyword = " ".join(value) if isinstance(value, list) else value
            transaction_ids = elasticsearch_helper.get_download_ids(keyword=keyword, field='transaction_id')
            # flatten IDs
            transaction_ids = list(itertools.chain.from_iterable(transaction_ids))
            logger.info('Found {} transactions based on keyword: {}'.format(len(transaction_ids), keyword))
            transaction_ids = [str(transaction_id) for transaction_id in transaction_ids]
            queryset = queryset.extra(
                where=['"transaction_normalized"."id" = ANY(\'{{{}}}\'::int[])'.format(','.join(transaction_ids))])

        elif key == "time_period":
            min_date = API_SEARCH_MIN_DATE
            if for_downloads:
                min_date = API_MIN_DATE
            queryset &= combine_date_range_queryset(value, model, min_date, API_MAX_DATE)

        elif key == "award_type_codes":
            queryset = queryset.filter(type__in=value)

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
                        if 'toptier_name' in v:
                            funding_subtier |= (Q(funding_subtier_agency_name=name) &
                                                Q(funding_toptier_agency_name=v['toptier_name']))
                        else:
                            funding_subtier |= Q(funding_subtier_agency_name=name)

                elif type == "awarding":
                    if tier == "toptier":
                        awarding_toptier |= Q(awarding_toptier_agency_name=name)
                    elif tier == "subtier":
                        if 'toptier_name' in v:
                            awarding_subtier |= (Q(awarding_subtier_agency_name=name) &
                                                 Q(awarding_toptier_agency_name=v['toptier_name']))
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
            # in_query = [v for v in value]
            # if len(in_query) != 0:
            #     queryset &= model.objects.filter(recipient_id__in=in_query)

        elif key == "recipient_search_text":
            all_filters_obj = Q()
            for recip in value:
                upper_recipient_string = str(recip).upper()
                # recipient_name_ts_vector is a postgres TS_Vector
                filter_obj = Q(recipient_name_ts_vector=upper_recipient_string)
                if len(upper_recipient_string) == 9 and upper_recipient_string[:5].isnumeric():
                    filter_obj |= Q(recipient_unique_id=upper_recipient_string)
                all_filters_obj |= filter_obj
            queryset = queryset.filter(all_filters_obj)

        elif key == "recipient_id":
            filter_obj = Q()
            recipient_hash = value[:-2]

            if value.endswith('P'):  # For parent types, gather all of the children's transactions
                parent_duns_rows = (
                    RecipientProfile.objects.filter(recipient_hash=recipient_hash, recipient_level='P')
                    .values('recipient_unique_id')
                )
                if len(parent_duns_rows) == 1:
                    parent_duns = parent_duns_rows[0]['recipient_unique_id']
                    filter_obj = Q(parent_recipient_unique_id=parent_duns)
                elif len(parent_duns_rows) > 2:
                    # shouldn't occur
                    raise InvalidParameterException('Non-unique parent record found in RecipientProfile')
            elif value.endswith('C'):
                filter_obj = Q(recipient_hash=recipient_hash, parent_recipient_unique_id__isnull=False)
            else:
                # "R" recipient level
                filter_obj = Q(recipient_hash=recipient_hash, parent_recipient_unique_id__isnull=True)
            queryset = queryset.filter(filter_obj)

        elif key == "recipient_scope":
            if value == "domestic":
                queryset = queryset.filter(recipient_scope_q)
            elif value == "foreign":
                queryset = queryset.exclude(recipient_scope_q)
            else:
                raise InvalidParameterException('Invalid filter: recipient_scope type is invalid.')

        elif key == "recipient_locations":
            queryset = queryset.filter(geocode_filter_locations('recipient_location', value, True))

        elif key == "recipient_type_names":
            if len(value) != 0:
                queryset = queryset.filter(business_categories__overlap=value)

        elif key == "place_of_performance_scope":
            if value == "domestic":
                queryset = queryset.filter(pop_scope_q)
            elif value == "foreign":
                queryset = queryset.exclude(pop_scope_q)
            else:
                raise InvalidParameterException('Invalid filter: place_of_performance_scope is invalid.')

        elif key == "place_of_performance_locations":
            queryset = queryset.filter(geocode_filter_locations('pop', value, True))

        elif key == "award_amounts":
            queryset &= total_obligation_queryset(value, model, filters)

        elif key == "award_ids":
            filter_obj = Q()
            for val in value:
                # award_id_string is a Postgres TS_vector
                # award_id_string = piid + fain + uri
                filter_obj |= Q(award_ts_vector=val)
            queryset = queryset.filter(filter_obj)

        elif key == "program_numbers":
            in_query = [v for v in value]
            if len(in_query) != 0:
                queryset = queryset.filter(cfda_number__in=in_query)

        elif key == "naics_codes":
            in_query = [v for v in value]
            if len(in_query) != 0:
                queryset = queryset.filter(naics_code__in=in_query)

        elif key == "psc_codes":
            in_query = [v for v in value]
            if len(in_query) != 0:
                queryset = queryset.filter(product_or_service_code__in=in_query)

        elif key == "contract_pricing_type_codes":
            in_query = [v for v in value]
            if len(in_query) != 0:
                queryset = queryset.filter(type_of_contract_pricing__in=in_query)

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

        # Federal Account Filter
        elif key == "federal_account_ids":
            faba_flag = True
            or_queryset = Q()
            for v in value:
                or_queryset |= Q(treasury_account__federal_account_id=v)
            faba_queryset = faba_queryset.filter(or_queryset)

        # Federal Account Filter
        elif key == "object_class":
            faba_flag = True
            result = Q()
            for oc in value:
                subresult = Q()
                for (key, values) in oc.items():
                    subresult &= filter_on("treasury_account__program_balances__object_class", key, values)
                result |= subresult
            faba_queryset = faba_queryset.filter(result)

        # Federal Account Filter
        elif key == "program_activity":
            faba_flag = True
            or_queryset = Q()
            for v in value:
                or_queryset |= Q(treasury_account__program_balances__program_activity__program_activity_code=v)
            faba_queryset = faba_queryset.filter(or_queryset)

    if faba_flag:
        award_ids = faba_queryset.values('award_id')
        queryset = queryset.filter(award_id__in=award_ids)

    return queryset
