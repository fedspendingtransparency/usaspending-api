import logging

from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from django.db.models import Sum, Count, F, Value, FloatField
from django.db.models.functions import ExtractMonth, ExtractYear, Cast, Coalesce

from usaspending_api.awards.models_matviews import UniversalAwardView, UniversalTransactionView
from usaspending_api.awards.v2.filters.filter_helpers import sum_transaction_amount
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_by_category as sbc_view_queryset
from usaspending_api.awards.v2.lookups.lookups import (award_type_mapping, contract_type_mapping, loan_type_mapping,
                                                       non_loan_assistance_type_mapping, grant_type_mapping,
                                                       contract_subaward_mapping, grant_subaward_mapping)
from usaspending_api.awards.v2.lookups.matview_lookups import (award_contracts_mapping, loan_award_mapping,
                                                               non_loan_assistance_award_mapping)
from usaspending_api.common.exceptions import ElasticsearchConnectionException, InvalidParameterException
from usaspending_api.common.helpers import generate_fiscal_month, generate_fiscal_year, get_simple_pagination_metadata
from usaspending_api.core.validator.award_filter import AWARD_FILTER
from usaspending_api.core.validator.pagination import PAGINATION
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.references.abbreviations import code_to_state, fips_to_code, pad_codes
from usaspending_api.references.models import Cfda
from usaspending_api.search.v2.elasticsearch_helper import (search_transactions, spending_by_transaction_count,
                                                            spending_by_transaction_sum_and_count)

logger = logging.getLogger(__name__)


class business_logic:
    def __init__(self, payload):
        self.category = payload['category']
        self.scope = payload['scope']
        self.page = payload['page']
        self.limit = payload['limit']
        self.filters = {
            item['name']: payload[item['name']] for item in AWARD_FILTER if item['name'] in payload}

        self.lower_limit = (self.page - 1) * self.limit
        self.upper_limit = self.page * self.limit

        if (self.scope is None) and (self.category != "cfda_programs"):
            raise InvalidParameterException("Missing one or more required request parameters: scope")

        self.filter_types = self.filters['award_type_codes'] if 'award_type_codes' in self.filters else award_type_mapping

    def logic(self):
        # filter queryset
        # queryset = matview_search_filter(self.filters, UniversalTransactionView)

        # filter the transactions by category
        if self.category == "awarding_agency":
            results = self.awarding_agency()
        elif self.category == "funding_agency":
            results = []
        else:  # recipient_type
            raise InvalidParameterException("Category \"{}\" is not yet implemented".format(self.category))

        page_metadata = get_simple_pagination_metadata(len(results), self.limit, self.page)

        response = {
            "category": self.category,
            "limit": self.limit,
            "page_metadata": page_metadata,
            "results": results[:self.limit],
            "scope": self.scope,
        }
        return response

        # elif self.category == "funding_agency":
        #     if self.scope == "agency":
        #         queryset = queryset \
        #             .filter(funding_toptier_agency_name__isnull=False) \
        #             .values(
        #                 agency_name=F('funding_toptier_agency_name'),
        #                 agency_abbreviation=F('funding_toptier_agency_abbreviation'))

        #     elif self.scope == "subagency":
        #         queryset = queryset \
        #             .filter(
        #                 funding_subtier_agency_name__isnull=False) \
        #             .values(
        #                 agency_name=F('funding_subtier_agency_name'),
        #                 agency_abbreviation=F('funding_subtier_agency_abbreviation'))

        #     elif self.scope == "office":
        #         # NOT IMPLEMENTED IN UI
        #         raise NotImplementedError

        #     queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=self.filter_types) \
        #         .order_by('-aggregated_amount')
        #     results = list(queryset[self.lower_limit:self.upper_limit + 1])

        #     page_metadata = get_simple_pagination_metadata(len(results), self.limit, self.page)
        #     results = results[:self.limit]

        #     response = {"category": category, "scope": scope, "limit": limit, "results": results,
        #                 "page_metadata": page_metadata}
        #     return response

        # elif category == "recipient":
        #     if scope == "duns":
        #         queryset = queryset \
        #             .values(legal_entity_id=F("recipient_id"))
        #         queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
        #             .order_by('-aggregated_amount') \
        #             .values("aggregated_amount", "legal_entity_id", "recipient_name") \
        #             .order_by("-aggregated_amount")

        #         # Begin DB hits here
        #         results = list(queryset[lower_limit:upper_limit + 1])

        #         page_metadata = get_simple_pagination_metadata(len(results), limit, page)
        #         results = results[:limit]

        #     elif scope == "parent_duns":
        #         queryset = queryset \
        #             .filter(parent_recipient_unique_id__isnull=False)
        #         queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types,
        #                                           calculate_totals=False) \
        #             .values(
        #                 'aggregated_amount',
        #                 'recipient_name',
        #                 'parent_recipient_unique_id') \
        #             .order_by('-aggregated_amount')

        #         # Begin DB hits here
        #         results = list(queryset[lower_limit:upper_limit + 1])
        #         page_metadata = get_simple_pagination_metadata(len(results), limit, page)
        #         results = results[:limit]

        #     else:  # recipient_type
        #         raise InvalidParameterException("recipient type is not yet implemented")

        #     response = {"category": category, "scope": scope, "limit": limit, "results": results,
        #                 "page_metadata": page_metadata}
        #     return response

        # elif category == "cfda_programs":
        #     if can_use_view(filters, 'SummaryCfdaNumbersView'):
        #         queryset = get_view_queryset(filters, 'SummaryCfdaNumbersView')
        #         queryset = queryset \
        #             .filter(
        #                 federal_action_obligation__isnull=False,
        #                 cfda_number__isnull=False) \
        #             .values(cfda_program_number=F("cfda_number"))
        #         queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
        #             .values(
        #                 "aggregated_amount",
        #                 "cfda_program_number",
        #                 program_title=F("cfda_title")) \
        #             .order_by('-aggregated_amount')

        #         # Begin DB hits here
        #         results = list(queryset[lower_limit:upper_limit + 1])
        #         page_metadata = get_simple_pagination_metadata(len(results), limit, page)
        #         results = results[:limit]
        #         for trans in results:
        #             trans['popular_name'] = None
        #             # small DB hit every loop here
        #             cfda = Cfda.objects \
        #                 .filter(
        #                     program_title=trans['program_title'],
        #                     program_number=trans['cfda_program_number']) \
        #                 .values('popular_name').first()

        #             if cfda:
        #                 trans['popular_name'] = cfda['popular_name']

        #     else:
        #         queryset = queryset \
        #             .filter(
        #                 cfda_number__isnull=False) \
        #             .values(cfda_program_number=F("cfda_number"))
        #         queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
        #             .values(
        #                 "aggregated_amount",
        #                 "cfda_program_number",
        #                 popular_name=F("cfda_popular_name"),
        #                 program_title=F("cfda_title")) \
        #             .order_by('-aggregated_amount')

        #         # Begin DB hits here
        #         results = list(queryset[lower_limit:upper_limit + 1])
        #         page_metadata = get_simple_pagination_metadata(len(results), limit, page)
        #         results = results[:limit]

        #     response = {"category": category, "limit": limit, "results": results, "page_metadata": page_metadata}
        #     return response

        # elif category == "industry_codes":  # industry_codes
        #     if scope == "psc":
        #         if can_use_view(filters, 'SummaryPscCodesView'):
        #             queryset = get_view_queryset(filters, 'SummaryPscCodesView')
        #             queryset = queryset \
        #                 .filter(product_or_service_code__isnull=False) \
        #                 .values(psc_code=F("product_or_service_code"))
        #         else:
        #             queryset = queryset \
        #                 .filter(psc_code__isnull=False) \
        #                 .values("psc_code")

        #         queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
        #             .order_by('-aggregated_amount')
        #         # Begin DB hits here
        #         results = list(queryset[lower_limit:upper_limit + 1])

        #         page_metadata = get_simple_pagination_metadata(len(results), limit, page)
        #         results = results[:limit]

        #         response = {"category": category, "scope": scope, "limit": limit, "results": results,
        #                     "page_metadata": page_metadata}
        #         return response

        #     elif scope == "naics":
        #         if can_use_view(filters, 'SummaryNaicsCodesView'):
        #             queryset = get_view_queryset(filters, 'SummaryNaicsCodesView')
        #             queryset = queryset \
        #                 .filter(naics_code__isnull=False) \
        #                 .values('naics_code')
        #             queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
        #                 .order_by('-aggregated_amount') \
        #                 .values(
        #                     'naics_code',
        #                     'aggregated_amount',
        #                     'naics_description')
        #         else:
        #             queryset = queryset \
        #                 .filter(naics_code__isnull=False) \
        #                 .values("naics_code")
        #             queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=filter_types) \
        #                 .order_by('-aggregated_amount') \
        #                 .values(
        #                     'naics_code',
        #                     'aggregated_amount',
        #                     'naics_description')

        #         # Begin DB hits here
        #         results = list(queryset[lower_limit:upper_limit + 1])

        #         page_metadata = get_simple_pagination_metadata(len(results), limit, page)
        #         results = results[:limit]

        #         response = {"category": category, "scope": scope, "limit": limit, "results": results,
        #                     "page_metadata": page_metadata}
        #     else:  # recipient_type
        #         raise InvalidParameterException("recipient type is not yet implemented")
        # return response

    def awarding_agency(self):
        # filter queryset
        queryset = sbc_view_queryset(self.category, self.filters)
        if self.scope == "agency":
            queryset = queryset \
                .filter(awarding_toptier_agency_name__isnull=False) \
                .values(
                    agency_name=F('awarding_toptier_agency_name'),
                    agency_abbreviation=F('awarding_toptier_agency_abbreviation'))

        elif self.scope == "subagency":
            queryset = queryset \
                .filter(
                    awarding_subtier_agency_name__isnull=False) \
                .values(
                    agency_name=F('awarding_subtier_agency_name'),
                    agency_abbreviation=F('awarding_subtier_agency_abbreviation'))

        elif self.scope == "office":
                # NOT IMPLEMENTED IN UI
                raise NotImplementedError

        queryset = sum_transaction_amount(queryset, 'aggregated_amount', filter_types=self.filter_types)\
            .order_by('-aggregated_amount')
        results = list(queryset[self.lower_limit:self.upper_limit + 1])

        return results
