import copy
import logging

from django.conf import settings
from django.db.models import Sum
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_by_category as sbc_view_queryset
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.api_helper import alias_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.core.validator.award_filter import AWARD_FILTER
from usaspending_api.core.validator.pagination import PAGINATION
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.references.models import Cfda

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION

ALIAS_DICT = {
    'awarding_agency': {'awarding_toptier_agency_name': 'agency_name', 'awarding_subtier_agency_name': 'agency_name',
                        'awarding_toptier_agency_abbreviation': 'agency_abbreviation',
                        'awarding_subtier_agency_abbreviation': 'agency_abbreviation'},
    'funding_agency': {'funding_toptier_agency_name': 'agency_name', 'funding_subtier_agency_name': 'agency_name',
                       'funding_toptier_agency_abbreviation': 'agency_abbreviation',
                       'funding_subtier_agency_abbreviation': 'agency_abbreviation'},
    'recipient': {'recipient_unique_id': 'legal_entity_id'},
    'cfda_programs': {'cfda_number': 'cfda_program_number', 'cfda_popular_name': 'popular_name',
                      'cfda_title': 'popular_title'},
    'industry_codes': {'product_or_service_code': 'psc_code'}

}


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByCategoryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by the defined category/scope.
    The category is defined by the category keyword, and the scope is defined by is denoted by the scope keyword.
    endpoint_doc: /advanced_award_search/spending_by_category.md
    """
    @cache_response()
    def post(self, request: dict):
        """Return all budget function/subfunction titles matching the provided search text"""
        categories = ['awarding_agency', 'funding_agency', 'recipient', 'cfda_programs', 'industry_codes']
        scopes = ['agency', 'subagency', 'cfda', 'psc', 'naics', 'duns', 'parent_duns', None]
        models = [
            {'name': 'category', 'key': 'category', 'type': 'enum', 'enum_values': categories, 'optional': False},
            {'name': 'scope', 'key': 'scope', 'type': 'enum', 'enum_values': scopes, 'default': None},
            {'name': 'subawards', 'key': 'subawards', 'type': 'boolean', 'default': False, 'optional': True}
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))

        # Apply/enforce POST body schema and data validation in request
        validated_payload = TinyShield(models).block(request.data)

        # Execute the business logic for the endpoint and return a python dict to be converted to a Django response
        return Response(BusinessLogic(validated_payload).results())


class BusinessLogic:
    # __slots__ will keep this object smaller
    __slots__ = (
        'subawards', 'category', 'scope', 'page', 'limit', 'obligation_column',
        'lower_limit', 'upper_limit', 'filters', 'queryset', 'model',
    )

    def __init__(self, payload: dict):
        """
            payload is tightly integrated with
        """
        self.subawards = payload['subawards']
        self.category = payload['category']
        self.scope = payload['scope']
        self.page = payload['page']
        self.limit = payload['limit']
        self.filters = payload['filters']

        self.lower_limit = (self.page - 1) * self.limit
        self.upper_limit = self.page * self.limit + 1  # Add 1 for simple "Next Page" check

        if (self.scope is None) and (self.category != 'cfda_programs'):
            raise InvalidParameterException('Missing one or more required request parameters: scope')

        self.scope = self.scope if not self.category == 'cfda_programs' else None
        # some category-scope combinations allow different matviews, combine strings for easier logic
        category_scope = '{}-{}'.format(self.category, self.scope or '')
        if self.subawards:
            self.queryset, self.model = subaward_filter(self.filters), None
            self.obligation_column = 'amount'
        else:
            self.queryset, self.model = sbc_view_queryset(category_scope, self.filters)
            self.obligation_column = 'generated_pragmatic_obligation'

    def raise_not_implemented(self):
        msg = "Scope '{}' is not implemented for '{}' Category"
        if self.subawards:
            msg += ' when subawards is True'
        raise InvalidParameterException(msg.format(self.scope, self.category))

    def results(self) -> dict:
        # filter the transactions by category
        if self.category == 'awarding_agency':
            results = self.awarding_agency()
        elif self.category == 'funding_agency':
            results = self.funding_agency()
        elif self.category == 'recipient':
            results = self.recipient()
        elif self.category == 'cfda_programs':
            results = self.cfda_programs()
        elif self.category == 'industry_codes':
            results = self.industry_codes()
        else:
            raise InvalidParameterException("Category '{}' is not yet implemented".format(self.category))

        page_metadata = get_simple_pagination_metadata(len(results), self.limit, self.page)

        response = {
            'category': self.category,
            'limit': self.limit,
            'page_metadata': page_metadata,
            'results': alias_response(ALIAS_DICT[self.category], results[:self.limit]),
            'scope': self.scope,
        }
        return response

    def awarding_agency(self) -> list:
        if self.scope == 'agency':
            self.queryset = self.queryset \
                .filter(awarding_toptier_agency_name__isnull=False) \
                .values('awarding_toptier_agency_name', 'awarding_toptier_agency_abbreviation') \
                .annotate(aggregated_amount=Sum(self.obligation_column)) \
                .order_by('-aggregated_amount')

        elif self.scope == 'subagency':
            self.queryset = self.queryset \
                .filter(
                    awarding_subtier_agency_name__isnull=False) \
                .values('awarding_subtier_agency_name', 'awarding_subtier_agency_abbreviation') \
                .annotate(aggregated_amount=Sum(self.obligation_column)) \
                .order_by('-aggregated_amount')

        else:
            self.raise_not_implemented()

        # DB hit here
        return list(self.queryset[self.lower_limit:self.upper_limit])

    def funding_agency(self) -> list:
        if self.scope == 'agency':
            self.queryset = self.queryset \
                .filter(funding_toptier_agency_name__isnull=False) \
                .values('funding_toptier_agency_name', 'funding_toptier_agency_abbreviation') \
                .annotate(aggregated_amount=Sum(self.obligation_column)) \
                .order_by('-aggregated_amount')

        elif self.scope == 'subagency':
            self.queryset = self.queryset \
                .filter(
                    funding_subtier_agency_name__isnull=False) \
                .values('funding_subtier_agency_name', 'funding_subtier_agency_abbreviation') \
                .annotate(aggregated_amount=Sum(self.obligation_column)) \
                .order_by('-aggregated_amount')

        else:
            self.raise_not_implemented()

        # DB hit here
        return list(self.queryset[self.lower_limit:self.upper_limit])

    def recipient(self) -> list:
        if self.scope == 'duns':
            self.queryset = self.queryset \
                .filter(recipient_unique_id__isnull=False) \
                .values('recipient_name', 'recipient_unique_id') \
                .annotate(aggregated_amount=Sum(self.obligation_column)) \
                .order_by('-aggregated_amount')

        elif self.scope == 'parent_duns':
            # TODO: check if we can aggregate on recipient name and parent duns,
            #    since parent recipient name isn't available
            self.queryset = self.queryset \
                .filter(parent_recipient_unique_id__isnull=False) \
                .values('recipient_name', 'parent_recipient_unique_id') \
                .annotate(aggregated_amount=Sum(self.obligation_column)) \
                .order_by('-aggregated_amount')

        else:
            self.raise_not_implemented()

        # DB hit here
        return list(self.queryset[self.lower_limit:self.upper_limit])

    def cfda_programs(self) -> list:
        if self.model == 'SummaryCfdaNumbersView':
            self.queryset = self.queryset \
                .filter(
                    federal_action_obligation__isnull=False,
                    cfda_number__isnull=False) \
                .values('cfda_number', 'cfda_title') \
                .annotate(aggregated_amount=Sum(self.obligation_column)) \
                .order_by('-aggregated_amount')

            # DB hit here
            results = list(self.queryset[self.lower_limit:self.upper_limit])

            for trans in results:
                trans['popular_name'] = None
                # small DB hit every loop
                cfda = Cfda.objects \
                    .filter(
                        program_title=trans['cfda_title'],
                        program_number=trans['cfda_number']) \
                    .values('popular_name').first()

                if cfda:
                    trans['popular_name'] = cfda['popular_name']

        else:
            filters = {'{}__isnull'.format(self.obligation_column): False, 'cfda_number__isnull': False}
            self.queryset = self.queryset \
                .filter(**filters) \
                .values('cfda_number', 'cfda_popular_name', 'cfda_title') \
                .annotate(aggregated_amount=Sum(self.obligation_column)) \
                .order_by('-aggregated_amount')

            # DB hit here
            results = list(self.queryset[self.lower_limit:self.upper_limit])

        return results

    def industry_codes(self) -> list:
        if self.scope == 'psc':
            self.queryset = self.queryset \
                .filter(product_or_service_code__isnull=False) \
                .values('product_or_service_code') \
                .annotate(aggregated_amount=Sum(self.obligation_column)) \
                .order_by('-aggregated_amount')
        elif self.scope == 'naics':
            # TODO: Not filterable on subawards directly, need business decisions around supporting these for subawards
            if self.model is None:  # self.model is None if self.subawards = True
                self.raise_not_implemented()
            self.queryset = self.queryset \
                .filter(naics_code__isnull=False) \
                .values('naics_code', 'naics_description') \
                .annotate(aggregated_amount=Sum(self.obligation_column)) \
                .order_by('-aggregated_amount')
        else:
            self.raise_not_implemented()
        # DB hit here
        return list(self.queryset[self.lower_limit:self.upper_limit])
