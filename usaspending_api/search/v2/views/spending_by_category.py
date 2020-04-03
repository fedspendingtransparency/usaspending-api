import copy
import logging

from django.conf import settings
from django.db.models import Sum
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_by_category_view_queryset
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException, NotImplementedException
from usaspending_api.common.experimental_api_flags import (
    is_experimental_elasticsearch_api,
    mirror_request_to_elasticsearch,
)
from usaspending_api.common.helpers.api_helper import alias_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata, get_generic_filters_message
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_agency_types import (
    AwardingAgencyViewSet,
    AwardingSubagencyViewSet,
    FundingAgencyViewSet,
    FundingSubagencyViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_industry_codes import (
    CfdaViewSet,
    PSCViewSet,
    NAICSViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_locations import (
    CountyViewSet,
    CountryViewSet,
    DistrictViewSet,
    StateTerritoryViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_recipient_duns import RecipientDunsViewSet

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION

ALIAS_DICT = {
    "recipient_parent_duns": {
        "recipient_id": "recipient_id",
        "recipient_name": "name",
        "recipient_unique_id": "code",
        "parent_recipient_unique_id": "code",
    },
    "federal_account": {"federal_account_id": "id", "federal_account_display": "code", "account_title": "name"},
}


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByCategoryVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by the defined category/scope.
    The category is defined by the category keyword, and the scope is defined by is denoted by the scope keyword.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category.md"

    @cache_response()
    def post(self, request: Request) -> Response:
        """Return all budget function/subfunction titles matching the provided search text"""
        categories = [
            "awarding_agency",
            "awarding_subagency",
            "funding_agency",
            "funding_subagency",
            "recipient_duns",
            "recipient_parent_duns",
            "cfda",
            "psc",
            "naics",
            "county",
            "district",
            "country",
            "state_territory",
            "federal_account",
        ]
        models = [
            {"name": "category", "key": "category", "type": "enum", "enum_values": categories, "optional": False},
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False, "optional": True},
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))

        # Apply/enforce POST body schema and data validation in request
        original_filters = request.data.get("filters")
        validated_payload = TinyShield(models).block(request.data)

        validated_payload["elasticsearch"] = is_experimental_elasticsearch_api(request)
        if not validated_payload["elasticsearch"]:
            mirror_request_to_elasticsearch(request)

        # Execute the business logic for the endpoint and return a python dict to be converted to a Django response
        business_logic_lookup = {
            "awarding_agency": AwardingAgencyViewSet().perform_search,
            "awarding_subagency": AwardingSubagencyViewSet().perform_search,
            "cfda": CfdaViewSet().perform_search,
            "country": CountryViewSet().perform_search,
            "county": CountyViewSet().perform_search,
            "district": DistrictViewSet().perform_search,
            "funding_agency": FundingAgencyViewSet().perform_search,
            "funding_subagency": FundingSubagencyViewSet().perform_search,
            "naics": NAICSViewSet().perform_search,
            "psc": PSCViewSet().perform_search,
            "recipient_duns": RecipientDunsViewSet().perform_search,
            "state_territory": StateTerritoryViewSet().perform_search,
        }
        business_logic_func = business_logic_lookup.get(validated_payload["category"])
        if business_logic_func:
            response = business_logic_func(validated_payload, original_filters)
        else:
            response = BusinessLogic(validated_payload, original_filters).results()

        return Response(response)


class BusinessLogic:
    # __slots__ will keep this object smaller
    __slots__ = (
        "subawards",
        "category",
        "page",
        "limit",
        "obligation_column",
        "lower_limit",
        "upper_limit",
        "filters",
        "queryset",
        "original_filters",
    )

    def __init__(self, payload: dict, original_filters):
        """
            payload is tightly integrated with
        """
        self.original_filters = original_filters
        self.subawards = payload["subawards"]
        self.category = payload["category"]
        self.page = payload["page"]
        self.limit = payload["limit"]
        self.filters = payload.get("filters", {})

        self.lower_limit = (self.page - 1) * self.limit
        self.upper_limit = self.page * self.limit + 1  # Add 1 for simple "Next Page" check

        if self.subawards:
            self.queryset = subaward_filter(self.filters)
            self.obligation_column = "amount"
        else:
            self.queryset = spending_by_category_view_queryset(self.category, self.filters)
            self.obligation_column = "generated_pragmatic_obligation"

    def raise_not_implemented(self):
        msg = "Category '{}' is not implemented"
        if self.subawards:
            msg += " when `subawards` is True"
        raise NotImplementedException(msg.format(self.category))

    def common_db_query(self, filters, values):
        return (
            self.queryset.filter(**filters)
            .values(*values)
            .annotate(amount=Sum(self.obligation_column))
            .order_by("-amount")
        )

    def results(self) -> dict:
        results = []
        # filter the transactions by category
        if self.category in ("recipient_parent_duns",):
            results = self.parent_recipient()
        elif self.category in ("federal_account"):
            results = self.federal_account()

        page_metadata = get_simple_pagination_metadata(len(results), self.limit, self.page)

        response = {
            "category": self.category,
            "limit": self.limit,
            "page_metadata": page_metadata,
            # alias_response is a workaround for tests instead of applying any aliases in the querysets
            "results": results[: self.limit],
            "messages": get_generic_filters_message(
                self.original_filters.keys(), [elem["name"] for elem in AWARD_FILTER]
            ),
        }
        return response

    def parent_recipient(self) -> list:
        # TODO: check if we can aggregate on recipient name and parent duns,
        #    since parent recipient name isn't available
        # Not implemented until "Parent Recipient Name" is in matviews
        self.raise_not_implemented()
        # filters = {'parent_recipient_unique_id__isnull': False}
        # values = ['recipient_name', 'parent_recipient_unique_id']

    def federal_account(self) -> list:
        # Awards -> FinancialAccountsByAwards -> TreasuryAppropriationAccount -> FederalAccount
        filters = {"federal_account_id__isnull": False}
        values = ["federal_account_id", "federal_account_display", "account_title"]

        if self.subawards:
            # N/A for subawards
            self.raise_not_implemented()

        # Note: For performance reasons, limiting to only recipient profile requests
        if "recipient_id" not in self.filters:
            raise InvalidParameterException("Federal Account category requires recipient_id in search filter")

        self.queryset = self.common_db_query(filters, values)

        # DB hit here
        query_results = list(self.queryset[self.lower_limit : self.upper_limit])
        return alias_response(ALIAS_DICT[self.category], query_results)
