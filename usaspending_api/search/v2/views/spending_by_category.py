import copy
import logging

from django.conf import settings
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import NotImplementedException
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
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_federal_account import FederalAccountViewSet
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_locations import (
    CountyViewSet,
    CountryViewSet,
    DistrictViewSet,
    StateTerritoryViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_recipient_duns import RecipientDunsViewSet

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION


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

        # Execute the business logic for the endpoint and return a python dict to be converted to a Django response
        business_logic_lookup = {
            "awarding_agency": AwardingAgencyViewSet().perform_search,
            "awarding_subagency": AwardingSubagencyViewSet().perform_search,
            "cfda": CfdaViewSet().perform_search,
            "country": CountryViewSet().perform_search,
            "county": CountyViewSet().perform_search,
            "district": DistrictViewSet().perform_search,
            "federal_account": FederalAccountViewSet().perform_search,
            "funding_agency": FundingAgencyViewSet().perform_search,
            "funding_subagency": FundingSubagencyViewSet().perform_search,
            "naics": NAICSViewSet().perform_search,
            "psc": PSCViewSet().perform_search,
            "recipient_duns": RecipientDunsViewSet().perform_search,
            "recipient": RecipientDunsViewSet().perform_search,
            "state_territory": StateTerritoryViewSet().perform_search,
        }
        business_logic_func = business_logic_lookup.get(validated_payload["category"])
        if business_logic_func:
            return Response(business_logic_func(validated_payload, original_filters))
        else:
            self.raise_not_implemented(validated_payload)

    @staticmethod
    def raise_not_implemented(payload):
        msg = f"Category '{payload['category']}' is not implemented"
        raise NotImplementedException(msg)
