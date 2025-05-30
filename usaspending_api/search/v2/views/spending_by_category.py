import logging

from django.conf import settings
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import NotImplementedException
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
    DEFCViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_federal_account import FederalAccountViewSet
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_locations import (
    CountyViewSet,
    CountryViewSet,
    DistrictViewSet,
    StateTerritoryViewSet,
)
from usaspending_api.search.v2.views.spending_by_category_views.spending_by_recipient import (
    RecipientViewSet,
    RecipientDunsViewSet,
)

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
            "recipient",
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
            "defc",
        ]
        models = [{"name": "category", "key": "category", "type": "enum", "enum_values": categories, "optional": False}]

        # Apply/enforce POST body schema and data validation in request
        validated_payload = TinyShield(models).block(request.data)

        view_set_lookup = {
            "awarding_agency": AwardingAgencyViewSet,
            "awarding_subagency": AwardingSubagencyViewSet,
            "cfda": CfdaViewSet,
            "country": CountryViewSet,
            "county": CountyViewSet,
            "district": DistrictViewSet,
            "federal_account": FederalAccountViewSet,
            "funding_agency": FundingAgencyViewSet,
            "funding_subagency": FundingSubagencyViewSet,
            "naics": NAICSViewSet,
            "psc": PSCViewSet,
            "recipient": RecipientViewSet,
            "recipient_duns": RecipientDunsViewSet,
            "state_territory": StateTerritoryViewSet,
            "defc": DEFCViewSet,
        }
        view_set = view_set_lookup.get(validated_payload["category"])
        if not view_set:
            self.raise_not_implemented(validated_payload)

        return view_set.as_view()(request._request)

    @staticmethod
    def raise_not_implemented(payload):
        msg = f"Category '{payload['category']}' is not implemented"
        raise NotImplementedException(msg)
