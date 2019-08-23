import copy

from django.conf import settings
from django.db.models import Count
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_connectors.spending_by_award_count_asyncpg import fetch_all_category_counts
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.orm_helpers import category_to_award_materialized_views
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByAwardCountVisualizationViewSet(APIView):
    """This route takes award filters, and returns the number of awards in each award type.

    (Contracts, Loans, Grants, etc.)
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_award_count.md"

    @cache_response()
    def post(self, request):
        models = [{"name": "subawards", "key": "subawards", "type": "boolean", "default": False}]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        json_request = TinyShield(models).block(request.data)
        filters = json_request.get("filters", None)
        subawards = json_request["subawards"]

        if filters is None:
            raise InvalidParameterException("Missing required request parameters: 'filters'")

        results = {"contracts": 0, "idvs": 0, "grants": 0, "direct_payments": 0, "loans": 0, "other": 0}
        if subawards:
            results = {"subcontracts": 0, "subgrants": 0}

        if "award_type_codes" in filters and "no intersection" in filters["award_type_codes"]:
            # "Special case": there will never be results when the website provides this value
            return Response({"results": results})

        if not subawards:
            results = fetch_all_category_counts(filters, category_to_award_materialized_views())
        else:
            # Explicitly check the Sub-Award Records to be linked to a Prime Award to include in the counts.
            queryset = (
                subaward_filter(filters)
                .filter(award_id__isnull=False)
                .values("award_type")
                .annotate(count=Count("subaward_id"))
            )

            results["subgrants"] = sum([sub["count"] for sub in queryset if sub["award_type"] == "grant"])
            results["subcontracts"] = sum([sub["count"] for sub in queryset if sub["award_type"] == "procurement"])

        return Response({"results": results})
