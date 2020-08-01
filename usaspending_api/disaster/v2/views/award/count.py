from django.db.models import Count
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.disaster.v2.views.disaster_base import FabaOutlayMixin, AwardTypeMixin


class AwardCountViewSet(DisasterBase, FabaOutlayMixin, AwardTypeMixin):
    """
    Obtain the count of Awards related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/award/count.md"

    required_filters = ["def_codes", "award_type_codes"]

    @cache_response()
    def post(self, request: Request) -> Response:

        filters = [
            self.is_in_provided_def_codes,
            self.all_closed_defc_submissions,
        ]
        count = FinancialAccountsByAwards.objects.filter(*filters)
        if self.award_type_codes:
            count = count.values("award_id").filter(award__type__in=self.filters.get("award_type_codes"))
        count = self.when_non_zero_award_spending(count.annotate(unique_c=self.unique_file_c)).aggregate(
            count=Count("unique_c", distinct=True)
        )["count"]

        return Response({"count": count})
