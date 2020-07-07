from django.db.models import Count
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.count_base import CountBase
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.disaster.v2.views.disaster_base import FabaOutlayMixin, AwardTypeMixin


class AwardCountViewSet(CountBase, FabaOutlayMixin, AwardTypeMixin):
    """
    Obtain the count of Agencies related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/award/count.md"

    required_filters = ["def_codes", "award_type_codes"]

    @cache_response()
    def post(self, request: Request) -> Response:

        filters = [
            self.is_in_provided_def_codes(),
            self.all_closed_defc_submissions,
            self.is_non_zero_award_cpe(),
        ]
        if self.award_type_codes:
            count = (
                FinancialAccountsByAwards.objects.filter(*filters)
                .values("award_id")
                .filter(award__type__in=self.filters.get("award_type_codes"))
                .count()
            )
        else:
            count = FinancialAccountsByAwards.objects.filter(*filters).aggregate(
                count=Count(self.unique_file_c, distinct=True)
            )["count"]

        return Response({"count": count})
