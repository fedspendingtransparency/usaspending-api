from django.db.models import OuterRef, Q, Exists
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.count_base import CountBase
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.awards.models import Award


class AwardCountViewSet(CountBase):
    """
    Obtain the count of Agencies related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/award/count.md"

    @cache_response()
    def post(self, request: Request) -> Response:
        filters = [
            Q(award=OuterRef("pk")),
            self.is_in_provided_def_codes(),
            self.is_after_min_date(),
            self.is_last_closed_submission_window(),
            self.is_non_zero_award_cpe(),
        ]
        count = (
            Award.objects.annotate(include=Exists(FinancialAccountsByAwards.objects.filter(*filters).values("pk")))
            .filter(include=True)
            .values("pk")
        )

        return Response({"count": count.count(), "debug": count.first()})
