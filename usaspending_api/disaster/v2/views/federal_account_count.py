from datetime import datetime

from django.db.models import OuterRef, Q, Exists
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.accounts.models import FederalAccount
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


class FederalAccountCountViewSet(DisasterBase):
    """
    Obtain the count of Federal Accounts related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/count.md"

    @cache_response()
    def post(self, request: Request) -> Response:
        filters = [
            Q(final_of_fy=True),
            Q(treasury_account__federal_account_id=OuterRef("pk")),
            Q(disaster_emergency_fund__code__in=self.def_codes),
            Q(submission__reporting_period_start__gte="2020-04-01"),
            Q(submission__reporting_period_end__lt=datetime.now().date()),
        ]
        count = (
            FederalAccount.objects.annotate(
                include=Exists(FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters).values("pk"))
            )
            .filter(include=True)
            .values("pk")
            .count()
        )
        return Response({"count": count})
