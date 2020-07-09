from django.db.models import OuterRef, Q, Exists
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.count_base import CountBase
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import ToptierAgency


class AgencyCountViewSet(CountBase):
    """
    Obtain the count of Agencies related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/count.md"

    @cache_response()
    def post(self, request: Request) -> Response:
        filters = [
            Q(treasury_account__funding_toptier_agency=OuterRef("pk")),
            self.is_in_provided_def_codes(),
            self.all_closed_defc_submissions,
            self.is_non_zero_object_class_cpe(),
        ]
        count = (
            ToptierAgency.objects.annotate(
                include=Exists(FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters).values("pk"))
            )
            .filter(include=True)
            .values("pk")
            .count()
        )
        return Response({"count": count})
