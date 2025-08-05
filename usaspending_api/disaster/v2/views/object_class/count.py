from django.db.models import OuterRef, Q, Exists
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.calculations.file_b import FileBCalculations
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import ObjectClass


class ObjectClassCountViewSet(DisasterBase):
    """
    Obtain the count of Object Class related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/object_class/count.md"

    @cache_response()
    def post(self, request: Request) -> Response:
        file_b_calculations = FileBCalculations(is_covid_page=True)
        filters = [
            Q(object_class_id=OuterRef("pk")),
            self.all_closed_defc_submissions,
            self.is_in_provided_def_codes,
            file_b_calculations.is_non_zero_total_spending(),
        ]
        count = (
            ObjectClass.objects.filter(Exists(FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)))
            .values("object_class")
            .distinct()
            .count()
        )

        return Response({"count": count})
