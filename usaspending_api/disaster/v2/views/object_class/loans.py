from django.db.models import Q, Sum, F, Value, Case, When, Count
from django.db.models.functions import Coalesce
from rest_framework.response import Response

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    LoansPaginationMixin,
    LoansMixin,
    FabaOutlayMixin,
)
from usaspending_api.disaster.v2.views.object_class.spending import shared_object_class_annotations, construct_response


class ObjectClassLoansViewSet(LoansMixin, LoansPaginationMixin, FabaOutlayMixin, DisasterBase):
    """Provides insights on the Object Classes' loans from disaster/emergency funding per the requested filters"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/object_class/loans.md"

    @cache_response()
    def post(self, request):
        # rename hack to use the Dataclasses, setting to Dataclass attribute name
        if self.pagination.sort_key == "face_value_of_loan":
            self.pagination.sort_key = "total_budgetary_resources"

        results = construct_response(list(self.queryset), self.pagination, False)

        # rename hack to use the Dataclasses, swapping back in desired loan field name
        for result in results["results"]:
            for child in result["children"]:
                child["face_value_of_loan"] = child.pop("total_budgetary_resources")
            result["face_value_of_loan"] = result.pop("total_budgetary_resources")

        return Response(results)

    @property
    def queryset(self):
        filters = [
            Q(award_id__isnull=False),
            Q(object_class__isnull=False),
            self.all_closed_defc_submissions,
            self.is_in_provided_def_codes,
            self.is_loan_award,
        ]

        annotations = {
            **shared_object_class_annotations(),
            "obligation": Coalesce(Sum("transaction_obligated_amount"), 0),
            "outlay": Coalesce(
                Sum(
                    Case(
                        When(self.final_period_submission_query_filters, then=F("gross_outlay_amount_by_award_cpe")),
                        default=Value(0),
                    )
                ),
                0,
            ),
            "total_budgetary_resources": Coalesce(Sum("award__total_loan_value"), 0),
            "award_count": Count("award_id", distinct=True),
        }

        # Assuming it is more performant to fetch all rows once rather than
        #  run a count query and fetch only a page's worth of results
        return (
            FinancialAccountsByAwards.objects.filter(*filters)
            .values("object_class__major_object_class", "object_class__major_object_class_name")
            .annotate(**annotations)
            .values(*annotations.keys())
        )
