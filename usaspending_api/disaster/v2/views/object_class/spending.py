from django.db.models import Q, Sum, F, Value, Case, When, Min, TextField, IntegerField
from django.db.models.functions import Coalesce, Cast
from rest_framework.response import Response

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.object_class.object_class_result import (
    ObjectClassResults,
    MajorClass,
    ObjectClass,
)
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    PaginationMixin,
    SpendingMixin,
    FabaOutlayMixin,
)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


def construct_response(results: list, pagination: Pagination, strip_total_budgetary_resources=True):
    object_classes = ObjectClassResults()
    for row in results:
        major_code = row.pop("major_code")
        major_class = MajorClass(
            id=major_code, code=major_code, award_count=0, description=row.pop("major_description")
        )
        object_classes[major_class].include(ObjectClass(**row))

    return {
        "results": object_classes.finalize(pagination, strip_total_budgetary_resources),
        "page_metadata": get_pagination_metadata(len(object_classes), pagination.limit, pagination.page),
    }


class ObjectClassSpendingViewSet(PaginationMixin, SpendingMixin, FabaOutlayMixin, DisasterBase):
    """View to implement the API"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/object_class/spending.md"

    @cache_response()
    def post(self, request):
        if self.spending_type == "award":
            results = list(self.award_queryset)
        else:
            results = list(self.total_queryset)

        return Response(construct_response(results, self.pagination))

    @property
    def total_queryset(self):
        filters = [
            self.is_in_provided_def_codes,
            self.is_non_zero_total_spending,
            self.all_closed_defc_submissions,
            Q(object_class__isnull=False),
        ]

        annotations = {
            **shared_object_class_annotations(),
            "obligation": Coalesce(
                Sum(
                    Case(
                        When(
                            self.final_period_submission_query_filters,
                            then=F("obligations_incurred_by_program_object_class_cpe"),
                        ),
                        default=Value(0),
                    )
                ),
                0,
            ),
            "outlay": Coalesce(
                Sum(
                    Case(
                        When(
                            self.final_period_submission_query_filters,
                            then=F("gross_outlay_amount_by_program_object_class_cpe"),
                        ),
                        default=Value(0),
                    )
                ),
                0,
            ),
            "award_count": Value(None, output_field=IntegerField()),
        }

        # Assuming it is more performant to fetch all rows once rather than
        #  run a count query and fetch only a page's worth of results
        return (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .values("object_class__major_object_class", "object_class__major_object_class_name",)
            .annotate(**annotations)
            .values(*annotations.keys())
        )

    @property
    def award_queryset(self):
        filters = [
            self.is_in_provided_def_codes,
            Q(object_class__isnull=False),
            self.all_closed_defc_submissions,
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
            "award_count": self.unique_file_c_count(),
        }

        # Assuming it is more performant to fetch all rows once rather than
        #  run a count query and fetch only a page's worth of results
        return (
            FinancialAccountsByAwards.objects.filter(*filters)
            .values("object_class__major_object_class", "object_class__major_object_class_name")
            .annotate(**annotations)
            .values(*annotations.keys())
        )


def shared_object_class_annotations():
    return {
        "major_code": F("object_class__major_object_class"),
        "description": F("object_class__object_class_name"),
        "code": F("object_class__object_class"),
        "id": Cast(Min("object_class_id"), TextField()),
        "major_description": F("object_class__major_object_class_name"),
    }
