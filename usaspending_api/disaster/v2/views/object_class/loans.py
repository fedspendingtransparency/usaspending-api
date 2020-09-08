from django.db.models import F, Value, TextField, Min
from django.db.models.functions import Cast
from rest_framework.response import Response
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.orm_helpers import ConcatAll
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    LoansPaginationMixin,
    LoansMixin,
    FabaOutlayMixin,
)
from usaspending_api.disaster.v2.views.object_class.spending import construct_response
from usaspending_api.references.models import ObjectClass


class ObjectClassLoansViewSet(LoansMixin, LoansPaginationMixin, FabaOutlayMixin, DisasterBase):
    """Provides insights on the Object Classes' loans from disaster/emergency funding per the requested filters"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/object_class/loans.md"

    @cache_response()
    def post(self, request):
        # rename hack to use the Dataclasses, setting to Dataclass attribute name
        if self.pagination.sort_key == "face_value_of_loan":
            self.pagination.sort_key = "total_budgetary_resources"

        results = list(self.queryset)
        results = [{("id" if k == "id_" else k): v for k, v in r.items()} for r in results]
        results = construct_response(results, self.pagination, False)

        # rename hack to use the Dataclasses, swapping back in desired loan field name
        for result in results["results"]:
            for child in result["children"]:
                child["face_value_of_loan"] = child.pop("total_budgetary_resources")
            result["face_value_of_loan"] = result.pop("total_budgetary_resources")

        return Response(results)

    @property
    def queryset(self):
        query = self.construct_loan_queryset(
            ConcatAll("object_class__major_object_class", Value(":"), "object_class__object_class"),
            ObjectClass.objects.annotate(join_key=ConcatAll("major_object_class", Value(":"), "object_class")),
            "join_key",
        )

        annotations = {
            "major_code": F("major_object_class"),
            "description": Min("object_class_name"),
            "code": F("object_class"),
            "id_": Cast(Min("id"), output_field=TextField()),
            "major_description": Min("major_object_class_name"),
            "obligation": query.obligation_column,
            "outlay": query.outlay_column,
            "total_budgetary_resources": query.face_value_of_loan_column,
            "award_count": query.award_count_column,
        }

        return query.queryset.values("major_object_class", "object_class").annotate(**annotations).values(*annotations)
