from django.contrib.postgres.fields import ArrayField
from django.db.models import Count, Sum, TextField, F
from django.db.models.functions import Coalesce, Cast
from rest_framework.response import Response
from usaspending_api.awards.models import FinancialAccountsByAwards, CovidFinancialAccountMatview
from usaspending_api.awards.v2.lookups.lookups import loan_type_mapping
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.validator import TinyShield
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, AwardTypeMixin, FabaOutlayMixin


class AmountViewSet(AwardTypeMixin, FabaOutlayMixin, DisasterBase):
    """Returns aggregated values of obligation, outlay, and count of Award records"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/award/amount.md"
    count_only = False

    @cache_response()
    def post(self, request):
        additional_models = [
            {
                "key": "filter|award_type",
                "name": "award_type",
                "type": "enum",
                "enum_values": ("assistance", "procurement"),
                "allow_nulls": False,
                "optional": True,
            }
        ]

        f = TinyShield(additional_models).block(self.request.data).get("filter")
        if f:
            self.filters["award_type"] = f.get("award_type")

        if all(x in self.filters for x in ["award_type_codes", "award_type"]):
            raise UnprocessableEntityException("Cannot provide both 'award_type_codes' and 'award_type'")

        if self.count_only:
            return Response({"count": self.aggregation["award_count"]})
        else:
            return Response(self.aggregation)

    @property
    def aggregation(self):
        return self._file_d_aggregation() if self.award_type_codes else self._file_c_aggregation()

    def _file_d_aggregation(self):
        aggregations = {
            "award_count": Count("award_id"),
            "obligation": Coalesce(Sum("obligation"), 0),
            "outlay": Coalesce(Sum("outlay"), 0),
        }

        if set(self.award_type_codes) <= set(loan_type_mapping.keys()):
            aggregations["face_value_of_loan"] = Coalesce(Sum("total_loan_value"), 0)

        return (
            CovidFinancialAccountMatview.objects.annotate(cast_def_codes=Cast("def_codes", ArrayField(TextField())))
            .filter(type__in=self.award_type_codes, cast_def_codes__overlap=self.def_codes)
            .values("award_id")
            .aggregate(**aggregations)
        )

    def _file_c_aggregation(self):
        filters = [
            self.all_closed_defc_submissions,
            self.has_award_of_classification,
            self.is_in_provided_def_codes,
        ]

        group_by_annotations = {"award_identifier": F("distinct_award_key")}

        dollar_annotations = {
            "inner_obligation": self.obligated_field_annotation,
            "inner_outlay": self.outlay_field_annotation,
        }

        all_annotations = {**group_by_annotations, **dollar_annotations}

        return (
            FinancialAccountsByAwards.objects.filter(*filters)
            .annotate(**group_by_annotations)
            .values(*group_by_annotations)
            .annotate(**dollar_annotations)
            .exclude(inner_obligation=0, inner_outlay=0)
            .values(*all_annotations)
            .aggregate(
                award_count=Count("award_identifier"),
                obligation=Coalesce(Sum("inner_obligation"), 0),
                outlay=Coalesce(Sum("inner_outlay"), 0),
            )
        )
