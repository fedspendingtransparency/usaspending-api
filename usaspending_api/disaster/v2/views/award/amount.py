from django.db.models import Count, F, Sum
from rest_framework.response import Response
from usaspending_api.awards.models import FinancialAccountsByAwards
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
            return Response({"count": self.queryset["award_count"]})
        else:
            return Response(self.queryset)

    @property
    def queryset(self):
        filters = [
            self.all_closed_defc_submissions,
            self.has_award_of_classification,
            self.has_award_of_provided_type,
            self.is_in_provided_def_codes,
        ]

        group_by_annotations = {
            "award_identifier": F("award_id") if self.award_type_codes else self.unique_file_c_awards
        }

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
                award_count=Count("award_identifier"), obligation=Sum("inner_obligation"), outlay=Sum("inner_outlay")
            )
        )
