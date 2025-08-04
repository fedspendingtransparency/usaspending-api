from django.db.models import Sum
from rest_framework.response import Response

from usaspending_api.awards.v2.lookups.lookups import (
    assistance_type_mapping,
    loan_type_mapping,
    procurement_type_mapping,
)
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.validator import TinyShield
from usaspending_api.disaster.models import CovidFABASpending
from usaspending_api.disaster.v2.views.disaster_base import AwardTypeMixin, DisasterBase, FabaOutlayMixin


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

        award_type = self.filters.get("award_type")

        queryset = (
            CovidFABASpending.objects.filter(spending_level="awards")
            .filter(defc__in=self.filters["def_codes"])
            .annotate(
                total_award_count=Sum("award_count"),
                total_obligation_sum=Sum("obligation_sum"),
                total_outlay_sum=Sum("outlay_sum"),
                total_face_value_of_loan=Sum("face_value_of_loan"),
            )
        )

        if self.award_type_codes is not None:
            queryset = queryset.filter(award_type__in=self.award_type_codes)

        if award_type is not None and award_type.lower() == "procurement":
            queryset = queryset.filter(award_type__in=procurement_type_mapping.keys())
        elif award_type is not None and award_type.lower() == "assistance":
            queryset = queryset.filter(award_type__in=assistance_type_mapping.keys())

        result = {
            "award_count": sum([row.total_award_count for row in queryset]),
            "obligation": sum([row.total_obligation_sum for row in queryset]),
            "outlay": sum([row.total_outlay_sum for row in queryset]),
        }

        # Add face_value_of_loan if any loan award types were included in the request
        if self.award_type_codes is not None and any(
            award_type in loan_type_mapping.keys() for award_type in self.award_type_codes
        ):
            result["face_value_of_loan"] = sum(
                [row.total_face_value_of_loan for row in queryset if row.total_face_value_of_loan is not None]
            )

        if self.count_only:
            return Response({"count": result["award_count"]})
        else:
            return Response(result)
