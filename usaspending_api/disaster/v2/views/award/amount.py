from django.db.models import Sum, Count
from django.db.models.functions import Coalesce
from rest_framework.response import Response

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.validator import TinyShield
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, AwardTypeMixin, FabaOutlayMixin


class AmountViewSet(AwardTypeMixin, FabaOutlayMixin, DisasterBase):
    """Returns aggregated values of obligation, outlay, and count of Award records"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/award/amount.md"

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
        return Response(self.queryset)

    @property
    def queryset(self):
        filters = [
            self.all_closed_defc_submissions,
            self.has_award_of_classification,
            self.has_award_of_provided_type,
            self.is_in_provided_def_codes,
        ]

        if self.award_type_codes:
            count_field = "award_id"
        else:
            count_field = self.unique_file_c

        fields = {
            "award_count": Count(count_field, distinct=True),
            "obligation": Coalesce(Sum("transaction_obligated_amount"), 0),
            "outlay": self.outlay_field_annotation,
        }

        if self.award_type_codes:
            return self.when_non_zero_award_spending(
                FinancialAccountsByAwards.objects.filter(*filters).values(count_field)
            ).aggregate(**fields)
        else:
            return self.when_non_zero_award_spending(
                FinancialAccountsByAwards.objects.filter(*filters).annotate(unique_c=count_field).values("unique_c")
            ).aggregate(**fields)
