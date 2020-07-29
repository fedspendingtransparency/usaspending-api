from django.db.models import Sum, Count
from django.db.models.functions import Coalesce
from rest_framework.response import Response

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    AwardTypeMixin,
    FabaOutlayMixin,
    when_non_zero_award_spending,
)


class AmountViewSet(AwardTypeMixin, FabaOutlayMixin, DisasterBase):
    """Returns aggregated values of obligation, outlay, and count of Award records"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/award/amount.md"

    @cache_response()
    def post(self, request):
        return Response(self.queryset)

    @property
    def queryset(self):
        filters = [
            self.all_closed_defc_submissions,
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
            return when_non_zero_award_spending(
                FinancialAccountsByAwards.objects.filter(*filters).values(count_field)
            ).aggregate(**fields)
        else:
            return when_non_zero_award_spending(
                FinancialAccountsByAwards.objects.filter(*filters).annotate(unique_c=count_field)
            ).aggregate(**fields)
