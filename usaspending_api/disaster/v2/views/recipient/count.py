from django.db.models import Count, Case, When, F, TextField, OuterRef, Subquery, Sum, DecimalField, Q
from django.db.models.functions import Cast, Coalesce
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.disaster.v2.views.disaster_base import (
    FabaOutlayMixin,
    AwardTypeMixin,
)
from usaspending_api.recipient.v2.lookups import SPECIAL_CASES
from usaspending_api.search.models import AwardSearchView


class RecipientCountViewSet(DisasterBase, FabaOutlayMixin, AwardTypeMixin):
    """
    Obtain the count of Recipients related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/recipient/count.md"

    required_filters = ["def_codes", "award_type_codes"]

    @cache_response()
    def post(self, request: Request) -> Response:

        filters = [
            self.is_in_provided_def_codes,
            self.all_closed_defc_submissions,
        ]

        related_faba = (
            FinancialAccountsByAwards.objects.filter(*filters)
            .filter(award_id=OuterRef("award_id"))
            .order_by()
            .values("award_id")
        )
        total_toa = related_faba.annotate(
            total_toa=Coalesce(Sum("transaction_obligated_amount", output_field=DecimalField()), 0.0)
        ).values("total_toa")
        total_outlay = related_faba.annotate(
            total_outlay=Coalesce(Sum("gross_outlay_amount_by_award_cpe", output_field=DecimalField()), 0.0)
        ).values("total_outlay")

        recipients = (
            AwardSearchView.objects.annotate(
                recipient=Case(
                    When(recipient_name__in=SPECIAL_CASES, then=F("recipient_name"),),
                    default=Cast(F("recipient_hash"), output_field=TextField()),
                    output_field=TextField(),
                )
            )
            .filter(self.has_award_of_provided_type)
            .values("recipient")
            .annotate(total_toa=Subquery(total_toa, output_field=DecimalField()))
            .annotate(total_outlay=Subquery(total_outlay, output_field=DecimalField()))
            .filter(~Q(total_toa=0.0) | ~Q(total_outlay=0.0))
            .extra(
                where=[
                    f"Exists({generate_raw_quoted_query(FinancialAccountsByAwards.objects.filter(*filters))}"
                    f" AND financial_accounts_by_awards.award_id = {AwardSearchView._meta.db_table}.award_id)"
                ]
            )
            .aggregate(count=Count("recipient", distinct=True))
        )

        return Response({"count": recipients["count"]})
