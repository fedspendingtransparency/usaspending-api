from django.db.models import Count, Case, When, F, TextField
from django.db.models.functions import Cast
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

        recipients = (
            AwardSearchView.objects.annotate(
                recipient=Case(
                    When(recipient_name__in=SPECIAL_CASES, then=F("recipient_name"),),
                    default=Cast(F("recipient_hash"), output_field=TextField()),
                    output_field=TextField(),
                )
            )
            .filter(self.has_award_of_provided_type)
            .extra(
                where=[
                    f"Exists({generate_raw_quoted_query(FinancialAccountsByAwards.objects.filter(*filters))}"
                    f" AND financial_accounts_by_awards.award_id = {AwardSearchView._meta.db_table}.award_id)"
                ]
            )
            .aggregate(count=Count("recipient", distinct=True))
        )

        return Response({"count": recipients["count"]})
