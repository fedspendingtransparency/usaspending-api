from django.db.models import Q, Count, Case, When, F, TextField
from django.db.models.functions import Cast
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.count_base import CountBase
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.disaster.v2.views.disaster_base import FabaOutlayMixin, AwardTypeMixin
from usaspending_api.recipient.v2.lookups import SPECIAL_CASES


class RecipientCountViewSet(CountBase, FabaOutlayMixin, AwardTypeMixin):
    """
    Obtain the count of Recipients related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/recipient/count.md"

    required_filters = ["def_codes", "award_type_codes"]

    @cache_response()
    def post(self, request: Request) -> Response:

        filters = [
            Q(recipient__isnull=False),
            self.is_in_provided_def_codes(),
            self.all_closed_defc_submissions,
            self.is_non_zero_award_spending(),
            self.has_award_of_provided_type(),
        ]
        award_ids = (
            FinancialAccountsByAwards.objects.annotate(
                recipient=Case(
                    When(
                        award__awardsearchview__recipient_name__in=SPECIAL_CASES,
                        then=F("award__awardsearchview__recipient_name"),
                    ),
                    default=Cast(F("award__awardsearchview__recipient_hash"), output_field=TextField()),
                    output_field=TextField(),
                )
            )
            .filter(*filters)
            .aggregate(count=Count("recipient", distinct=True))
        )

        return Response({"count": award_ids["count"]})
