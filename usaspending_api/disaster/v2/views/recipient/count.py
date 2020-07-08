from django.db.models import Q, Count
from django.db.models.functions import Coalesce
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.count_base import CountBase
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.disaster.v2.views.disaster_base import FabaOutlayMixin, AwardTypeMixin


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
            self.is_non_zero_award_cpe(),
            self.has_award_of_provided_type(),
        ]
        award_ids = (
            FinancialAccountsByAwards.objects.annotate(
                recipient=Coalesce(
                    "award__latest_transaction__contract_data__awardee_or_recipient_uniqu",
                    "award__latest_transaction__assistance_data__awardee_or_recipient_uniqu",
                )
            )
            .filter(*filters)
            .aggregate(count=Count("recipient", distinct=True))
        )

        return Response({"count": award_ids["count"]})
