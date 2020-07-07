from django.db.models import F
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.count_base import CountBase
from usaspending_api.awards.models import FinancialAccountsByAwards, TransactionFABS
from usaspending_api.disaster.v2.views.disaster_base import FabaOutlayMixin, AwardTypeMixin

from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query


class RecipientCountViewSet(CountBase, FabaOutlayMixin, AwardTypeMixin):
    """
    Obtain the count of Recipients related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/recipient/count.md"

    required_filters = ["def_codes", "award_type_codes"]

    @cache_response()
    def post(self, request: Request) -> Response:

        filters = [
            self.is_in_provided_def_codes(),
            self.all_closed_defc_submissions,
            self.is_non_zero_award_cpe(),
        ]
        award_ids = FinancialAccountsByAwards.objects.filter(*filters).values("award")
        count = TransactionFABS.objects.annotate(award=F("transaction__award__id")).filter(award__in=award_ids)
        if self.award_type_codes:
            count = count.filter(transaction__award__type__in=self.filters.get("award_type_codes"))

        print(generate_raw_quoted_query(count))
        return Response({"count": count.count()})
