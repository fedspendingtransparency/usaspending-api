from django.contrib.postgres.fields import ArrayField
from django.db.models import Case, When, F, TextField, Q
from django.db.models.functions import Cast
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.awards.models.mv_covid_financial_account import CovidFinancialAccountMatview
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase

from usaspending_api.disaster.v2.views.disaster_base import FabaOutlayMixin, AwardTypeMixin
from usaspending_api.recipient.v2.lookups import SPECIAL_CASES


class RecipientCountViewSet(FabaOutlayMixin, AwardTypeMixin, DisasterBase):
    """
    Obtain the count of Recipients related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/recipient/count.md"

    required_filters = ["def_codes", "award_type_codes"]

    @cache_response()
    def post(self, request: Request) -> Response:
        annotations = {
            "cast_def_codes": Cast("def_codes", ArrayField(TextField())),
            "recipient_identifier": Case(
                When(recipient_name__in=SPECIAL_CASES, then=F("recipient_name")),
                default=Cast(F("recipient_hash"), output_field=TextField()),
            ),
        }
        filters = [Q(cast_def_codes__overlap=self.def_codes), self.has_award_of_provided_type(should_join_awards=False)]

        recipients = (
            CovidFinancialAccountMatview.objects.annotate(**annotations)
            .filter(*filters)
            .values("recipient_identifier")
            .distinct()
        )

        return Response({"count": recipients.count()})
