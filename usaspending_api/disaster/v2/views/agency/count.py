from django.contrib.postgres.fields import ArrayField
from django.db.models import OuterRef, Q, Exists, TextField
from django.db.models.functions import Cast
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.awards.models import CovidFinancialAccountMatview
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import AwardTypeMixin
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, FabaOutlayMixin
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import ToptierAgency


class AgencyCountViewSet(AwardTypeMixin, FabaOutlayMixin, DisasterBase):
    """
    Obtain the count of Agencies related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/count.md"

    @cache_response()
    def post(self, request: Request) -> Response:
        filters = [
            self.all_closed_defc_submissions,
            self.has_award_of_provided_type(),
            self.is_in_provided_def_codes,
        ]

        if self.award_type_codes:
            count = (
                CovidFinancialAccountMatview.objects.annotate(cast_def_codes=Cast("def_codes", ArrayField(TextField())))
                .filter(
                    type__in=self.award_type_codes,
                    cast_def_codes__overlap=self.def_codes,
                    award__funding_agency__toptier_agency__isnull=False,
                )
                .values("award__funding_agency__toptier_agency")
                .distinct()
                .count()
            )

        else:
            filters.extend(
                [Q(treasury_account__funding_toptier_agency=OuterRef("pk")), self.is_non_zero_total_spending]
            )
            count = (
                ToptierAgency.objects.annotate(
                    include=Exists(FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters).values("pk"))
                )
                .filter(include=True)
                .values("pk")
                .count()
            )

        return Response({"count": count})
