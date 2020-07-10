from django.db.models import OuterRef, Q, Exists, Count
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.count_base import CountBase
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import ToptierAgency
from usaspending_api.disaster.v2.views.disaster_base import AwardTypeMixin


class AgencyCountViewSet(AwardTypeMixin, CountBase):
    """
    Obtain the count of Agencies related to supplied DEFC filter.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/count.md"

    @cache_response()
    def post(self, request: Request) -> Response:
        if self.award_type_codes:
            filters = [
                self.is_in_provided_def_codes(),
                self.all_closed_defc_submissions,
                self.is_non_zero_award_cpe(),
            ]
            count = FinancialAccountsByAwards.objects.filter(*filters)
            count = count.values("award_id").filter(award__type__in=self.filters.get("award_type_codes"))
            count = count.aggregate(count=Count("award__funding_agency__toptier_agency", distinct=True))["count"]
        else:
            filters = [
                Q(treasury_account__funding_toptier_agency=OuterRef("pk")),
                self.is_in_provided_def_codes(),
                self.all_closed_defc_submissions,
                self.is_non_zero_object_class_cpe(),
            ]
            count = (
                ToptierAgency.objects.annotate(
                    include=Exists(FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters).values("pk"))
                )
                .filter(include=True)
                .values("pk")
                .count()
            )
        return Response({"count": count})
