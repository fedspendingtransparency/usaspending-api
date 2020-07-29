from django.db.models import Q, Sum, F, Count
from django.db.models.functions import Coalesce
from rest_framework.response import Response

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    LoansPaginationMixin,
    LoansMixin,
    FabaOutlayMixin,
)
from usaspending_api.disaster.v2.views.federal_account.spending import construct_response


class LoansViewSet(LoansMixin, LoansPaginationMixin, FabaOutlayMixin, DisasterBase):
    """ Returns loan disaster spending by federal account. """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/loans.md"

    @cache_response()
    def post(self, request):
        # rename hack to use the Dataclasses, setting to Dataclass attribute name
        if self.pagination.sort_key == "face_value_of_loan":
            self.pagination.sort_key = "total_budgetary_resources"

        results = construct_response(list(self.queryset), self.pagination)

        # rename hack to use the Dataclasses, swapping back in desired loan field name
        for result in results["results"]:
            for child in result["children"]:
                child["face_value_of_loan"] = child.pop("total_budgetary_resources")
            result["face_value_of_loan"] = result.pop("total_budgetary_resources")

        return Response(results)

    @property
    def queryset(self):
        filters = [
            Q(award_id__isnull=False),
            Q(treasury_account__federal_account__isnull=False),
            Q(treasury_account__isnull=False),
            self.all_closed_defc_submissions,
            self.is_in_provided_def_codes,
            self.is_loan_award,
        ]

        annotations = {
            "fa_code": F("treasury_account__federal_account__federal_account_code"),
            "award_count": Count("award_id", distinct=True),
            "description": F("treasury_account__account_title"),
            "code": F("treasury_account__tas_rendering_label"),
            "id": F("treasury_account__treasury_account_identifier"),
            "fa_description": F("treasury_account__federal_account__account_title"),
            "fa_id": F("treasury_account__federal_account_id"),
            "obligation": Coalesce(Sum("transaction_obligated_amount"), 0),
            "outlay": self.outlay_field_annotation,
            # hack to use the Dataclasses, will be renamed later
            "total_budgetary_resources": Coalesce(Sum("award__total_loan_value"), 0),
        }

        # Assuming it is more performant to fetch all rows once rather than
        #  run a count query and fetch only a page's worth of results
        return (
            FinancialAccountsByAwards.objects.filter(*filters)
            .values(
                "treasury_account__federal_account__id",
                "treasury_account__federal_account__federal_account_code",
                "treasury_account__federal_account__account_title",
            )
            .annotate(**annotations)
            .values(*annotations.keys())
        )
