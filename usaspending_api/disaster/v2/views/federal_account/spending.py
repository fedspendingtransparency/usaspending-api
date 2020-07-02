from django.db.models import Q, Sum, Count, F, Value, DecimalField, Case, When
from django.db.models.functions import Coalesce
from rest_framework.response import Response

from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.federal_account.federal_account_result import FedAcctResults, FedAccount, TAS
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    PaginationMixin,
    SpendingMixin,
)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


def construct_response(results: list, pagination: Pagination):
    FederalAccounts = FedAcctResults()
    for row in results:
        FA = FedAccount(id=row.pop("fa_id"), code=row.pop("fa_code"), description=row.pop("fa_description"))
        FederalAccounts[FA].include(TAS(**row))

    return {
        "results": FederalAccounts.finalize(pagination),
        "page_metadata": get_pagination_metadata(len(FederalAccounts), pagination.limit, pagination.page),
    }


class Spending(PaginationMixin, SpendingMixin, DisasterBase):
    """View to implement the API"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/spending.md"

    @cache_response()
    def post(self, request):
        if self.spending_type == "award":
            results = list(self.award_queryset)
        else:
            results = list(self.total_queryset)

        return Response(construct_response(results, self.pagination))

    @property
    def total_queryset(self):
        filters = [
            Q(
                Q(obligations_incurred_by_program_object_class_cpe__gt=0)
                | Q(obligations_incurred_by_program_object_class_cpe__lt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__gt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__lt=0)
            ),
            Q(disaster_emergency_fund__in=self.def_codes),
            Q(treasury_account__isnull=False),
            Q(treasury_account__federal_account__isnull=False),
            self.all_closed_defc_submissions,
        ]

        annotations = {
            "fa_code": F("treasury_account__federal_account__federal_account_code"),
            "count": Count("treasury_account__tas_rendering_label", distinct=True),
            "description": F("treasury_account__account_title"),
            "code": F("treasury_account__tas_rendering_label"),
            "id": F("treasury_account__treasury_account_identifier"),
            "fa_description": F("treasury_account__federal_account__account_title"),
            "fa_id": F("treasury_account__federal_account_id"),
            "obligation": Coalesce(
                Sum(
                    Case(
                        When(
                            self.final_period_submission_query_filters,
                            then=F("obligations_incurred_by_program_object_class_cpe"),
                        ),
                        default=Value(0),
                    )
                ),
                0,
            ),
            "outlay": Coalesce(
                Sum(
                    Case(
                        When(
                            self.final_period_submission_query_filters,
                            then=F("gross_outlay_amount_by_program_object_class_cpe"),
                        ),
                        default=Value(0),
                    )
                ),
                0,
            ),
            "total_budgetary_resources": Coalesce(
                Sum("treasury_account__gtas__budget_authority_appropriation_amount_cpe"), 0
            ),
        }

        # Assuming it is more performant to fetch all rows once rather than
        #  run a count query and fetch only a page's worth of results
        return (
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*filters)
            .values(
                "treasury_account__federal_account__id",
                "treasury_account__federal_account__federal_account_code",
                "treasury_account__federal_account__account_title",
            )
            .annotate(**annotations)
            .values(*annotations.keys())
        )

    @property
    def award_queryset(self):
        filters = [
            Q(disaster_emergency_fund__in=self.def_codes),
            Q(treasury_account__isnull=False),
            Q(treasury_account__federal_account__isnull=False),
            self.all_closed_defc_submissions,
        ]

        annotations = {
            "fa_code": F("treasury_account__federal_account__federal_account_code"),
            "count": Count("treasury_account__tas_rendering_label", distinct=True),
            "description": F("treasury_account__account_title"),
            "code": F("treasury_account__tas_rendering_label"),
            "id": F("treasury_account__treasury_account_identifier"),
            "fa_description": F("treasury_account__federal_account__account_title"),
            "fa_id": F("treasury_account__federal_account_id"),
            "obligation": Coalesce(Sum("transaction_obligated_amount"), 0),
            "outlay": Coalesce(
                Sum(
                    Case(
                        When(self.final_period_submission_query_filters, then=F("gross_outlay_amount_by_award_cpe")),
                        default=Value(0),
                    )
                ),
                0,
            ),
            "total_budgetary_resources": Value(None, DecimalField()),  # NULL for award spending
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
