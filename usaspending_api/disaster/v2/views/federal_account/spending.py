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
    LoansPaginationMixin,
    LoansMixin,
    PaginationMixin,
    SpendingMixin,
)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass


def construct_response(
    results: list, pagination: Pagination, for_total: bool = False,
):
    federal_accounts = FedAcctResults()
    for row in results:
        FA = FedAccount(id=row["fa_id"], code=row["fa_code"], description=row["fa_description"],)
        Tas = TAS(
            id=row["id"],
            code=row["code"],
            count=row["count"],
            description=row["description"],
            obligation=row["obligation"],
            outlay=row["outlay"],
            total_budgetary_resources=row["total_budgetary_resources"] if for_total else None,
        )

        federal_accounts.add_if_missing(FA)
        federal_accounts[FA].include(Tas)

    page_metadata = get_pagination_metadata(len(federal_accounts), pagination.limit, pagination.page)

    return {
        "results": federal_accounts.finalize(
            pagination.sort_key, pagination.sort_order, pagination.lower_limit, pagination.upper_limit,
        ),
        "page_metadata": page_metadata,
    }


class Spending(PaginationMixin, SpendingMixin, DisasterBase):
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/spending.md"

    @cache_response()
    def post(self, request):
        if self.spending_type == "award":
            for_total = False
            # results = list(self.get_award_queryset())
            queryset = self.get_award_queryset()
            from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query

            print(f"=======================================\nQueryset: {generate_raw_quoted_query(queryset)}")
            results = list(queryset)
        else:
            for_total = True
            # results = list(self.get_total_queryset())
            queryset = self.get_total_queryset()
            from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query

            print(f"=======================================\nQueryset: {generate_raw_quoted_query(queryset)}")
            results = list(queryset)

        return Response(construct_response(results, self.pagination, for_total))

    def get_total_queryset(self):
        filters = [
            Q(submission__reporting_period_start__gte=self.reporting_period_min),
            Q(
                Q(
                    Q(submission__reporting_period_end__lte=self.recent_monthly_submission["submission_reveal_date"])
                    & Q(submission__quarter_format_flag=False)
                )
                | Q(
                    Q(submission__reporting_period_end__lte=self.recent_quarterly_submission["submission_reveal_date"])
                    & Q(submission__quarter_format_flag=True)
                )
            ),
            Q(
                Q(obligations_incurred_by_program_object_class_cpe__gt=0)
                | Q(obligations_incurred_by_program_object_class_cpe__lt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__gt=0)
                | Q(gross_outlay_amount_by_program_object_class_cpe__lt=0)
            ),
            Q(disaster_emergency_fund__in=self.def_codes),
            Q(treasury_account__isnull=False),
            Q(treasury_account__federal_account__isnull=False),
        ]
        annotations = {
            "fa_code": F("treasury_account__federal_account__federal_account_code"),
            "count": Count("treasury_account__tas_rendering_label", distinct=True),
            "description": F("treasury_account__account_title"),
            "code": F("treasury_account__tas_rendering_label"),
            "id": F("treasury_account__treasury_account_identifier"),
            "fa_description": F("treasury_account__federal_account__account_title"),
            "fa_id": F("treasury_account__federal_account_id"),
            "obligation": Sum("obligations_incurred_by_program_object_class_cpe"),
            "outlay": Sum("gross_outlay_amount_by_program_object_class_cpe"),
            "total_budgetary_resources": Sum(
                F("obligations_incurred_by_program_object_class_cpe")
                + F("gross_outlay_amount_by_program_object_class_cpe"),
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

    def get_award_queryset(self):
        filters = [
            Q(submission__reporting_period_start__gte=self.reporting_period_min),
            Q(
                Q(
                    Q(submission__reporting_period_end__lte=self.recent_monthly_submission["submission_reveal_date"])
                    & Q(submission__quarter_format_flag=False)
                )
                | Q(
                    Q(submission__reporting_period_end__lte=self.recent_quarterly_submission["submission_reveal_date"])
                    & Q(submission__quarter_format_flag=True)
                )
            ),
            Q(disaster_emergency_fund__in=self.def_codes),
            Q(treasury_account__isnull=False),
            Q(treasury_account__federal_account__isnull=False),
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
                        When(
                            Q(
                                submission__reporting_fiscal_year=self.recent_monthly_submission[
                                    "submission_fiscal_year"
                                ],
                                submission__reporting_fiscal_period=self.recent_monthly_submission[
                                    "submission_fiscal_month"
                                ],
                                submission__quarter_format_flag=False,
                            )
                            | Q(
                                submission__reporting_fiscal_year=self.recent_quarterly_submission[
                                    "submission_fiscal_year"
                                ],
                                submission__reporting_fiscal_quarter=self.recent_quarterly_submission[
                                    "submission_fiscal_quarter"
                                ],
                                submission__quarter_format_flag=True,
                            ),
                            then=F("gross_outlay_amount_by_award_cpe"),
                        ),
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


class Loans(LoansMixin, LoansPaginationMixin, DisasterBase):
    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/federal_account/loans.md"

    @cache_response()
    def post(self, request):

        results = list(self.get_queryset())
        results = construct_response(results, self.pagination, False)

        for result in results["results"]:
            for child in result["children"]:
                child.pop("obligation")
                child.pop("total_budgetary_resources")
                child["face_value_of_loan"] = child.pop("outlay")
            result.pop("obligation")
            result.pop("total_budgetary_resources")
            result["face_value_of_loan"] = result.pop("outlay")

        return Response(results)

    def get_queryset(self):
        filters = [
            Q(submission__reporting_period_start__gte=self.reporting_period_min),
            Q(
                Q(
                    Q(submission__reporting_period_end__lte=self.recent_monthly_submission["submission_reveal_date"])
                    & Q(submission__quarter_format_flag=False)
                )
                | Q(
                    Q(submission__reporting_period_end__lte=self.recent_quarterly_submission["submission_reveal_date"])
                    & Q(submission__quarter_format_flag=True)
                )
            ),
            Q(disaster_emergency_fund__in=self.def_codes),
            Q(award_id__isnull=False),
            Q(treasury_account__isnull=False),
            Q(treasury_account__federal_account__isnull=False),
        ]
        annotations = {
            "fa_code": F("treasury_account__federal_account__federal_account_code"),
            "count": Count("award_id", distinct=True),
            "description": F("treasury_account__account_title"),
            "code": F("treasury_account__tas_rendering_label"),
            "id": F("treasury_account__treasury_account_identifier"),
            "fa_description": F("treasury_account__federal_account__account_title"),
            "fa_id": F("treasury_account__federal_account_id"),
            "obligation": Value(0, DecimalField(max_digits=23, decimal_places=2)),
            "outlay": Coalesce(Sum("award__total_loan_value"), 0),
            "total_budgetary_resources": Value(None, DecimalField(max_digits=23, decimal_places=2)),  # Throw-away field
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
