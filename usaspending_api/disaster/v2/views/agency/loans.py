from django.contrib.postgres.fields import ArrayField
from django.db.models import Q, Sum, F, Value, Case, When, IntegerField
from django.db.models.functions import Coalesce
from rest_framework.response import Response
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.awards.v2.lookups.lookups import loan_type_mapping
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, LoansPaginationMixin, LoansMixin


class LoansByAgencyViewSet(LoansPaginationMixin, LoansMixin, DisasterBase):
    """ Returns loan disaster spending by agency. """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/loans.md"

    @cache_response()
    def post(self, request):
        results = self.queryset
        return Response(
            {
                "results": results.order_by(self.pagination.order_by)[
                    self.pagination.lower_limit : self.pagination.upper_limit
                ],
                "page_metadata": get_pagination_metadata(results.count(), self.pagination.limit, self.pagination.page),
            }
        )

    @property
    def queryset(self):
        filters = [
            Q(award__type__in=loan_type_mapping),
            Q(disaster_emergency_fund__in=self.def_codes),
            Q(treasury_account__isnull=False),
            Q(treasury_account__funding_toptier_agency__isnull=False),
            self.all_closed_defc_submissions,
        ]

        annotations = {
            "id": F("treasury_account__funding_toptier_agency"),
            "code": F("treasury_account__funding_toptier_agency__toptier_code"),
            "description": F("treasury_account__funding_toptier_agency__name"),
            # Currently, this endpoint can never have children.
            "children": Value([], output_field=ArrayField(IntegerField())),
            "count": Value(0, output_field=IntegerField()),
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
            "face_value_of_loan": Coalesce(Sum("award__total_loan_value"), 0),
        }

        return (
            FinancialAccountsByAwards.objects.filter(*filters)
            .values(
                "treasury_account__funding_toptier_agency",
                "treasury_account__funding_toptier_agency__toptier_code",
                "treasury_account__funding_toptier_agency__name",
            )
            .annotate(**annotations)
            .values(*annotations.keys())
        )
