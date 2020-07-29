import json
import logging

from decimal import Decimal
from django.contrib.postgres.fields import ArrayField
from django.db.models import Q, Sum, F, Value, Case, When, IntegerField, Subquery, OuterRef, Count
from django.db.models.functions import Coalesce
from django.views.decorators.csrf import csrf_exempt
from django_cte import With
from rest_framework.response import Response
from typing import List
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    LoansPaginationMixin,
    LoansMixin,
    FabaOutlayMixin,
    final_submissions_for_all_fy,
)
from usaspending_api.disaster.v2.views.elasticsearch_base import (
    ElasticsearchDisasterBase,
    ElasticsearchLoansPaginationMixin,
)
from usaspending_api.references.models import Agency, ToptierAgency

logger = logging.getLogger(__name__)


def route_agency_loans_backend(**initkwargs):
    """
    Per API contract, delegate requests that specify `award_type_codes` to the Elasticsearch-backend that gets sum
    amounts based on subtier Agency associated with the linked award.
    Otherwise use the Postgres-backend that gets sum amount from toptier Agency associated with the File C TAS
    """
    loans_by_subtier_agency = LoansBySubtierAgencyViewSet.as_view(**initkwargs)
    loans_by_agency = LoansByAgencyViewSet.as_view(**initkwargs)

    @csrf_exempt
    def route_agency_loans_backend(request, *args, **kwargs):
        if DisasterBase.requests_award_type_codes(request):
            return loans_by_subtier_agency(request, *args, **kwargs)
        return loans_by_agency(request, *args, **kwargs)

    route_agency_loans_backend.endpoint_doc = LoansBySubtierAgencyViewSet.endpoint_doc
    route_agency_loans_backend.__doc__ = LoansBySubtierAgencyViewSet.__doc__
    return route_agency_loans_backend


class LoansByAgencyViewSet(LoansPaginationMixin, LoansMixin, FabaOutlayMixin, DisasterBase):
    """
        This endpoint provides insights on the Agencies awarding loans from
        disaster/emergency funding per the requested filters.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/loans.md"

    @cache_response()
    def post(self, request):

        # This is an expensive query and can only ever return a little over a hundred rows so it's
        # actually faster to materialize the entire result set and carve off what we need than to
        # run it twice (once for the page of results we want and a second time for the overall count).
        results = list(self.queryset.order_by(*self.pagination.robust_order_by_fields))
        return Response(
            {
                "results": results[self.pagination.lower_limit : self.pagination.upper_limit],
                "page_metadata": get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page),
            }
        )

    @property
    def queryset(self):

        base_values = With(
            FinancialAccountsByAwards.objects.filter(
                Q(treasury_account__isnull=False),
                self.all_closed_defc_submissions,
                self.is_in_provided_def_codes,
                self.is_loan_award,
            )
            .annotate(
                funding_toptier_agency_id=F("treasury_account__funding_toptier_agency_id"),
                total_loan_value=F("award__total_loan_value"),
                reporting_fiscal_year=F("submission__reporting_fiscal_year"),
                reporting_fiscal_period=F("submission__reporting_fiscal_period"),
                quarter_format_flag=F("submission__quarter_format_flag"),
            )
            .values(
                "funding_toptier_agency_id",
                "financial_accounts_by_awards_id",
                "award_id",
                "transaction_obligated_amount",
                "gross_outlay_amount_by_award_cpe",
                "reporting_fiscal_year",
                "reporting_fiscal_period",
                "quarter_format_flag",
                "total_loan_value",
            ),
            "base_values",
        )

        q = Q()
        for sub in final_submissions_for_all_fy():
            q |= (
                Q(reporting_fiscal_year=sub.fiscal_year)
                & Q(quarter_format_flag=sub.is_quarter)
                & Q(reporting_fiscal_period=sub.fiscal_period)
            )

        aggregate_faba = With(
            base_values.queryset()
            .values("funding_toptier_agency_id")
            .annotate(
                obligation=Coalesce(Sum("transaction_obligated_amount"), 0),
                outlay=Coalesce(Sum(Case(When(q, then=F("gross_outlay_amount_by_award_cpe")), default=Value(0),)), 0,),
            )
            .values("funding_toptier_agency_id", "obligation", "outlay"),
            "aggregate_faba",
        )

        distinct_awards = With(
            base_values.queryset().values("funding_toptier_agency_id", "award_id", "total_loan_value").distinct(),
            "distinct_awards",
        )

        aggregate_awards = With(
            distinct_awards.queryset()
            .values("funding_toptier_agency_id")
            .annotate(award_count=Count("award_id"), face_value_of_loan=Coalesce(Sum("total_loan_value"), 0))
            .values("funding_toptier_agency_id", "award_count", "face_value_of_loan"),
            "aggregate_awards",
        )

        annotations = {
            "id": Subquery(
                Agency.objects.filter(toptier_agency=OuterRef("toptier_agency_id"))
                .order_by("-toptier_flag", "id")
                .values("id")[:1]
            ),
            "code": F("toptier_code"),
            "description": F("name"),
            # Currently, this flavor of the endpoint can never have children
            "children": Value([], output_field=ArrayField(IntegerField())),
            "award_count": aggregate_awards.col.award_count,
            "obligation": aggregate_faba.col.obligation,
            "outlay": aggregate_faba.col.outlay,
            "face_value_of_loan": aggregate_awards.col.face_value_of_loan,
        }

        return (
            aggregate_awards.join(
                aggregate_faba.join(ToptierAgency, toptier_agency_id=aggregate_faba.col.funding_toptier_agency_id),
                toptier_agency_id=aggregate_awards.col.funding_toptier_agency_id,
            )
            .with_cte(base_values)
            .with_cte(aggregate_faba)
            .with_cte(distinct_awards)
            .with_cte(aggregate_awards)
            .annotate(**annotations)
            .values(*annotations)
        )


class LoansBySubtierAgencyViewSet(ElasticsearchLoansPaginationMixin, ElasticsearchDisasterBase):
    """
    This route takes DEF Codes and Award Type Codes and returns Loans by Subtier Agency, rolled up to include
    totals for each distinct Toptier agency.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/loans.md"

    required_filters = ["def_codes", "_loan_award_type_codes", "query"]
    query_fields = ["funding_toptier_agency_name"]
    agg_key = "funding_toptier_agency_agg_key"  # primary (tier-1) aggregation key
    sub_agg_key = "funding_subtier_agency_agg_key"  # secondary (tier-2) sub-aggregation key

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        info_buckets = response.get(self.agg_group_name, {}).get("buckets", [])
        for bucket in info_buckets:
            result = self._build_json_result(bucket)
            child_info_buckets = bucket.get(self.sub_agg_group_name, {}).get("buckets", [])
            children = []
            for child_bucket in child_info_buckets:
                children.append(self._build_json_result(child_bucket))
            result["children"] = children
            results.append(result)

        return results

    def _build_json_result(self, bucket: dict):
        info = json.loads(bucket.get("key"))
        return {
            "id": int(info["id"]),
            "code": info["code"],
            "description": info["name"],
            # the count of distinct awards contributing to the totals
            "award_count": int(bucket.get("doc_count", 0)),
            **{
                column: int(bucket.get(self.sum_column_mapping[column], {"value": 0})["value"]) / Decimal("100")
                for column in self.sum_column_mapping
            },
        }
