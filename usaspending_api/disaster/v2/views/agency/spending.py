import json
import logging
from decimal import Decimal

from django.contrib.postgres.fields import ArrayField
from django.db.models import Case, DecimalField, F, IntegerField, Q, Sum, Value, When, Subquery, OuterRef, Func, Exists
from django.db.models.functions import Coalesce
from django.views.decorators.csrf import csrf_exempt
from django_cte import With
from rest_framework.response import Response
from typing import List

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    FabaOutlayMixin,
    latest_gtas_of_each_year_queryset,
    PaginationMixin,
    SpendingMixin,
)
from usaspending_api.disaster.v2.views.elasticsearch_base import (
    ElasticsearchDisasterBase,
    ElasticsearchSpendingPaginationMixin,
)
from usaspending_api.disaster.v2.views.elasticsearch_account_base import ElasticsearchAccountDisasterBase
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import Agency, ToptierAgency
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.search.v2.elasticsearch_helper import get_summed_value_as_float

logger = logging.getLogger(__name__)


def route_agency_spending_backend(**initkwargs):
    """
    Per API contract, delegate requests that specify `award_type_codes` to the Elasticsearch-backend that gets sum
    amounts based on subtier Agency associated with the linked award.
    Otherwise use the Postgres-backend that gets sum amount from toptier Agency associated with the File C TAS
    """
    spending_by_subtier_agency = SpendingBySubtierAgencyViewSet.as_view(**initkwargs)
    spending_by_agency = SpendingByAgencyViewSet.as_view(**initkwargs)

    @csrf_exempt
    def route_agency_spending_backend(request, *args, **kwargs):
        """
        Returns disaster spending by agency.  If agency type codes are provided, the characteristics of
        the result are modified a bit.  Instead of being purely a rollup of File C agency loans, the results
        become a rollup of File D subtier agencies by toptier agency and subtiers will be included as children
        of the toptier agency.
        """
        if DisasterBase.requests_award_type_codes(request) & DisasterBase.requests_award_spending_type(request):
            return spending_by_subtier_agency(request, *args, **kwargs)
        return spending_by_agency(request, *args, **kwargs)

    route_agency_spending_backend.endpoint_doc = SpendingBySubtierAgencyViewSet.endpoint_doc
    route_agency_spending_backend.__doc__ = SpendingBySubtierAgencyViewSet.__doc__
    return route_agency_spending_backend


class SpendingByAgencyViewSet(PaginationMixin, SpendingMixin, FabaOutlayMixin, ElasticsearchAccountDisasterBase):
    """ Returns disaster spending by agency. """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/spending.md"
    required_filters = ["def_codes", "award_type_codes", "query"]
    nested_nonzero_fields = {"obligation": "transaction_obligated_amount", "outlay": "gross_outlay_amount_by_award_cpe"}
    query_fields = [
        "funding_toptier_agency_name",
        "funding_toptier_agency_name.contains",
    ]
    agg_key = "financial_accounts_by_award.funding_toptier_agency_id"  # primary (tier-1) aggregation key
    top_hits_fields = [
        "financial_accounts_by_award.funding_toptier_agency_code",
        "financial_accounts_by_award.funding_toptier_agency_name",
    ]

    @cache_response()
    def post(self, request):
        if self.spending_type == "award":
            return self.perform_elasticsearch_search()
        else:
            results = self.total_queryset
            extra_columns = ["total_budgetary_resources"]

        results = list(results.order_by(*self.pagination.robust_order_by_fields))
        for item in results:  # we're checking for items that do not have an agency profile page
            if item.get("link") is not None:
                if not item["link"]:
                    item["id"] = None  # if they don't have a page (means they have no submission), we don't send the id
                item.pop("link")
        return Response(
            {
                "totals": self.accumulate_total_values(results, extra_columns),
                "results": results[self.pagination.lower_limit : self.pagination.upper_limit],
                "page_metadata": get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page),
            }
        )

    def build_elasticsearch_result(self, info_buckets: List[dict]) -> List[dict]:
        results = []
        for bucket in info_buckets:
            results.append(self._build_json_result(bucket))
        return results

    def _build_json_result(self, bucket: dict):
        return {
            "id": int(bucket["key"]),
            "code": bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["funding_toptier_agency_code"],
            "description": bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["funding_toptier_agency_name"],
            "children": [],
            # the count of distinct awards contributing to the totals
            "award_count": int(bucket["count_awards_by_dim"]["award_count"]["value"]),
            **{
                key: Decimal(bucket.get(f"sum_{val}", {"value": 0})["value"])
                for key, val in self.nested_nonzero_fields.items()
            },
            "total_budgetary_resources": None,
        }

    @property
    def total_queryset(self):

        cte_filters = [
            Q(treasury_account__isnull=False),
            self.all_closed_defc_submissions,
            self.is_in_provided_def_codes,
            self.is_non_zero_total_spending,
        ]

        cte_annotations = {
            "funding_toptier_agency_id": F("treasury_account__funding_toptier_agency_id"),
            "obligation": Coalesce(
                Sum(
                    Case(
                        When(
                            self.final_period_submission_query_filters,
                            then=F("obligations_incurred_by_program_object_class_cpe")
                            + F("deobligations_recoveries_refund_pri_program_object_class_cpe"),
                        ),
                        default=Value(0),
                        output_field=DecimalField(max_digits=23, decimal_places=2),
                    )
                ),
                0,
                output_field=DecimalField(max_digits=23, decimal_places=2),
            ),
            "outlay": Coalesce(
                Sum(
                    Case(
                        When(
                            self.final_period_submission_query_filters,
                            then=F("gross_outlay_amount_by_program_object_class_cpe")
                            + F("ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe")
                            + F("ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe"),
                        ),
                        default=Value(0),
                        output_field=DecimalField(max_digits=23, decimal_places=2),
                    )
                ),
                0,
                output_field=DecimalField(max_digits=23, decimal_places=2),
            ),
        }

        cte = With(
            FinancialAccountsByProgramActivityObjectClass.objects.filter(*cte_filters)
            .values("treasury_account__funding_toptier_agency_id")
            .annotate(**cte_annotations)
            .values(*cte_annotations)
        )

        annotations = {
            "id": Subquery(
                Agency.objects.filter(toptier_agency_id=OuterRef("toptier_agency_id"))
                .order_by("-toptier_flag", "id")
                .values("id")[:1]
            ),
            "code": F("toptier_code"),
            "description": F("name"),
            # Currently, this endpoint can never have children w/o type = `award` & `award_type_codes`
            "children": Value([], output_field=ArrayField(IntegerField())),
            "award_count": Value(None, output_field=IntegerField()),
            "obligation": cte.col.obligation,
            "outlay": cte.col.outlay,
            "total_budgetary_resources": Coalesce(
                Subquery(
                    latest_gtas_of_each_year_queryset()
                    .filter(
                        disaster_emergency_fund_id__in=self.def_codes,
                        treasury_account_identifier__funding_toptier_agency_id=OuterRef("toptier_agency_id"),
                    )
                    .annotate(
                        amount=Func("total_budgetary_resources_cpe", function="Sum"),
                        unobligated_balance=Func(
                            "budget_authority_unobligated_balance_brought_forward_cpe", function="Sum"
                        ),
                        deobligation=Func("deobligations_or_recoveries_or_refunds_from_prior_year_cpe", function="Sum"),
                        prior_year=Func("prior_year_paid_obligation_recoveries", function="Sum"),
                        unobligated_adjustments=Func(
                            "adjustments_to_unobligated_balance_brought_forward_cpe", function="Sum"
                        ),
                    )
                    .annotate(
                        total_budget_authority=F("amount")
                        - F("unobligated_balance")
                        - F("deobligation")
                        - F("prior_year")
                        - F("unobligated_adjustments")
                    )
                    .values("total_budget_authority"),
                    output_field=DecimalField(max_digits=23, decimal_places=2),
                ),
                0,
                output_field=DecimalField(max_digits=23, decimal_places=2),
            ),
            "link": Exists(SubmissionAttributes.objects.filter(toptier_code=OuterRef("toptier_code"))),
        }

        return (
            cte.join(ToptierAgency, toptier_agency_id=cte.col.funding_toptier_agency_id)
            .with_cte(cte)
            .annotate(**annotations)
            .values(*annotations)
        )


class SpendingBySubtierAgencyViewSet(ElasticsearchSpendingPaginationMixin, ElasticsearchDisasterBase):
    """
    This route takes DEF Codes and Award Type Codes and returns Spending by Subtier Agency, rolled up to include
    totals for each distinct Toptier agency.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/spending.md"

    required_filters = ["def_codes", "award_type_codes", "query"]
    query_fields = [
        "funding_toptier_agency_name.contains",
        "funding_toptier_agency_name",
        "funding_subtier_agency_name.contains",
        "funding_subtier_agency_name",
    ]
    agg_key = "funding_toptier_agency_agg_key"  # primary (tier-1) aggregation key
    sub_agg_key = "funding_subtier_agency_agg_key"  # secondary (tier-2) sub-aggregation key

    def build_elasticsearch_result(self, info_buckets: List[dict]) -> List[dict]:
        results = []
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
            "id": info["id"],
            "code": info["code"],
            "description": info["name"],
            # the count of distinct awards contributing to the totals
            "award_count": int(bucket.get("doc_count", 0)),
            **{
                column: get_summed_value_as_float(
                    bucket.get("nested", {}).get("filtered_aggs", {}), self.sum_column_mapping[column]
                )
                for column in self.sum_column_mapping
            },
        }
