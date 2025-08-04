import logging
from decimal import Decimal
from typing import List

from django.contrib.postgres.fields import ArrayField
from django.db.models import (
    DecimalField,
    Exists,
    F,
    Func,
    IntegerField,
    OuterRef,
    Q,
    Subquery,
    Sum,
    Value,
)
from django.db.models.functions import Coalesce
from django.views.decorators.csrf import csrf_exempt
from django_cte import With
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.calculations.file_b import FileBCalculations
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    FabaOutlayMixin,
    PaginationMixin,
    SpendingMixin,
    latest_gtas_of_each_year_queryset,
)
from usaspending_api.disaster.v2.views.elasticsearch_base import (
    ElasticsearchDisasterBase,
    ElasticsearchSpendingPaginationMixin,
)
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import Agency, ToptierAgency
from usaspending_api.search.v2.elasticsearch_helper import get_summed_value_as_float
from usaspending_api.submissions.models import SubmissionAttributes

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


class SpendingByAgencyViewSet(FabaOutlayMixin, PaginationMixin, DisasterBase, SpendingMixin):
    """Returns disaster spending by agency."""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/spending.md"
    required_filters = ["def_codes", "award_type_codes", "query"]

    @cache_response()
    def post(self, request):
        if self.spending_type == "award":
            covid_faba_agency_spending = self.get_covid_faba_spending(
                spending_level="subtier_agency",
                def_codes=self.filters["def_codes"],
                columns_to_return=[
                    "funding_toptier_agency_id",
                    "funding_toptier_agency_code",
                    "funding_toptier_agency_name",
                ],
                award_types=self.filters.get("award_type_codes"),
                search_query=self.query,
                search_query_fields=["funding_toptier_agency_name"],
            )
            json_result = self._build_json_result(covid_faba_agency_spending)
            sorted_json_result = self.sort_json_result(
                data_to_sort=json_result,
                sort_key=self.pagination.sort_key,
                sort_order=self.pagination.sort_order,
                has_children=False,
            )

            return Response(sorted_json_result)
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

    def _build_json_result(self, queryset: List[dict]) -> dict:
        """Build the JSON response that will be returned for this endpoint.

        Args:
            queryset: Database query results.

        Returns:
            Formatted JSON response.
        """

        response = {}

        results = [
            {
                "id": int(row["funding_toptier_agency_id"]),
                "code": row["funding_toptier_agency_code"],
                "description": row["funding_toptier_agency_name"],
                "children": [],  # This type of spending will never have children
                "award_count": int(row["award_count"]),
                "obligation": Decimal(row["obligation_sum"]),
                "outlay": Decimal(row["outlay_sum"]),
                "total_budgetary_resources": None,
            }
            for row in queryset
        ]

        response["totals"] = {
            "obligation": sum(result["obligation"] for result in results),
            "outlay": sum(result["outlay"] for result in results),
            "award_count": sum(result["award_count"] for result in results),
        }
        response["results"] = results[self.pagination.lower_limit : self.pagination.upper_limit]
        response["page_metadata"] = get_pagination_metadata(len(results), self.pagination.limit, self.pagination.page)

        return response

    @property
    def total_queryset(self):
        file_b_calculations = FileBCalculations(include_final_sub_filter=True, is_covid_page=True)

        cte_filters = [
            Q(treasury_account__isnull=False),
            self.all_closed_defc_submissions,
            self.is_in_provided_def_codes,
            file_b_calculations.is_non_zero_total_spending(),
        ]

        cte_annotations = {
            "funding_toptier_agency_id": F("treasury_account__funding_toptier_agency_id"),
            "obligation": Sum(file_b_calculations.get_obligations()),
            "outlay": Sum(file_b_calculations.get_outlays()),
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
                            "adjustments_to_unobligated_balance_brought_forward_fyb", function="Sum"
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
    top_hits_fields = ["funding_toptier_agency_name", "funding_toptier_agency_code", "funding_agency_id"]
    sub_top_hits_fields = ["funding_subtier_agency_name", "funding_subtier_agency_code", "funding_agency_id"]

    def build_elasticsearch_result(self, info_buckets: List[dict]) -> List[dict]:
        results = []
        for bucket in info_buckets:
            result = self._build_json_result(bucket, child=False)
            child_info_buckets = bucket.get(self.sub_agg_group_name, {}).get("buckets", [])
            children = []
            for child_bucket in child_info_buckets:
                children.append(self._build_json_result(child_bucket, child=True))
            result["children"] = children
            results.append(result)

        if self.pagination.sort_key == "description":
            results = sorted(
                results, key=lambda val: val.get("description").lower(), reverse=self.pagination.sort_order == "desc"
            )

        return results

    def _build_json_result(self, bucket: dict, child: bool):
        agency_id = None
        tier = "sub" if child else "top"
        tid = Agency.objects.filter(
            id=bucket["dim_metadata"]["hits"]["hits"][0]["_source"]["funding_agency_id"]
        ).first()
        if tid:
            toptier_id = tid.toptier_agency_id
            aid = Agency.objects.filter(toptier_agency_id=toptier_id).order_by("-toptier_flag", "-id").first()
            if aid:
                agency_id = aid.id
        return {
            "id": agency_id,
            "code": bucket.get("key"),
            "description": bucket["dim_metadata"]["hits"]["hits"][0]["_source"][f"funding_{tier}tier_agency_name"],
            # the count of distinct awards contributing to the totals
            "award_count": int(bucket.get("doc_count", 0)),
            **{
                column: get_summed_value_as_float(
                    bucket.get("nested", {}).get("filtered_aggs", {}), self.sum_column_mapping[column]
                )
                for column in self.sum_column_mapping
            },
        }
