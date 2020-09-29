import logging

from django.contrib.postgres.fields import ArrayField
from django.db.models import Case, DecimalField, F, IntegerField, Q, Sum, Value, When, Subquery, OuterRef, Func, Exists
from django.db.models.functions import Coalesce
from django.views.decorators.csrf import csrf_exempt
from django_cte import With
from rest_framework.response import Response
from typing import List
from elasticsearch_dsl import Q as ES_Q

from usaspending_api import settings
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.json_helpers import json_str_to_dict
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.disaster.v2.views.disaster_base import (
    DisasterBase,
    PaginationMixin,
    SpendingMixin,
    FabaOutlayMixin,
)
from usaspending_api.disaster.v2.views.elasticsearch_base import (
    ElasticsearchDisasterBase,
    ElasticsearchSpendingPaginationMixin,
)
from usaspending_api.disaster.v2.views.elasticsearch_account_base import ElasticsearchAccountDisasterBase
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import GTASSF133Balances, Agency, ToptierAgency
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


class SpendingByAgencyViewSet(
    PaginationMixin,
    SpendingMixin,
    FabaOutlayMixin,
    ElasticsearchAccountDisasterBase,
    ElasticsearchSpendingPaginationMixin,
    DisasterBase,
):
    """ Returns disaster spending by agency. """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/agency/spending.md"
    required_filters = ["def_codes", "award_type_codes", "query"]
    query_fields = ["funding_toptier_agency_name.contains"]
    agg_key = "financial_accounts_by_award.funding_toptier_agency_id"  # primary (tier-1) aggregation key
    top_hits_fields = [
        "financial_accounts_by_award.funding_toptier_agency_code",
        "financial_accounts_by_award.funding_toptier_agency_name",
    ]

    @cache_response()
    def post(self, request):
        if self.spending_type == "award":
            # query = self.filters.pop("query", None)
            # if query:
            #     self.filters["query"] = {"text": query, "fields": self.query_fields}
            self.filter_query = ES_Q("bool", filter={"exists": {"field": "financial_account_distinct_award_key"}})
            defc = self.filters.pop("def_codes")
            self.filters.update({"nested_def_codes": defc})
            self.filter_query = QueryWithFilters.generate_accounts_elasticsearch_query(self.filters)

            # Ensure that only non-zero values are taken into consideration
            # TODO: Refactor to use new NonzeroFields filter in QueryWithFilters
            # non_zero_queries = []
            # for field in self.sum_column_mapping.values():
            #     non_zero_queries.append(ES_Q("range", **{field: {"gt": 0}}))
            #     non_zero_queries.append(ES_Q("range", **{field: {"lt": 0}}))
            # self.filter_query.must.append(ES_Q("bool", should=non_zero_queries, minimum_should_match=1))

            self.bucket_count = (
                10000  # get_number_of_unique_terms_for_awards(self.filter_query, f"{self.agg_key}.hash")
            )
            messages = []
            if self.pagination.sort_key in ("id", "code"):
                messages.append(
                    (
                        f"Notice! API Request to sort on '{self.pagination.sort_key}' field isn't fully implemented."
                        " Results were actually sorted using 'description' field."
                    )
                )
            if self.bucket_count > 10000 and self.agg_key == settings.ES_ROUTING_FIELD:
                self.bucket_count = 10000
                messages.append(
                    (
                        "Notice! API Request is capped at 10,000 results. Either download to view all results or"
                        " filter using the 'query' attribute."
                    )
                )

            response = self.query_elasticsearch()
            response["page_metadata"] = get_pagination_metadata(
                self.bucket_count, self.pagination.limit, self.pagination.page
            )
            if messages:
                response["messages"] = messages
            return Response(response)
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
            # the count of distinct awards contributing to the totals
            "award_count": int(bucket["count_awards_by_dim"]["award_count"]["value"]),
            "obligations": int(bucket["sum_covid_obligation"]["value"]),
            "outlays": int(bucket["sum_covid_outlay"]["value"]),
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
                    GTASSF133Balances.objects.filter(
                        disaster_emergency_fund_code__in=self.def_codes,
                        fiscal_period=self.latest_reporting_period["submission_fiscal_month"],
                        fiscal_year=self.latest_reporting_period["submission_fiscal_year"],
                        treasury_account_identifier__funding_toptier_agency_id=OuterRef("toptier_agency_id"),
                    )
                    .annotate(amount=Func("total_budgetary_resources_cpe", function="Sum"))
                    .values("amount"),
                    output_field=DecimalField(),
                ),
                0,
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
    query_fields = ["funding_toptier_agency_name.contains"]
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
        info = json_str_to_dict(bucket.get("key"))
        return {
            "id": int(info["id"]),
            "code": info["code"],
            "description": info["name"],
            # the count of distinct awards contributing to the totals
            "award_count": int(bucket.get("doc_count", 0)),
            **{
                column: get_summed_value_as_float(bucket, self.sum_column_mapping[column])
                for column in self.sum_column_mapping
            },
        }
