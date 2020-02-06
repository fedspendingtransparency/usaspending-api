from typing import List

from django.db.models import QuerySet, F
from elasticsearch_dsl import A, Q as ES_Q

from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.search.helpers.spending_by_category_helpers import fetch_agency_tier_id_by_agency
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import (
    BaseSpendingByCategoryViewSet,
    Category,
)


class AwardingSubagencyViewSet(BaseSpendingByCategoryViewSet):
    """
    This route takes award filters, and returns spending by awarding subagencies.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/awarding_subagency.md"
    category = Category(name="awarding_subagency", primary_field="awarding_subtier_agency_name.keyword")

    def build_elasticsearch_search_with_aggregations(
        self, filter_query: ES_Q, curr_partition: int, num_partitions: int, size: int
    ) -> TransactionSearch:
        # Create filtered Search object
        search = TransactionSearch().filter(filter_query)

        # Define all aggregations needed to build the response
        group_by_agency_name = A(
            "terms",
            field="awarding_subtier_agency_name.keyword",
            include={"partition": curr_partition, "num_partitions": num_partitions},
            size=size,
        )
        group_by_agency_abbreviation = A("terms", field="awarding_subtier_agency_abbreviation.keyword")
        group_by_agency_id = A("terms", field="awarding_subtier_agency_id")
        sum_as_cents = A("sum", field="generated_pragmatic_obligation", script={"source": "_value * 100"})
        sum_as_dollars = A(
            "bucket_script", buckets_path={"sum_as_cents": "sum_as_cents"}, script="params.sum_as_cents / 100"
        )

        # Have to create a separate dictionary for the bucket_sort values since "from" is a reserved word
        bucket_sort_values = {
            "sort": {"sum_as_dollars": {"order": "desc"}},
            "from": (self.pagination.page - 1) * self.pagination.limit,
            "size": self.pagination.limit + 1,
        }
        sum_bucket_sort = A("bucket_sort", **bucket_sort_values)

        # Apply the aggregations to TransactionSearch object
        search.aggs.bucket("group_by_agency_name", group_by_agency_name)
        search.aggs["group_by_agency_name"].bucket("group_by_agency_abbreviation", group_by_agency_abbreviation)
        search.aggs["group_by_agency_name"].bucket("group_by_agency_id", group_by_agency_id)
        search.aggs["group_by_agency_name"].metric("sum_as_cents", sum_as_cents).pipeline(
            "sum_as_dollars", sum_as_dollars
        ).pipeline("sum_bucket_sort", sum_bucket_sort)

        return search

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        agency_name_buckets = response.get("group_by_agency_name", {}).get("buckets", [])
        for bucket in agency_name_buckets:
            agency_abbreviation_buckets = bucket.get("group_by_agency_abbreviation", {}).get("buckets", [])
            agency_id_buckets = bucket.get("group_by_agency_id", {}).get("buckets", [])
            results.append(
                {
                    "amount": bucket.get("sum_as_dollars", {"value": 0})["value"],
                    "name": bucket.get("key"),
                    "code": agency_abbreviation_buckets[0].get("key") if len(agency_abbreviation_buckets) > 0 else None,
                    "id": int(agency_id_buckets[0].get("key")) if len(agency_id_buckets) > 0 else None,
                }
            )
        return results

    def query_django(self, base_queryset: QuerySet):
        django_filters = {"awarding_subtier_agency_name__isnull": False}
        django_values = ["awarding_subtier_agency_name", "awarding_subtier_agency_abbreviation"]
        queryset = self.common_db_query(base_queryset, django_filters, django_values).annotate(
            name=F("awarding_subtier_agency_name"), code=F("awarding_subtier_agency_abbreviation")
        )
        lower_limit = self.pagination.lower_limit
        upper_limit = self.pagination.upper_limit
        query_results = list(queryset[lower_limit:upper_limit])
        for row in query_results:
            row["id"] = fetch_agency_tier_id_by_agency(agency_name=row["name"], is_subtier=True)
            row.pop("awarding_subtier_agency_name")
            row.pop("awarding_subtier_agency_abbreviation")
        return query_results
