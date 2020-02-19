from abc import ABCMeta
from typing import List

from django.db.models import QuerySet
from elasticsearch_dsl import A, Q as ES_Q

from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.search.v2.elasticsearch_helper import get_sum_aggregations
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import (
    BaseSpendingByCategoryViewSet,
)
from usaspending_api.common.helpers.api_helper import alias_response
from usaspending_api.search.helpers.spending_by_category_helpers import fetch_cfda_id_title_by_number
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import Category


ALIAS_DICT = {
    "cfda_number": "code",
    # Note: we could pull cfda title from the matviews but noticed the titles vary for the same cfda number
    #       which leads to incorrect groupings
    # 'cfda_title': 'name'
}


class CFDAView(BaseSpendingByCategoryViewSet, metaclass=ABCMeta):
    """
    Base class used by the different Awarding / Funding Agencies and Subagencies
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_category/cfda.md"
    category = Category(name="cfda", primary_field="cfda_number")

    def build_elasticsearch_search_with_aggregations(
        self, filter_query: ES_Q, curr_partition: int, num_partitions: int, size: int
    ) -> TransactionSearch:
        print(f"\n\n>>>>>>>>>>>>>we got here<<<<<<<<<<<<<")
        # Create filtered Search object
        search = TransactionSearch().filter(filter_query)

        # Define all aggregations needed to build the response
        group_by_cfda_id = A(
            "terms",
            field=f"cfda_id",
            include={"partition": curr_partition, "num_partitions": num_partitions},
            size=size,
        )

        sum_aggregations = get_sum_aggregations("generated_pragmatic_obligation", self.pagination)
        sum_as_cents = sum_aggregations["sum_as_cents"]
        sum_as_dollars = sum_aggregations["sum_as_dollars"]
        sum_bucket_sort = sum_aggregations["sum_bucket_sort"]

        # Apply the aggregations to TransactionSearch object
        search.aggs.bucket("group_by_cfda_id", group_by_cfda_id)
        search.aggs["group_by_cfda_id"].metric("sum_as_cents", sum_as_cents).pipeline(
            "sum_as_dollars", sum_as_dollars
        ).pipeline("sum_bucket_sort", sum_bucket_sort)

        return search

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        cfda_buckets = response.get("group_by_cfda_id", {}).get("buckets", [])
        for bucket in cfda_buckets:
            results.append(
                {
                    "amount": bucket.get("sum_as_dollars", {"value": 0})["value"],
                    "code": "dummy",
                    "id": 0,
                    "name": bucket.get("key"),
                }
            )
        return results

    def query_django(self, base_queryset: QuerySet):
        django_filters = {"{}__isnull".format(self.obligation_column): False, "cfda_number__isnull": False}
        django_values = ["cfda_number"]
        queryset = self.common_db_query(base_queryset, django_filters, django_values)

        lower_limit = self.pagination.lower_limit
        upper_limit = self.pagination.upper_limit
        query_results = list(queryset[lower_limit:upper_limit])
        results = alias_response(ALIAS_DICT, query_results)
        for row in results:
            row["id"], row["name"] = fetch_cfda_id_title_by_number(row["code"])

        return query_results
