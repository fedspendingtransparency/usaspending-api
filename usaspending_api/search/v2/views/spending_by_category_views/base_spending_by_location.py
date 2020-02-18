from abc import ABCMeta
from enum import Enum
from typing import List

from django.db.models import QuerySet, Sum
from elasticsearch_dsl import A, Q as ES_Q

from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.helpers.api_helper import alias_response
from usaspending_api.search.helpers.spending_by_category_helpers import (
    fetch_country_name_from_code,
    fetch_state_name_from_code,
)
from usaspending_api.search.v2.elasticsearch_helper import get_sum_aggregations
from usaspending_api.search.v2.views.spending_by_category_views.base_spending_by_category import (
    BaseSpendingByCategoryViewSet,
)


class LocationType(Enum):
    COUNTY = "county"
    DISTRICT = "congressional"
    COUNTRY = "country"
    STATE = "state"


class BaseLocationViewSet(BaseSpendingByCategoryViewSet, metaclass=ABCMeta):
    """Base class used by the different Location categories for Spending By Category"""

    location_type: LocationType

    def build_elasticsearch_search_with_aggregations(
        self, filter_query: ES_Q, curr_partition: int, num_partitions: int, size: int
    ) -> TransactionSearch:
        # Create filtered Search object
        search = TransactionSearch().filter(filter_query)

        # Define all aggregations needed to build the response
        group_by_location = A(
            "terms",
            field=f"pop_{self.location_type.value}_code",
            include={"partition": curr_partition, "num_partitions": num_partitions},
            size=size,
        )

        sum_aggregations = get_sum_aggregations("generated_pragmatic_obligation", self.pagination)

        sum_as_cents = sum_aggregations["sum_as_cents"]
        sum_as_dollars = sum_aggregations["sum_as_dollars"]
        sum_bucket_sort = sum_aggregations["sum_bucket_sort"]

        # Apply the aggregations to TransactionSearch object
        search.aggs.bucket("group_by_location", group_by_location)
        search.aggs["group_by_location"].metric("sum_as_cents", sum_as_cents).pipeline(
            "sum_as_dollars", sum_as_dollars
        ).pipeline("sum_bucket_sort", sum_bucket_sort)

        return search

    def build_elasticsearch_result(self, response: dict) -> List[dict]:
        results = []
        country_code_buckets = response.get("group_by_location", {}).get("buckets", [])
        for bucket in country_code_buckets:
            results.append(
                {
                    "amount": bucket.get("sum_as_dollars", {"value": 0})["value"],
                    "code": bucket.get("key"),
                    "id": None,
                    # we're hitting the DB here because otherwise we have no guarantee that the transactions will have the country_name filled in
                    "name": fetch_country_name_from_code(bucket.get("key")),
                }
            )
        return results

    def query_django(self, base_queryset: QuerySet):
        filters = {}
        values = {}
        if self.category.name == "county":
            filters = {"pop_county_code__isnull": False}
            values = ["pop_county_code", "pop_county_name"]
        elif self.category.name == "district":
            filters = {"pop_congressional_code__isnull": False}
            values = ["pop_congressional_code", "pop_state_code"]
        elif self.category.name == "country":
            filters = {"pop_country_code__isnull": False}
            values = ["pop_country_code"]
        elif self.category.name == "state_territory":
            filters = {"pop_state_code__isnull": False}
            values = ["pop_state_code"]

        queryset = (
            base_queryset.filter(**filters)
            .values(*values)
            .annotate(amount=Sum(self.obligation_column))
            .order_by("-amount")
        )
        code_lookup = {
            "county": {"pop_county_code": "code", "pop_county_name": "name"},
            "district": {"pop_congressional_code": "code"},
            "state_territory": {"pop_state_code": "code"},
            "country": {"pop_country_code": "code"},
        }
        # DB hit here
        query_results = list(queryset[self.pagination.lower_limit : self.pagination.upper_limit])
        results = alias_response(code_lookup[self.category.name], query_results)
        for row in results:
            row["id"] = None
            if self.category.name == "district":
                cd_code = row["code"]
                if cd_code == "90":  # 90 = multiple districts
                    cd_code = "MULTIPLE DISTRICTS"

                row["name"] = "{}-{}".format(row["pop_state_code"], cd_code)
                del row["pop_state_code"]
            if self.category.name == "country":
                row["name"] = fetch_country_name_from_code(row["code"])
            if self.category.name == "state_territory":
                row["name"] = fetch_state_name_from_code(row["code"])

        return results
