from abc import abstractmethod
from typing import List, Optional, Dict

from django.conf import settings
from django.contrib.postgres.fields import ArrayField
from django.db.models import Sum, Count, TextField, Q
from django.db.models.functions import Cast
from django.utils.functional import cached_property
from elasticsearch_dsl import Q as ES_Q, A
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.awards.models import CovidFinancialAccountMatview
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.data_classes import Pagination
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.exceptions import ForbiddenException
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase, _BasePaginationMixin
from usaspending_api.search.v2.elasticsearch_helper import (
    get_scaled_sum_aggregations,
    get_number_of_unique_terms_for_awards,
    get_summed_value_as_float,
)


class ElasticsearchSpendingPaginationMixin(_BasePaginationMixin):
    sum_column_mapping = {"obligation": "total_covid_obligation", "outlay": "total_covid_outlay"}
    sort_column_mapping = {
        "award_count": "_count",
        "description": "_key",  # _key will ultimately sort on description value
        "code": "_key",  # Façade sort behavior, really sorting on description
        "id": "_key",  # Façade sort behavior, really sorting on description
        **sum_column_mapping,
    }

    @cached_property
    def pagination(self):
        return self.run_models(list(self.sort_column_mapping), default_sort_column="id")


class ElasticsearchLoansPaginationMixin(_BasePaginationMixin):
    sum_column_mapping = {
        "obligation": "total_covid_obligation",
        "outlay": "total_covid_outlay",
        "face_value_of_loan": "total_loan_value",
    }
    sort_column_mapping = {
        "award_count": "_count",
        "description": "_key",  # _key will ultimately sort on description value
        "code": "_key",  # Façade sort behavior, really sorting on description
        "id": "_key",  # Façade sort behavior, really sorting on description
        **sum_column_mapping,
    }

    @cached_property
    def pagination(self):
        return self.run_models(list(self.sort_column_mapping), default_sort_column="id")


class ElasticsearchDisasterBase(DisasterBase):

    query_fields: List[str]
    agg_key: str
    agg_group_name: str = "group_by_agg_key"  # name used for the tier-1 aggregation group
    sub_agg_key: str = None  # will drive including of a sub-bucket-aggregation if overridden by subclasses
    sub_agg_group_name: str = "sub_group_by_sub_agg_key"  # name used for the tier-2 aggregation group

    filter_query: ES_Q
    bucket_count: int

    pagination: Pagination  # Overwritten by a pagination mixin
    sort_column_mapping: Dict[str, str]  # Overwritten by a pagination mixin
    sum_column_mapping: Dict[str, str]  # Overwritten by a pagination mixin

    @cache_response()
    def post(self, request: Request) -> Response:
        # Need to update the value of "query" to have the fields to search on
        query = self.filters.pop("query", None)
        if query:
            self.filters["query"] = {"text": query, "fields": self.query_fields}
        self.filter_query = QueryWithFilters.generate_awards_elasticsearch_query(self.filters)

        # Ensure that only non-zero values are taken into consideration
        # TODO: Refactor to use new NonzeroFields filter in QueryWithFilters
        non_zero_queries = []
        for field in self.sum_column_mapping.values():
            non_zero_queries.append(ES_Q("range", **{field: {"gt": 0}}))
            non_zero_queries.append(ES_Q("range", **{field: {"lt": 0}}))
        self.filter_query.must.append(ES_Q("bool", should=non_zero_queries, minimum_should_match=1))

        self.bucket_count = get_number_of_unique_terms_for_awards(
            self.filter_query, f"{self.agg_key.replace('.keyword', '')}.hash"
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

    @abstractmethod
    def build_elasticsearch_result(self, info_buckets: List[dict]) -> List[dict]:
        pass

    def build_elasticsearch_search_with_aggregations(self) -> Optional[AwardSearch]:
        """
        Using the provided ES_Q object creates an AwardSearch object with the necessary applied aggregations.
        """
        # Create the initial search using filters
        search = AwardSearch().filter(self.filter_query)

        # As of writing this the value of settings.ES_ROUTING_FIELD is the only high cardinality aggregation that
        # we support. Since the Elasticsearch clusters are routed by this field we don't care to get a count of
        # unique buckets, but instead we use the upper_limit and don't allow an upper_limit > 10k.
        if self.bucket_count == 0:
            return None
        elif self.agg_key == settings.ES_ROUTING_FIELD:
            size = self.bucket_count
            shard_size = size
            group_by_agg_key_values = {
                "order": [
                    {self.sort_column_mapping[self.pagination.sort_key]: self.pagination.sort_order},
                    {self.sort_column_mapping["id"]: self.pagination.sort_order},
                ]
            }
            bucket_sort_values = None
        else:
            size = self.bucket_count
            shard_size = self.bucket_count + 100
            group_by_agg_key_values = {}
            bucket_sort_values = {
                "sort": [
                    {self.sort_column_mapping[self.pagination.sort_key]: {"order": self.pagination.sort_order}},
                    {self.sort_column_mapping["id"]: {"order": self.pagination.sort_order}},
                ]
            }

        if shard_size > 10000:
            raise ForbiddenException(
                "Current filters return too many unique items. Narrow filters to return results or use downloads."
            )

        # Define all aggregations needed to build the response
        group_by_agg_key_values.update({"field": self.agg_key, "size": size, "shard_size": shard_size})
        group_by_agg_key = A("terms", **group_by_agg_key_values)

        sum_aggregations = {
            mapping: get_scaled_sum_aggregations(mapping, self.pagination)
            for mapping in self.sum_column_mapping.values()
        }

        search.aggs.bucket(self.agg_group_name, group_by_agg_key)
        for field, sum_aggregations in sum_aggregations.items():
            search.aggs[self.agg_group_name].metric(field, sum_aggregations["sum_field"])

        if bucket_sort_values:
            bucket_sort_aggregation = A("bucket_sort", **bucket_sort_values)
            search.aggs[self.agg_group_name].pipeline("pagination_aggregation", bucket_sort_aggregation)

        # If provided, break down primary bucket aggregation into sub-aggregations based on a sub_agg_key
        if self.sub_agg_key:
            self.extend_elasticsearch_search_with_sub_aggregation(search)

        # Set size to 0 since we don't care about documents returned
        search.update_from_dict({"size": 0})

        return search

    def extend_elasticsearch_search_with_sub_aggregation(self, search: AwardSearch):
        """
        This template method is called if the `self.sub_agg_key` is supplied, in order to post-process the query and
        inject a sub-aggregation on a secondary dimension (that is subordinate to the first agg_key's dimension).

        Example: Subtier Agency spending rolled up to Toptier Agency spending
        """
        sub_bucket_count = get_number_of_unique_terms_for_awards(self.filter_query, f"{self.sub_agg_key}.hash")
        size = sub_bucket_count
        shard_size = sub_bucket_count + 100
        sub_group_by_sub_agg_key_values = {}

        if shard_size > 10000:
            raise ForbiddenException(
                "Current filters return too many unique items. Narrow filters to return results or use downloads."
            )

        # Sub-aggregation to append to primary agg
        sub_group_by_sub_agg_key_values.update(
            {
                "field": self.sub_agg_key,
                "size": size,
                "shard_size": shard_size,
                "order": [
                    {self.sort_column_mapping[self.pagination.sort_key]: self.pagination.sort_order},
                    {self.sort_column_mapping["id"]: self.pagination.sort_order},
                ],
            }
        )
        sub_group_by_sub_agg_key = A("terms", **sub_group_by_sub_agg_key_values)

        sum_aggregations = {
            mapping: get_scaled_sum_aggregations(mapping) for mapping in self.sum_column_mapping.values()
        }

        # Append sub-agg to primary agg, and include the sub-agg's sum metric aggs too
        search.aggs[self.agg_group_name].bucket(self.sub_agg_group_name, sub_group_by_sub_agg_key)
        for field, sum_aggregations in sum_aggregations.items():
            search.aggs[self.agg_group_name].aggs[self.sub_agg_group_name].metric(field, sum_aggregations["sum_field"])

    def build_totals(self, response: List[dict]) -> dict:
        # Need to use a Postgres in this case since we only look at the first 10k results for Elasticsearch.
        # Since the endpoint is performing aggregations on the entire matview with no grouping or joins
        # the query takes minimal time to complete.
        if self.agg_key == settings.ES_ROUTING_FIELD:
            annotations = {"cast_def_codes": Cast("def_codes", ArrayField(TextField()))}
            filters = [
                Q(cast_def_codes__overlap=self.def_codes),
                self.has_award_of_provided_type(should_join_awards=False),
            ]
            aggregations = {
                "face_value_of_loan": Sum("total_loan_value"),
                "obligation": Sum("obligation"),
                "outlay": Sum("outlay"),
            }
            aggregations = {col: aggregations[col] for col in self.sum_column_mapping.keys()}
            aggregations["award_count"] = Count("award_id")

            if self.filters.get("query"):
                filters.append(Q(recipient_name__icontains=self.filters["query"]["text"]))

            totals = (
                CovidFinancialAccountMatview.objects.annotate(**annotations)
                .filter(*filters)
                .values()
                .aggregate(**aggregations)
            )
            return totals

        totals = {key: 0 for key in self.sum_column_mapping.keys()}
        award_count = 0

        for bucket in response:
            for key in totals.keys():
                totals[key] += get_summed_value_as_float(bucket, self.sum_column_mapping[key])
            award_count += int(bucket.get("doc_count", 0))

        totals["award_count"] = award_count

        return totals

    def query_elasticsearch(self) -> dict:
        search = self.build_elasticsearch_search_with_aggregations()
        if search is None:
            totals = self.build_totals(response=[])
            return {"totals": totals, "results": []}

        response = search.handle_execute()
        response = response.aggs.to_dict()
        buckets = response.get("group_by_agg_key", {}).get("buckets", [])

        totals = self.build_totals(buckets)

        results = self.build_elasticsearch_result(buckets[self.pagination.lower_limit : self.pagination.upper_limit])

        return {"totals": totals, "results": results}
