import copy
import logging
import pandas as pd
from calendar import monthrange
from collections import OrderedDict
from datetime import datetime, timezone

from django.conf import settings
from django.db.models import Sum
from elasticsearch_dsl import A, Search
from elasticsearch_dsl.response import AggResponse
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_over_time
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.experimental_api_flags import is_experimental_elasticsearch_api
from usaspending_api.common.helpers.orm_helpers import FiscalMonth, FiscalQuarter, FiscalYear
from usaspending_api.common.helpers.generic_helper import (
    bolster_missing_time_periods,
    generate_fiscal_year,
    min_and_max_from_date_ranges,
    generate_fiscal_date_range,
)
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.v2.elasticsearch_helper import get_base_search_with_filters

logger = logging.getLogger("console")

API_VERSION = settings.API_VERSION
GROUPING_LOOKUP = {
    "quarter": "quarter",
    "q": "quarter",
    "fiscal_year": "fiscal_year",
    "fy": "fiscal_year",
    "month": "month",
    "m": "month",
}


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingOverTimeVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by time. The amount of time is denoted by the "group" value.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_over_time.md"

    @staticmethod
    def validate_request_data(json_data):
        models = [
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
            {
                "name": "group",
                "key": "group",
                "type": "enum",
                "enum_values": list(GROUPING_LOOKUP.keys()),
                "default": "fy",
                "optional": False,  # allow to be optional in the future
            },
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        validated_data = TinyShield(models).block(json_data)

        if validated_data.get("filters", None) is None:
            raise InvalidParameterException("Missing request parameters: filters")

        return validated_data

    def database_data_layer(self):
        if self.subawards:
            queryset = subaward_filter(self.filters)
            obligation_column = "amount"
        else:
            queryset = spending_over_time(self.filters)
            obligation_column = "generated_pragmatic_obligation"

        values = ["fy"]
        if self.group == "month":
            queryset = queryset.annotate(month=FiscalMonth("action_date"), fy=FiscalYear("action_date"))
            values.append("month")

        elif self.group == "quarter":
            queryset = queryset.annotate(quarter=FiscalQuarter("action_date"), fy=FiscalYear("action_date"))
            values.append("quarter")

        elif self.group == "fiscal_year":
            queryset = queryset.annotate(fy=FiscalYear("action_date"))

        queryset = (
            queryset.values(*values)
            .annotate(aggregated_amount=Sum(obligation_column))
            .order_by(*["{}".format(value) for value in values])
        )

        return queryset, values

    def apply_elasticsearch_aggregations(self, search: Search) -> None:
        """
        Takes in an instance of the elasticsearch-dsl.Search object and applies the necessary
        aggregations in a specific order to get expected results.
        """
        interval = "year" if self.group == "fiscal_year" else self.group

        # The individual aggregations that are needed; with two different sum aggregations to handle issues with
        # summing together floats.
        group_by_time_period_agg = A(
            "date_histogram", field="fiscal_action_date", interval=interval, format="yyyy-MM-dd"
        )
        sum_as_cents_agg = A(
            "sum", script={"lang": "painless", "source": f"doc['generated_pragmatic_obligation'].value * 100"}
        )
        sum_as_dollars_agg = A(
            "bucket_script", buckets_path={"sum_as_cents": "sum_as_cents"}, script="params.sum_as_cents / 100"
        )

        # Putting the aggregations together; in order for the aggregations to the correct structure they
        # unfortunately need to be one after the other. This allows for nested aggregations as opposed to sibling.
        search.aggs.bucket("group_by_time_period", group_by_time_period_agg).metric(
            "sum_as_cents", sum_as_cents_agg
        ).pipeline("sum_as_dollars", sum_as_dollars_agg)

    def parse_elasticsearch_bucket(self, bucket):
        key_as_date = pd.to_datetime(bucket["key_as_string"], format="%Y-%m-%d")
        time_period = {"fiscal_year": str(key_as_date.year)}

        if self.group == "quarter":
            time_period["quarter"] = str(key_as_date.quarter)
        elif self.group == "month":
            time_period["month"] = str(key_as_date.month)

        aggregated_amount = bucket.get("sum_as_dollars", {"value": 0})["value"]
        return {"aggregated_amount": aggregated_amount, "time_period": time_period}

    def build_elasticsearch_result(self, agg_response: AggResponse, time_periods: list) -> list:
        results = []
        min_date, max_date = min_and_max_from_date_ranges(time_periods)
        date_range = generate_fiscal_date_range(min_date, max_date, self.group)
        date_buckets = agg_response.group_by_time_period.buckets
        parsed_bucket = None

        for date in date_range:
            if date_buckets and parsed_bucket is None:
                parsed_bucket = self.parse_elasticsearch_bucket(date_buckets.pop(0))

            time_period = {"fiscal_year": str(date.year)}
            if self.group == "quarter":
                time_period["quarter"] = str(date.quarter)
            elif self.group == "month":
                time_period["month"] = str(date.month)

            if parsed_bucket is not None and time_period == parsed_bucket["time_period"]:
                results.append(parsed_bucket)
                parsed_bucket = None
            else:
                results.append({"aggregated_amount": 0, "time_period": time_period})

        return results

    def query_elasticsearch(self, time_periods: list) -> list:
        search = get_base_search_with_filters(
            index_name="{}*".format(settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX), filters=self.filters
        )
        self.apply_elasticsearch_aggregations(search)
        response = search.execute()
        return self.build_elasticsearch_result(response.aggs, time_periods)

    @cache_response()
    def post(self, request: Request) -> Response:
        json_request = self.validate_request_data(request.data)
        self.group = GROUPING_LOOKUP[json_request["group"]]
        self.subawards = json_request["subawards"]
        self.filters = json_request["filters"]
        self.elasticsearch = is_experimental_elasticsearch_api(request)

        # time_period is optional so we're setting a default window from API_SEARCH_MIN_DATE to end of the current FY.
        # Otherwise, users will see blank results for years
        current_fy = generate_fiscal_year(datetime.now(timezone.utc))
        if self.group == "fiscal_year":
            end_date = "{}-09-30".format(current_fy)
        else:
            current_month = datetime.now(timezone.utc).month
            days_in_month = monthrange(current_fy, current_month)[1]
            end_date = f"{current_fy}-{current_month}-{days_in_month}"

        default_time_period = {"start_date": settings.API_SEARCH_MIN_DATE, "end_date": end_date}
        time_periods = self.filters.get("time_period", [default_time_period])

        if self.elasticsearch and not self.subawards:
            logger.info("Using experimental Elasticsearch functionality for 'spending_over_time'")
            results = self.query_elasticsearch(time_periods)
        else:
            db_results, values = self.database_data_layer()
            results = bolster_missing_time_periods(
                filter_time_periods=time_periods,
                queryset=db_results,
                date_range_type=values[-1],
                columns={"aggregated_amount": "aggregated_amount"},
            )

        return Response(OrderedDict([("group", self.group), ("results", results)]))
