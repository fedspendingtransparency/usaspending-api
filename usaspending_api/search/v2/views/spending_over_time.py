import copy
import logging

from calendar import monthrange
from collections import OrderedDict
from datetime import datetime, timezone

from django.conf import settings
from django.db.models import Sum
from elasticsearch_dsl import A
from elasticsearch_dsl.response import AggResponse
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_over_time
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.experimental_api_flags import (
    is_experimental_elasticsearch_api,
    mirror_request_to_elasticsearch,
)
from usaspending_api.common.helpers.fiscal_year_helpers import (
    bolster_missing_time_periods,
    generate_fiscal_date_range,
    generate_fiscal_month,
    generate_fiscal_year,
)
from usaspending_api.common.helpers.generic_helper import (
    get_generic_filters_message,
    min_and_max_from_date_ranges,
)
from usaspending_api.common.helpers.orm_helpers import FiscalMonth, FiscalQuarter, FiscalYear
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield

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
    def validate_request_data(json_data: dict) -> dict:
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

    def database_data_layer(self) -> tuple:
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

    def apply_elasticsearch_aggregations(self, search: TransactionSearch) -> None:
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
        sum_as_cents_agg = A("sum", field="generated_pragmatic_obligation", script={"source": "_value * 100"})
        sum_as_dollars_agg = A(
            "bucket_script", buckets_path={"sum_as_cents": "sum_as_cents"}, script="params.sum_as_cents / 100"
        )

        # Putting the aggregations together; in order for the aggregations to the correct structure they
        # unfortunately need to be one after the other. This allows for nested aggregations as opposed to sibling.
        search.aggs.bucket("group_by_time_period", group_by_time_period_agg).metric(
            "sum_as_cents", sum_as_cents_agg
        ).pipeline("sum_as_dollars", sum_as_dollars_agg)

    def parse_elasticsearch_bucket(self, bucket: dict) -> dict:
        """
        Takes a dictionary representing one of the Elasticsearch buckets returned from the aggregation
        and returns a dictionary representation used in the API response.

        It should be noted that `key_as_string` is the name given by `date_histogram` to represent the key
        for each bucket which is a date as a string.
        """
        key_as_date = datetime.strptime(bucket["key_as_string"], "%Y-%m-%d")
        time_period = {"fiscal_year": str(key_as_date.year)}

        if self.group == "quarter":
            quarter = (key_as_date.month - 1) // 3 + 1
            time_period["quarter"] = str(quarter)
        elif self.group == "month":
            time_period["month"] = str(key_as_date.month)

        aggregated_amount = bucket.get("sum_as_dollars", {"value": 0})["value"]
        return {"aggregated_amount": aggregated_amount, "time_period": time_period}

    def build_elasticsearch_result(self, agg_response: AggResponse, time_periods: list) -> list:
        results = []
        min_date, max_date = min_and_max_from_date_ranges(time_periods)
        fiscal_date_range = generate_fiscal_date_range(min_date, max_date, self.group)
        date_buckets = agg_response.group_by_time_period.buckets
        parsed_bucket = None

        for fiscal_date in fiscal_date_range:
            if date_buckets and parsed_bucket is None:
                parsed_bucket = self.parse_elasticsearch_bucket(date_buckets.pop(0))

            time_period = {"fiscal_year": str(fiscal_date["fiscal_year"])}
            if self.group == "quarter":
                time_period["quarter"] = str(fiscal_date["fiscal_quarter"])
            elif self.group == "month":
                time_period["month"] = str(fiscal_date["fiscal_month"])

            if parsed_bucket is not None and time_period == parsed_bucket["time_period"]:
                results.append(parsed_bucket)
                parsed_bucket = None
            else:
                results.append({"aggregated_amount": 0, "time_period": time_period})

        return results

    def query_elasticsearch(self, time_periods: list) -> list:
        filter_query = QueryWithFilters.generate_transactions_elasticsearch_query(self.filters)
        search = TransactionSearch().filter(filter_query)
        self.apply_elasticsearch_aggregations(search)
        response = search.handle_execute()
        return self.build_elasticsearch_result(response.aggs, time_periods)

    @cache_response()
    def post(self, request: Request) -> Response:
        self.original_filters = request.data.get("filters")
        json_request = self.validate_request_data(request.data)
        self.group = GROUPING_LOOKUP[json_request["group"]]
        self.subawards = json_request["subawards"]
        self.filters = json_request["filters"]
        self.elasticsearch = is_experimental_elasticsearch_api(request)
        if not self.elasticsearch:
            mirror_request_to_elasticsearch(request)

        # time_period is optional so we're setting a default window from API_SEARCH_MIN_DATE to end of the current FY.
        # Otherwise, users will see blank results for years
        current_fy = generate_fiscal_year(datetime.now(timezone.utc))
        if self.group == "fiscal_year":
            end_date = "{}-09-30".format(current_fy)
        else:
            current_fiscal_month = generate_fiscal_month(datetime.now(timezone.utc))
            days_in_month = monthrange(current_fy, current_fiscal_month)[1]
            end_date = f"{current_fy}-{current_fiscal_month}-{days_in_month}"

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

        return Response(
            OrderedDict(
                [
                    ("group", self.group),
                    ("results", results),
                    (
                        "messages",
                        get_generic_filters_message(
                            self.original_filters.keys(), [elem["name"] for elem in AWARD_FILTER]
                        ),
                    ),
                ]
            )
        )
