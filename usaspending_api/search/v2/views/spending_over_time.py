import copy
import logging

from calendar import monthrange
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Tuple

from django.conf import settings
from django.db.models import Sum, F
from elasticsearch_dsl import A
from elasticsearch_dsl.response import AggResponse
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod
from usaspending_api.common.helpers.fiscal_year_helpers import (
    bolster_missing_time_periods,
    generate_date_range,
    generate_fiscal_month,
    generate_fiscal_year,
)
from usaspending_api.common.helpers.generic_helper import (
    get_generic_filters_message,
    min_and_max_from_date_ranges,
)
from usaspending_api.common.helpers.orm_helpers import FiscalMonth, FiscalQuarter
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER_W_FILTERS
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.filters.elasticsearch.filter import _QueryType
from usaspending_api.search.filters.time_period.query_types import TransactionSearchTimePeriod


logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION
GROUPING_LOOKUP = {
    "calendar_year": "calendar_year",
    "cy": "calendar_year",
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
    def validate_request_data(json_data: dict) -> Tuple[dict, dict]:
        program_activities_rule = [
            {
                "name": "program_activities",
                "type": "array",
                "key": "filters|program_activities",
                "object_keys_min": 1,
                "array_type": "object",
                "object_keys": {
                    "name": {"type": "text", "text_type": "search"},
                    "code": {
                        "type": "integer",
                    },
                },
            }
        ]
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
        models.extend(copy.deepcopy(AWARD_FILTER_W_FILTERS))
        models.extend(copy.deepcopy(program_activities_rule))
        models.extend(copy.deepcopy(PAGINATION))
        tiny_shield = TinyShield(models)
        validated_data = tiny_shield.block(json_data)
        if "filters" in validated_data and "program_activities" in validated_data["filters"]:
            tiny_shield.enforce_object_keys_min(validated_data, program_activities_rule[0])

        if validated_data.get("filters", None) is None:
            raise InvalidParameterException("Missing request parameters: filters")

        return validated_data, models

    def database_data_layer_for_subawards(self) -> tuple:
        queryset = subaward_filter(self.filters)
        obligation_column = "subaward_amount"

        # Note: SubawardSearch already has an "fy" field that corresponds to the prime award's fiscal year.
        #       This, however, needs "fy" to be the fiscal year of the sub_action_date (i.e. "sub_fiscal_year").
        #       And so, Django gets confused simply aliasing it to "fy" as "fy" is already a field in the model.
        #       To get around that, we're doing this little dance of annotate() and values().
        queryset = queryset.annotate(prime_fy=F("fy"))

        month_quarter_cols = []
        if self.group == "month":
            queryset = queryset.annotate(month=FiscalMonth("sub_action_date"))
            month_quarter_cols.append("month")
        elif self.group == "quarter":
            queryset = queryset.annotate(quarter=FiscalQuarter("sub_action_date"))
            month_quarter_cols.append("quarter")

        first_values = ["sub_fiscal_year"] + month_quarter_cols
        second_values = ["aggregated_amount"] + month_quarter_cols
        second_values_dict = {"fy": F("sub_fiscal_year")}
        order_by_cols = ["fy"] + month_quarter_cols
        queryset = (
            queryset.values(*first_values)
            .annotate(aggregated_amount=Sum(obligation_column))
            .values(*second_values, **second_values_dict)
            .order_by(*order_by_cols)
        )

        return queryset, order_by_cols

    def apply_elasticsearch_aggregations(self, search: TransactionSearch) -> None:
        """
        Takes in an instance of the elasticsearch-dsl.Search object and applies the necessary
        aggregations in a specific order to get expected results.
        """
        interval = "year" if (self.group == "fiscal_year" or self.group == "calendar_year") else self.group

        """
        The individual aggregations that are needed; with two different sum aggregations to handle issues with
        summing together floats.
        """
        field = "action_date" if self.group == "calendar_year" else "fiscal_action_date"
        group_by_time_period_agg = A("date_histogram", field=field, interval=interval, format="yyyy-MM-dd")
        sum_as_cents_agg = A("sum", field="generated_pragmatic_obligation", script={"source": "_value * 100"})
        sum_as_dollars_agg = A(
            "bucket_script", buckets_path={"sum_as_cents": "sum_as_cents"}, script="params.sum_as_cents / 100"
        )

        """
        Putting the aggregations together; in order for the aggregations to the correct structure they
        unfortunately need to be one after the other. This allows for nested aggregations as opposed to sibling.
        We also add another aggregation breakdown where inside the "group_by_time_period" bucket, we further group
        by Category, which returns us buckets talling up the award data for a given award type
        """
        search.aggs.bucket("group_by_time_period", group_by_time_period_agg)
        search.aggs["group_by_time_period"].bucket("group_by_category", "terms", field="award_category").metric(
            "sum_as_cents", sum_as_cents_agg
        ).pipeline("sum_as_dollars", sum_as_dollars_agg)

    def parse_elasticsearch_bucket(self, bucket: dict) -> dict:
        """
        Takes a dictionary representing one of the Elasticsearch buckets returned from the aggregation
        and returns a dictionary representation used in the API response.

        It should be noted that `key_as_string` is the name given by `date_histogram` to represent the key
        for each bucket which is a date as a string.

        Default time_period is set to "fiscal_year", however "quarter" and "month" also includes
        "fiscal_year" in the response object. When "calendar_year" is passed in as a group filter
        for the API, do not have to worry about any other time period.
        """
        time_period = {}
        key_as_date = datetime.strptime(bucket["key_as_string"], "%Y-%m-%d")
        time_period["calendar_year" if self.group == "calendar_year" else "fiscal_year"] = str(key_as_date.year)
        if self.group == "quarter":
            quarter = (key_as_date.month - 1) // 3 + 1
            time_period["quarter"] = str(quarter)
        elif self.group == "month":
            time_period["month"] = str(key_as_date.month)

        # The given time_period bucket contains buckets for the differnt categories, so extracting those.
        categories_breakdown = bucket["group_by_category"]["buckets"]

        # Initialize a dictionary to hold the query results for each obligation type.
        category_dictionary = {
            "Contract_Obligations": 0,
            "Direct_Obligations": 0,
            "Grant_Obligations": 0,
            "Idv_Obligations": 0,
            "Loan_Obligations": 0,
            "Other_Obligations": 0,
        }

        # Mapping of category keys to their corresponding obligation types.
        category_map = {
            "contract": "Contract_Obligations",
            "direct payment": "Direct_Obligations",
            "grant": "Grant_Obligations",
            "idv": "Idv_Obligations",
            "loans": "Loan_Obligations",
            "other": "Other_Obligations",
            "insurance": "Other_Obligations",
        }

        # Populate the category dictionary based on the award breakdown for a given bucket.
        for category in categories_breakdown:
            key = category["key"]
            if key in category_map:
                category_dictionary[category_map[key]] += category.get("sum_as_dollars", {"value": 0})["value"]

        aggregated_amount = sum(category_dictionary.values())
        response_object = {
            "aggregated_amount": aggregated_amount,
            "time_period": time_period,
            **category_dictionary,
        }

        return response_object

    def build_elasticsearch_result(self, agg_response: AggResponse, time_periods: list) -> list:
        """
        In this function we are just taking the elasticsearch aggregate response and looping through the
        buckets to create a results object for each time interval.

        Using a min_date, max_date, and a frequency indicator generates either a list of dictionaries
        containing fiscal year information (fiscal year, fiscal quarter, and fiscal month) or a list
        of dictionaries containing calendar year information (calendar year). The following are the format
        of date_range based on the frequency:
            * "calendar_year" returns a list of dictionaries containing {calendar year}
            * "fiscal_year" returns list of dictionaries containing {fiscal year}
            * "quarter" returns a list of dictionaries containing {fiscal year and quarter}
            * "month" returns a list of dictionaries containg {fiscal year and month}
        NOTE the generate_date_range() can also generate non fiscal date range (calendar ranges) as well.
        """

        results = []
        min_date, max_date = min_and_max_from_date_ranges(time_periods)

        date_range = generate_date_range(min_date, max_date, self.group)
        date_buckets = agg_response.group_by_time_period.buckets
        parsed_bucket = None

        for fiscal_date in date_range:
            if date_buckets and parsed_bucket is None:
                parsed_bucket = self.parse_elasticsearch_bucket(date_buckets.pop(0))

            if self.group == "calendar_year":
                time_period = {"calendar_year": str(fiscal_date["calendar_year"])}
            else:
                time_period = {"fiscal_year": str(fiscal_date["fiscal_year"])}

            if self.group == "quarter":
                time_period["quarter"] = str(fiscal_date["fiscal_quarter"])
            elif self.group == "month":
                time_period["month"] = str(fiscal_date["fiscal_month"])

            if parsed_bucket is not None and time_period == parsed_bucket["time_period"]:
                results.append(parsed_bucket)
                parsed_bucket = None
            else:
                results.append(
                    {
                        "aggregated_amount": 0,
                        "time_period": time_period,
                        "Contract_Obligations": 0,
                        "Direct_Obligations": 0,
                        "Grant_Obligations": 0,
                        "Idv_Obligations": 0,
                        "Loan_Obligations": 0,
                        "Other_Obligations": 0,
                    }
                )

        return results

    def query_elasticsearch_for_prime_awards(self, time_periods: list) -> list:
        filter_options = {}
        time_period_obj = TransactionSearchTimePeriod(
            default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
        )
        new_awards_only_decorator = NewAwardsOnlyTimePeriod(
            time_period_obj=time_period_obj, query_type=_QueryType.TRANSACTIONS
        )
        filter_options["time_period_obj"] = new_awards_only_decorator

        filter_query = QueryWithFilters.generate_transactions_elasticsearch_query(self.filters, **filter_options)
        search = TransactionSearch().filter(filter_query)
        self.apply_elasticsearch_aggregations(search)
        response = search.handle_execute()
        overall_results = self.build_elasticsearch_result(response.aggs, time_periods)

        return overall_results

    @cache_response()
    def post(self, request: Request) -> Response:
        self.original_filters = request.data.get("filters")
        json_request, models = self.validate_request_data(request.data)
        self.group = GROUPING_LOOKUP[json_request["group"]]
        self.subawards = json_request["subawards"]
        self.filters = json_request["filters"]

        # time_period is optional so we're setting a default window from API_SEARCH_MIN_DATE to end of the current FY.
        # Otherwise, users will see blank results for years
        current_fy = generate_fiscal_year(datetime.now(timezone.utc))
        if self.group == "fiscal_year":
            end_date = "{}-09-30".format(current_fy)
        elif self.group == "calendar_year":
            date = datetime.now(timezone.utc)  # current date
            end_date = f"{date.year}-12-31"  # last day of current year
        else:
            current_fiscal_month = generate_fiscal_month(datetime.now(timezone.utc))
            days_in_month = monthrange(current_fy, current_fiscal_month)[1]
            end_date = f"{current_fy}-{current_fiscal_month}-{days_in_month}"

        default_time_period = {"start_date": settings.API_SEARCH_MIN_DATE, "end_date": end_date}

        # if time periods have been passed in use those, otherwise use the one calculated above
        time_periods = self.filters.get("time_period", [default_time_period])

        if self.subawards:
            db_results, order_by_cols = self.database_data_layer_for_subawards()
            results = bolster_missing_time_periods(
                filter_time_periods=time_periods,
                queryset=db_results,
                date_range_type=order_by_cols[-1],
                columns={"aggregated_amount": "aggregated_amount"},
            )
        else:
            results = self.query_elasticsearch_for_prime_awards(time_periods)

        raw_response = OrderedDict(
            [
                ("group", self.group),
                ("results", results),
                (
                    "messages",
                    get_generic_filters_message(self.original_filters.keys(), [elem["name"] for elem in models]),
                ),
            ]
        )

        return Response(raw_response)
