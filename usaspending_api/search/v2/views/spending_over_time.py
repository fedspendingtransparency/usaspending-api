import copy
import logging
from calendar import monthrange
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Tuple, Optional
from dataclasses import dataclass, asdict

from django.conf import settings
from elasticsearch_dsl import A, Search
from elasticsearch_dsl.response import AggResponse
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.api_versioning import API_TRANSFORM_FUNCTIONS, api_transformations
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch, SubawardSearch, TransactionSearch
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.fiscal_year_helpers import (
    generate_date_range,
    generate_fiscal_month,
    generate_fiscal_year,
)
from usaspending_api.common.helpers.generic_helper import (
    get_generic_filters_message,
    min_and_max_from_date_ranges,
)
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER_W_FILTERS
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.filters.elasticsearch.filter import _QueryType
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod
from usaspending_api.search.filters.time_period.query_types import (
    AwardSearchTimePeriod,
    SubawardSearchTimePeriod,
    TransactionSearchTimePeriod,
)

from usaspending_api.search.v2.views.enums import SpendingLevel

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


@dataclass
class TimePeriod:
    start_date: str
    end_date: str
    date_type: Optional[str] = None


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
            {
                "name": "spending_level",
                "key": "spending_level",
                "type": "enum",
                "enum_values": [level.value for level in SpendingLevel],
                "optional": True,
                "default": "transactions",
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

    def subawards_group_by_time_period_agg(self) -> A:
        if self.group == "fiscal_year":
            return A("terms", field="sub_fiscal_year")
        else:
            return A(
                "date_histogram",
                field="sub_action_date",
                interval="year" if (self.group == "calendar_year") else self.group,
                format="yyyy-MM-dd",
            )

    def awards_group_by_time_period_agg(self) -> A:
        if self.group == "fiscal_year":
            return A("terms", field="fiscal_year")
        else:
            return A(
                "date_histogram",
                field="action_date",
                interval="year" if (self.group == "calendar_year") else self.group,
                format="yyyy-MM-dd",
            )

    def apply_elasticsearch_aggregations(self, search: Search) -> None:
        """
        Takes in an instance of the elasticsearch-dsl.Search object and applies the necessary
        aggregations in a specific order to get expected results.

        Args:
            search: An instance of the Elasticsearch Search object.
        """

        if isinstance(search, AwardSearch):
            category_field = "category.keyword"
            obligation_field = "generated_pragmatic_obligation"
            group_by_time_period_agg = self.awards_group_by_time_period_agg()
        elif isinstance(search, TransactionSearch):
            category_field = "award_category"
            obligation_field = "generated_pragmatic_obligation"

            group_by_time_period_agg = A(
                "date_histogram",
                field="action_date" if self.group == "calendar_year" else "fiscal_action_date",
                interval="year" if (self.group == "fiscal_year" or self.group == "calendar_year") else self.group,
                format="yyyy-MM-dd",
            )

        elif isinstance(search, SubawardSearch):
            category_field = "subaward_type.keyword"
            obligation_field = "subaward_amount"
            group_by_time_period_agg = self.subawards_group_by_time_period_agg()

        """
        The individual aggregations that are needed; with two different sum aggregations to handle issues with
        summing together floats.
        """
        sum_as_cents_agg_outlay = A("sum", field="total_outlays", script={"source": "_value * 100"})
        sum_as_dollars_agg_outlay = A(
            "bucket_script",
            buckets_path={"sum_as_cents_outlay": "sum_as_cents_outlay"},
            script="params.sum_as_cents_outlay / 100",
        )

        sum_as_cents_agg_obligation = A("sum", field=obligation_field, script={"source": "_value * 100"})
        sum_as_dollars_agg_obligation = A(
            "bucket_script",
            buckets_path={"sum_as_cents_obligation": "sum_as_cents_obligation"},
            script="params.sum_as_cents_obligation / 100",
        )

        """
        Putting the aggregations together; in order for the aggregations to the correct structure they
        unfortunately need to be one after the other. This allows for nested aggregations as opposed to sibling.
        We also add another aggregation breakdown where inside the "group_by_time_period" bucket, we further group
        by Category, which returns us buckets talling up the award data for a given award type
        """
        search.aggs.bucket("group_by_time_period", group_by_time_period_agg)
        search.aggs["group_by_time_period"].bucket("group_by_category", "terms", field=category_field).metric(
            "sum_as_cents_outlay", sum_as_cents_agg_outlay
        ).pipeline("sum_as_dollars_outlay", sum_as_dollars_agg_outlay).metric(
            "sum_as_cents_obligation", sum_as_cents_agg_obligation
        ).pipeline(
            "sum_as_dollars_obligation", sum_as_dollars_agg_obligation
        )

    def set_time_period(self, bucket: dict) -> dict:
        time_period = {}

        if self.group == "fiscal_year" and self.spending_level != SpendingLevel.TRANSACTION:
            key_as_date = datetime.strptime(str(bucket["key"]), "%Y")
        else:
            key_as_date = datetime.strptime(bucket["key_as_string"], "%Y-%m-%d")

        time_period["calendar_year" if self.group == "calendar_year" else "fiscal_year"] = str(key_as_date.year)
        if self.group == "quarter":
            quarter = (key_as_date.month - 1) // 3 + 1
            time_period["quarter"] = str(quarter)
        elif self.group == "month":
            time_period["month"] = str(key_as_date.month)

        return time_period

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
        time_period = self.set_time_period(bucket)

        # The given time_period bucket contains buckets for the differnt categories, so extracting those.
        categories_breakdown = bucket["group_by_category"]["buckets"]

        # Initialize a dictionary to hold the query results for each obligation type.
        if self.spending_level == SpendingLevel.SUBAWARD:
            obligation_dictionary = {
                "Contract_Obligations": 0,
                "Grant_Obligations": 0,
            }
            outlay_map = {
                "contract": "Contract_Outlays",
                "grant": "Grant_Outlays",
            }
        else:
            obligation_dictionary = {
                "Contract_Obligations": 0,
                "Direct_Obligations": 0,
                "Grant_Obligations": 0,
                "Idv_Obligations": 0,
                "Loan_Obligations": 0,
                "Other_Obligations": 0,
            }
            outlay_map = {
                "contract": "Contract_Outlays",
                "direct payment": "Direct_Outlays",
                "grant": "Grant_Outlays",
                "idv": "Idv_Outlays",
                "loans": "Loan_Outlays",
                "other": "Other_Outlays",
                "insurance": "Other_Outlays",
            }

        # Mapping of category keys to their corresponding obligation types.
        obligation_map = {
            "contract": "Contract_Obligations",
            "direct payment": "Direct_Obligations",
            "grant": "Grant_Obligations",
            "idv": "Idv_Obligations",
            "loans": "Loan_Obligations",
            "other": "Other_Obligations",
            "insurance": "Other_Obligations",
            "sub-contract": "Contract_Obligations",
            "sub-grant": "Grant_Obligations",
        }

        # Outlays are only supported on Awards
        outlay_dictionary = {v: 0 if self.spending_level == SpendingLevel.AWARD else None for v in outlay_map.values()}

        # Populate the category dictionary based on the award breakdown for a given bucket.
        for category in categories_breakdown:
            key = category["key"]
            if key in obligation_map:
                obligation_dictionary[obligation_map[key]] += category.get("sum_as_dollars_obligation", {"value": 0})[
                    "value"
                ]

            if self.spending_level == SpendingLevel.AWARD and key in outlay_map:
                outlay_dictionary[outlay_map[key]] += category.get("sum_as_dollars_outlay", {"value": 0})["value"]
        response_object = {
            "aggregated_amount": sum(obligation_dictionary.values()),
            "time_period": time_period,
            **obligation_dictionary,
            "total_outlays": sum(outlay_dictionary.values()) if self.spending_level == SpendingLevel.AWARD else None,
            **outlay_dictionary,
        }

        return response_object

    def build_elasticsearch_result_transactions(
        self, agg_response: AggResponse, time_periods: list[TimePeriod]
    ) -> list:
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
        min_date, max_date = min_and_max_from_date_ranges([asdict(time_period) for time_period in time_periods])

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
                        "total_outlays": None,
                        "Contract_Outlays": None,
                        "Direct_Outlays": None,
                        "Grant_Outlays": None,
                        "Idv_Outlays": None,
                        "Loan_Outlays": None,
                        "Other_Outlays": None,
                    }
                )

        return results

    def build_elasticsearch_result_awards_subawards(self, agg_response: AggResponse) -> list:
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
        date_buckets = agg_response.group_by_time_period.buckets
        parsed_bucket = None

        if date_buckets is not None:
            for bucket in date_buckets:
                parsed_bucket = self.parse_elasticsearch_bucket(bucket.to_dict())
                results.append(parsed_bucket)

        return results

    def set_default_for_subawards(self, time_periods: list[TimePeriod], overall_results: list) -> list:
        """if there is no data for that fiscal year, set default overall_results for that year"""

        min_date, max_date = min_and_max_from_date_ranges([asdict(time_period) for time_period in time_periods])
        date_range = generate_date_range(min_date, max_date, self.group)
        if date_range.count != overall_results.count:
            for year in date_range:
                if not (
                    any(
                        overall_result["time_period"] == {"fiscal_year": str(year["fiscal_year"])}
                        for overall_result in overall_results
                    )
                ):
                    overall_results.append(
                        {
                            "aggregated_amount": 0,
                            "total_outlays": None,
                            "time_period": {"fiscal_year": str(year["fiscal_year"])},
                            "Contract_Obligations": 0,
                            "Contract_Outlays": None,
                            "Grant_Obligations": 0,
                            "Grant_Outlays": None,
                        }
                    )
            overall_results = sorted(overall_results, key=lambda x: x["time_period"]["fiscal_year"])

        return overall_results

    @cache_response()
    def post(self, request: Request) -> Response:
        self.original_filters = request.data.get("filters")
        json_request, models = self.validate_request_data(request.data)
        self.group = GROUPING_LOOKUP[json_request["group"]]
        self.subawards = (
            SpendingLevel(json_request.get("spending_level").lower()) == SpendingLevel.SUBAWARD
            or json_request["subawards"]
        )
        self.spending_level = (
            SpendingLevel.SUBAWARD if self.subawards else SpendingLevel(json_request.get("spending_level").lower())
        )
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
        results = None
        time_periods = [
            TimePeriod(**time_period) for time_period in self.filters.get("time_period", [default_time_period])
        ]

        # gets the query type ex: _QueryType.AWARDS if self.spending_level = SpendingLevel.AWARDS
        query_type = _QueryType(self.spending_level.value)
        query_with_filters = QueryWithFilters(query_type)
        filter_query = query_with_filters.query_elasticsearch(self.filters)
        if self.spending_level == SpendingLevel.SUBAWARD:
            search = SubawardSearch().filter(filter_query)
            self.apply_elasticsearch_aggregations(search)
            response = search.handle_execute()
            results_with_values = self.build_elasticsearch_result_awards_subawards(response.aggs)
            results = self.set_default_for_subawards(time_periods, results_with_values)
        elif self.spending_level == SpendingLevel.TRANSACTION:
            search = TransactionSearch().filter(filter_query)
            self.apply_elasticsearch_aggregations(search)
            response = search.handle_execute()
            results = self.build_elasticsearch_result_transactions(response.aggs, time_periods)
        elif self.spending_level == SpendingLevel.AWARD:
            search = AwardSearch().filter(filter_query)        
            self.apply_elasticsearch_aggregations(search)
            response = search.handle_execute()
            results = self.build_elasticsearch_result_awards_subawards(response.aggs)

        raw_response = OrderedDict(
            [
                ("group", self.group),
                ("results", results),
                ("spending_level", self.spending_level.value),
                (
                    "messages",
                    [
                        *get_generic_filters_message(self.original_filters.keys(), [elem["name"] for elem in models]),
                        (
                            "The 'subawards' field will be deprecated in the future. "
                            "Set 'spending_level' to 'subawards' instead. See documentation for more information."
                        ),
                        (
                            "You may see additional month, quarter and year results when searching for "
                            "Awards or Subawards. This is due to Awards or Subawards overlapping with the "
                            "time period specified but having an 'action date' outside of that time period."
                        ),
                    ],
                ),
            ]
        )

        return Response(raw_response)
