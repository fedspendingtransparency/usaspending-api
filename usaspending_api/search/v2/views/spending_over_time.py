import copy
import logging

from calendar import monthrange
from collections import OrderedDict
from datetime import datetime, timezone

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
    generate_fiscal_date_range,
    generate_fiscal_month,
    generate_fiscal_year,
)
from usaspending_api.common.helpers.generic_helper import (
    get_generic_filters_message,
    min_and_max_from_date_ranges,
)
from usaspending_api.common.helpers.orm_helpers import FiscalMonth, FiscalQuarter
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.filters.elasticsearch.filter import _QueryType
from usaspending_api.search.filters.time_period.query_types import TransactionSearchTimePeriod

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION
GROUPING_LOOKUP = {
    "calendar_year": "calendar_year",
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

    # in this function we are justing taking the elasticsearch aggregate response and looping through the 
    # buckets to create a results object for each time interval
    # the breakdown for the aggregated values based on award types should be done before this. 
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
            if self.group == "calendar_year":
                time_period = {"calendar_year": str(fiscal_date["calendar_year"])}
            elif self.group == "quarter":
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
                        "time_period": time_period
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

        user_input_filters = copy.deepcopy(self.filters)  # Copy of the original filters

        # Generate the overall filter query (aggregated results for all the codes.)
        filter_query = QueryWithFilters.generate_transactions_elasticsearch_query(self.filters, **filter_options)
        search = TransactionSearch().filter(filter_query)
        self.apply_elasticsearch_aggregations(search)
        response = search.handle_execute()
        overall_results = self.build_elasticsearch_result(response.aggs, time_periods)

        # Mapping of obligation types to their corresponding codes
        OBLIGATION_TYPE_TO_CODES = {
            "contract_obligations": ["A", "B", "C", "D"],
            "idv_obligations": ["IDV_A", "IDV_B", "IDV_B_A", "IDV_B_B", "IDV_B_C", "IDV_C", "IDV_D", "IDV_E"],
            "grant_obligations": ["02", "03", "04", "05"],
            "direct_payment_obligations": ["06", "10"],
            "loan_obligations": ["07", "08"],
            "other_obligations": ["09", "11", "-1"],
        }

        # Initialize a dictionary to hold the results for each obligation type
        obligation_breakdown_results = {
            "overall": overall_results,
        }
            
        # this is where we create different filter_queries based on different filters [idv_d, idv_e, and etc.. ]
        # then we aggregate those results
        # Generate separate filter queries for each obligation type
        for obligation_type, codes in OBLIGATION_TYPE_TO_CODES.items():
            if any(code in user_input_filters['award_type_codes'] for code in codes):
                # Create a specific filter for the current obligation type
                specific_filters = copy.deepcopy(self.filters)
                specific_filters['award_type_codes'] = [code for code in codes if code in user_input_filters['award_type_codes']]

                specific_filter_query = QueryWithFilters.generate_transactions_elasticsearch_query(specific_filters, **filter_options)
                specific_search = TransactionSearch().filter(specific_filter_query)
                self.apply_elasticsearch_aggregations(specific_search)
                specific_response = specific_search.handle_execute()
                specific_results = self.build_elasticsearch_result(specific_response.aggs, time_periods)

                obligation_breakdown_results[obligation_type] = specific_results

        # Combine the results into the final format
        final_results = []
        for overall_result in overall_results:
            combined_result = {
                "aggregated_amount": overall_result["aggregated_amount"],
                "time_period": overall_result["time_period"],
            }
            total_obligations = 0
            for obligation_type in OBLIGATION_TYPE_TO_CODES.keys():
                if obligation_type in obligation_breakdown_results:
                    matching_result = next(
                        (res for res in obligation_breakdown_results[obligation_type] if res["time_period"] == overall_result["time_period"]),
                        {"aggregated_amount": 0}
                    )
                    combined_result[obligation_type] = matching_result["aggregated_amount"]
                    total_obligations += matching_result["aggregated_amount"]
                else:
                    combined_result[obligation_type] = 0
            combined_result["Values_add_up"] = (total_obligations == overall_result["aggregated_amount"])
            final_results.append(combined_result)

        print("stop right here so we can inspect final_results!!!!")
        return final_results

    @cache_response()
    def post(self, request: Request) -> Response:
        self.original_filters = request.data.get("filters")
        json_request = self.validate_request_data(request.data)
        self.group = GROUPING_LOOKUP[json_request["group"]]
        self.subawards = json_request["subawards"]
        self.filters = json_request["filters"]

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

        if self.subawards:
            db_results, order_by_cols = self.database_data_layer_for_subawards()
            results = bolster_missing_time_periods(
                filter_time_periods=time_periods,
                queryset=db_results,
                date_range_type=order_by_cols[-1],
                columns={"aggregated_amount": "aggregated_amount"},
            )
        else:
            # this is where we make the call to retrieve the data/results.
            results = self.query_elasticsearch_for_prime_awards(time_periods)

        raw_response = OrderedDict(
            [
                ("group", self.group),
                ("results", results),
                (
                    "messages",
                    get_generic_filters_message(self.original_filters.keys(), [elem["name"] for elem in AWARD_FILTER]),
                ),
            ]
        )

        return Response(raw_response)
