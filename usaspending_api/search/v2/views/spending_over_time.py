import copy
import logging
from datetime import datetime, timezone

from collections import OrderedDict
from django.conf import settings
from django.db.models import Sum
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_over_time
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.client import es_client_query
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.orm_helpers import FiscalMonth, FiscalQuarter, FiscalYear
from usaspending_api.common.helpers.generic_helper import bolster_missing_time_periods, generate_fiscal_year
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.search.v2.elasticsearch_helper import base_awards_query

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingOverTimeVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by time. The amount of time is denoted by the "group" value.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_over_time.md"

    GROUP_MAPPINGS = {
        "quarter": "quarter",
        "q": "quarter",
        "fiscal_year": "fiscal_year",
        "fy": "fiscal_year",
        "month": "month",
        "m": "month",
    }

    def validate_request_data(self, json_data):
        models = [
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
            {
                "name": "group",
                "key": "group",
                "type": "enum",
                "enum_values": list(self.GROUP_MAPPINGS.keys()),
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
        queryset = subaward_filter(self.filters)
        obligation_column = "amount"

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

    def create_elasticsearch_aggregation(self):
        all_aggregations = {
            "aggs": {"group_by_fiscal_year": {"terms": {"field": "fiscal_year", "order": {"_key": "asc"}, "size": 50}}}
        }

        sum_obligated_amount_aggregation = {
            "obligated_amount_sum_as_cents": {
                "sum": {"script": {"lang": "painless", "source": "doc['generated_pragmatic_obligation'].value * 100"}}
            },
            "obligated_amount_sum_as_dollars": {
                "bucket_script": {
                    "buckets_path": {"obligated_amount_sum": "obligated_amount_sum_as_cents"},
                    "script": "params.obligated_amount_sum / 100",
                }
            },
        }

        if self.group == "month" or self.group == "quarter":
            field_name = "fiscal_{}".format(self.group)
            nested_group_by_name = "group_by_{}".format(field_name)
            all_aggregations["aggs"]["group_by_fiscal_year"]["aggs"] = {
                nested_group_by_name: {
                    "terms": {
                        "field": field_name,
                        "order": {"_key": "asc"},
                        "size": 12 if self.group == "month" else 4,
                    },
                    "aggs": sum_obligated_amount_aggregation,
                }
            }
        else:
            all_aggregations["aggs"]["group_by_fiscal_year"]["aggs"] = sum_obligated_amount_aggregation

        return all_aggregations

    def parse_elasticsearch_response(self, hits):
        results = []

        if self.group == "fiscal_year":
            for year_bucket in hits["aggregations"]["group_by_fiscal_year"]["buckets"]:
                results.append(
                    {
                        "aggregated_amount": year_bucket["obligated_amount_sum_as_dollars"]["value"],
                        "time_period": {"fiscal_year": str(year_bucket["key"])},
                    }
                )
        else:
            nested_group_by_string = "group_by_fiscal_{}".format(self.group)
            for year_bucket in hits["aggregations"]["group_by_fiscal_year"]["buckets"]:
                for nested_bucket in year_bucket[nested_group_by_string]["buckets"]:
                    results.append(
                        {
                            "aggregated_amount": nested_bucket["obligated_amount_sum_as_dollars"]["value"],
                            "time_period": {
                                self.group: str(nested_bucket["key"]),
                                "fiscal_year": str(year_bucket["key"]),
                            },
                        }
                    )

        return results

    def query_elasticsearch(self):
        query = {"query": {**base_awards_query(self.filters)}, **self.create_elasticsearch_aggregation(), "size": 0}

        hits = es_client_query(index="{}*".format(settings.TRANSACTIONS_INDEX_ROOT), body=query)

        results = []
        if hits and hits["hits"]["total"] > 0:
            results = self.parse_elasticsearch_response(hits)
        return results

    @cache_response()
    def post(self, request):
        json_request = self.validate_request_data(request.data)
        self.group = self.GROUP_MAPPINGS[json_request["group"]]
        self.subawards = json_request["subawards"]
        self.filters = json_request["filters"]

        # Temporarily will handle Subawards with ORM and Awards with ES for proof of concept
        if self.subawards:
            db_results, values = self.database_data_layer()

            # time_period is optional so we're setting a default window from API_SEARCH_MIN_DATE to end of the current FY.
            # Otherwise, users will see blank results for years
            current_fy = generate_fiscal_year(datetime.now(timezone.utc))
            if self.group == "fiscal_year":
                end_date = "{}-09-30".format(current_fy)
            else:
                end_date = "{}-{}-30".format(current_fy, datetime.now(timezone.utc).month)

            default_time_period = {"start_date": settings.API_SEARCH_MIN_DATE, "end_date": end_date}
            time_periods = self.filters.get("time_period", [default_time_period])

            results = bolster_missing_time_periods(
                filter_time_periods=time_periods,
                queryset=db_results,
                date_range_type=values[-1],
                columns={"aggregated_amount": "aggregated_amount"},
            )
        else:
            results = self.query_elasticsearch()

        return Response(OrderedDict([("group", self.group), ("results", results)]))
