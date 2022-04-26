import copy
from datetime import datetime

import logging

from django.conf import settings
from elasticsearch_dsl import Q, A
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.aggregation_helpers import create_count_aggregation
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.helpers.fiscal_year_helpers import (
    generate_fiscal_year,
    generate_fiscal_month,
    generate_fiscal_quarter,
    current_fiscal_year,
)
from usaspending_api.common.helpers.generic_helper import get_generic_filters_message
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.recipient.models import RecipientProfile

logger = logging.getLogger(__name__)

API_VERSION = settings.API_VERSION


class NewAwardsOverTimeVisualizationViewSet(APIView):
    """
    This route returns a list of time periods with the new awards in the
    appropriate period within the provided time range
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/new_awards_over_time.md"

    def validate_api_request(self, json_payload):
        self.groupings = {
            "quarter": "quarter",
            "q": "quarter",
            "fiscal_year": "fiscal_year",
            "fy": "fiscal_year",
            "month": "month",
            "m": "month",
        }
        models = [
            {
                "name": "group",
                "key": "group",
                "type": "enum",
                "enum_values": list(self.groupings.keys()),
                "default": "fy",
            }
        ]
        advanced_search_filters = [
            model for model in copy.deepcopy(AWARD_FILTER) if model["name"] in ("time_period", "recipient_id")
        ]

        for model in advanced_search_filters:
            if model["name"] in ("time_period", "recipient_id"):
                model["optional"] = False
        models.extend(advanced_search_filters)
        return TinyShield(models).block(json_payload)

    def query_elasticsearch(self):
        filters = self.filters
        filters["time_period"][0]["date_type"] = "date_signed"
        recipient_hash = self.filters["recipient_id"][:-2]
        if self.filters["recipient_id"][-1] == "P":
            # there *should* only one record with that hash and recipient_level = 'P'
            parent_uei_rows = RecipientProfile.objects.filter(
                recipient_hash=recipient_hash, recipient_level="P"
            ).values("uei")
            if len(parent_uei_rows) != 1:
                raise InvalidParameterException("Provided recipient_id has no parent records")
            parent_uei = parent_uei_rows[0]["uei"]
            # This is for two reasons - we don't store the `parent_recipient_hash` for awards,
            # and the original postgres version of the code also searched by parent_uei instead of the parent hash
            filters.pop("recipient_id")
            filter_query = QueryWithFilters.generate_awards_elasticsearch_query(filters)
            filter_query.must.insert(0, Q("match", parent_uei=parent_uei))
        else:
            filter_query = QueryWithFilters.generate_awards_elasticsearch_query(filters)
        # This has to be hard coded in since QueryWithFilters automatically uses "action_date" for awards
        filter_query.must[1].should[0].should[0] = Q(
            "range", **{"date_signed": {"gte": filters["time_period"][0]["start_date"]}}
        )
        search = AwardSearch().filter(filter_query)
        if self.group == "month":
            time_period_field = "month"
        elif self.group == "quarter":
            time_period_field = "quarter"
        elif self.group == "fiscal_year":
            time_period_field = "year"

        group_by_time = A("date_histogram", field="date_signed", interval=time_period_field, offset="+274d")
        search.aggs.bucket("time_period", group_by_time).metric("award_count", create_count_aggregation("award_id"))
        search.update_from_dict({"size": 0})
        response = search.handle_execute()
        return response

    def complete_missing_periods(self, results):
        required_years = range(
            generate_fiscal_year(datetime.strptime(self.filters["time_period"][0]["start_date"], "%Y-%m-%d")), current_fiscal_year()
        )
        years = [x["time_period"]["fiscal_year"] for x in results]
        if self.group == "fiscal_year":
            for x in set(required_years) - set(years):
                results.append(
                    {
                        "new_award_count_in_period": 0,
                        "time_period": {"fiscal_year": x},
                    }
                )
        else:
            if self.group == "month":
                time_range = range(1, 13)
            elif self.group == "quarter":
                time_range = range(1, 5)
            years_pairs = [(x["time_period"][self.group], x["time_period"]["fiscal_year"]) for x in results]
            required_year_pairs = []
            for x in required_years:
                for y in time_range:
                    required_year_pairs.append((y, x))
            for x in set(required_year_pairs) - set(years_pairs):
                results.append(
                    {
                        "new_award_count_in_period": 0,
                        "time_period": {
                            self.group: x[0],
                            "fiscal_year": x[1],
                        },
                    }
                )
        results = sorted(results, key=lambda x: (x["time_period"]["fiscal_year"], x["time_period"][self.group]))
        return results

    def format_results(self, es_results):
        results = []
        if self.group == "month":
            date_function = generate_fiscal_month
        elif self.group == "quarter":
            date_function = generate_fiscal_quarter
        for x in es_results.aggs.to_dict().get("time_period", {}).get("buckets", []):
            date = datetime.strptime(x.get("key_as_string"), "%Y-%m-%d")
            if self.group != "fiscal_year":
                time_period = {self.group: date_function(date), "fiscal_year": generate_fiscal_year(date)}
            else:
                time_period = {"fiscal_year": generate_fiscal_year(date)}
            results.append(
                {
                    "new_award_count_in_period": x.get("award_count", {}).get("value", 0),
                    "time_period": time_period,
                }
            )
        results = self.complete_missing_periods(results)
        return results

    @cache_response()
    def post(self, request):
        self.original_filters = request.data.get("filters")
        self.json_request = self.validate_api_request(request.data)
        self.filters = self.json_request.get("filters", None)
        self.group = self.groupings[self.json_request["group"]]
        if self.filters is None:
            raise InvalidParameterException("Missing request parameters: filters")

        es_results = self.query_elasticsearch()
        results = self.format_results(es_results)
        response = {
            "group": self.group,
            "results": results,
            "messages": get_generic_filters_message(self.original_filters.keys(), {"time_period", "recipient_id"}),
        }
        return Response(response)
