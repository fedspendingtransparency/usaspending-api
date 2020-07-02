import copy
import json
from decimal import Decimal
from enum import Enum
from typing import Optional, List, Dict

from rest_framework.request import Request
from rest_framework.response import Response
from elasticsearch_dsl import A, Q as ES_Q

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.helpers.generic_helper import get_generic_filters_message
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator import TinyShield, PAGINATION
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.search.v2.elasticsearch_helper import get_number_of_unique_terms, get_scaled_sum_aggregations


class GeoLayer(Enum):
    COUNTY = "county"
    DISTRICT = "district"
    STATE = "state"


class SpendingByGeographyViewSet(DisasterBase):
    """Spending by Recipient Location"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/spending_by_geography.md"
    # TODO: Are contracts under https://github.com/fedspendingtransparency/usaspending-api/tree/dev/usaspending_api/api_contracts/contracts/v2/disaster/recipient/location needed anymore??

    agg_key: Optional[str]  # name of ES index field whose term value will be used for grouping the agg
    filters: dict  # API request filters to manipulate query
    geo_layer: GeoLayer
    geo_layer_filters: Optional[List[str]]
    loc_field_name: str
    loc_lookup: str
    metric_field: str  # field in ES index whose value will be summed across matching docs
    scope_field_name: str = "recipient_location"  # only based on recipient location data

    @cache_response()
    def post(self, request: Request) -> Response:

        # ======================================================================
        # Advanced Search's Spending By Geography
        #     usaspending-api/usaspending_api/search/v2/views/spending_by_geography.py
        # Elasticsearch helpers
        #     usaspending-api/usaspending_api/common/query_with_filters.py

        # self.source_fields contains which ES document fields are necessary for the API response

        # ======================================================================

        models = [
            {
                "key": "geo_layer",
                "name": "geo_layer",
                "type": "enum",
                "enum_values": ["state", "county", "district"],
                "text_type": "search",
                "allow_nulls": False,
                "optional": True,
            },
            {
                "key": "geo_layer_filters",
                "name": "geo_layer_filters",
                "type": "array",
                "array_type": "text",
                "text_type": "search",
                "allow_nulls": False,
                "optional": False,
            },
            {
                "key": "spending_type",
                "name": "spending_type",
                "type": "enum",
                "enum_values": ["obligation", "outlay", "face_value_of_loan"],
                "allow_nulls": False,
                "optional": False,
            },
        ]

        # TODO: Need to reconcile the use of defc_codes as per the API contract
        # TODO: def_codes cannot be used here, becasue the metric being summed is already pre-aggregated to include
        #       _ALL_ COVID def codes, and therefore cannot be sliced into smaller portions of the sum per DEFC
        # TODO: This really then becomes an endpoint only usable for the COVID-19 disaster, and not arbitrary
        #       disasters (as it is setup right now)

        models.extend(copy.deepcopy([f for f in AWARD_FILTER if f["name"] == "award_type_codes"]))
        models.extend(copy.deepcopy(PAGINATION))
        original_filters = request.data.get("filters")
        json_request = TinyShield(models).block(request.data)

        agg_key_dict = {
            "county": "county_agg_key",
            "district": "congressional_agg_key",
            "state": "state_agg_key",
        }
        location_dict = {"county": "county_code", "district": "congressional_code", "state": "state_code"}

        self.agg_key = f"{self.scope_field_name}_{agg_key_dict[json_request['geo_layer']]}"
        self.filters = json_request.get("filters")
        self.geo_layer = GeoLayer(json_request["geo_layer"])
        self.geo_layer_filters = json_request.get("geo_layer_filters")
        self.loc_field_name = location_dict[self.geo_layer.value]
        self.loc_lookup = f"{self.scope_field_name}_{self.loc_field_name}"

        # Set which field will be the aggregation amount
        if self.filters["spending_type"] == "obligation":
            self.metric_field = "total_covid_obligation"
        elif self.filters["spending_type"] == "outlay":
            self.metric_field = "total_covid_outlay"
        elif self.filters["spending_type"] == "loan":
            self.metric_field = "total_loan_value"
        else:
            raise UnprocessableEntityException(
                f"Unrecognized value '{self.filters['spending_type']}' for field " f"'spending_type'"
            )

        filter_query = QueryWithFilters.generate_transactions_elasticsearch_query(self.filters)
        result = self.query_elasticsearch(filter_query)

        return Response(
            {
                "scope": json_request["scope"],
                "geo_layer": self.geo_layer.value,
                "results": result,
                "messages": get_generic_filters_message(
                    original_filters.keys(), [elem["name"] for elem in AWARD_FILTER]
                ),
            }
        )

    def build_elasticsearch_search_with_aggregation(self, filter_query: ES_Q) -> Optional[AwardSearch]:
        # Create the initial search using filters
        search = AwardSearch().filter(filter_query)

        # Check number of unique terms (buckets) for performance and restrictions on maximum buckets allowed
        bucket_count = get_number_of_unique_terms(filter_query, f"{self.agg_key}.hash")

        if bucket_count == 0:
            return None

        # Add 100 to make sure that we consider enough records in each shard for accurate results
        group_by_agg_key = A("terms", field=self.agg_key, size=bucket_count, shard_size=bucket_count + 100)
        sum_aggregations = get_scaled_sum_aggregations(self.metric_field)
        sum_field = sum_aggregations["sum_field"]

        search.aggs.bucket("group_by_agg_key", group_by_agg_key).metric("sum_field", sum_field)

        # Set size to 0 since we don't care about documents returned
        search.update_from_dict({"size": 0})

        return search

    def build_elasticsearch_result(self, response: dict) -> Dict[str, dict]:
        results = {}
        geo_info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        for bucket in geo_info_buckets:
            geo_info = json.loads(bucket.get("key"))

            if self.geo_layer == GeoLayer.STATE:
                display_name = geo_info.get("state_name").title()
                shape_code = geo_info.get("state_code").upper()
            elif self.geo_layer == GeoLayer.COUNTY:
                display_name = geo_info["county_name"].title()
                shape_code = f"{geo_info['state_fips']}{geo_info['county_code']}"
            else:
                display_name = f"{geo_info['state_code']}-{geo_info['congressional_code']}".upper()
                shape_code = f"{geo_info['state_fips']}{geo_info['congressional_code']}"

            per_capita = None
            aggregated_amount = int(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100")
            population = int(geo_info["population"]) if geo_info["population"] else None
            if population:
                per_capita = (Decimal(aggregated_amount) / Decimal(population)).quantize(Decimal(".01"))

            results[shape_code] = {
                "shape_code": shape_code or None,
                "aggregated_amount": aggregated_amount,
                "display_name": display_name or None,
                "population": population,
                "per_capita": per_capita,
            }

        return results

    def query_elasticsearch(self, filter_query: ES_Q) -> list:
        search = self.build_elasticsearch_search_with_aggregation(filter_query)
        if search is None:
            return []
        response = search.handle_execute()
        results_dict = self.build_elasticsearch_result(response.aggs.to_dict())

        if self.geo_layer_filters:
            filtered_shape_codes = set(self.geo_layer_filters) & set(results_dict.keys())
            results = [results_dict[shape_code] for shape_code in filtered_shape_codes]
        else:
            results = results_dict.values()

        return results
