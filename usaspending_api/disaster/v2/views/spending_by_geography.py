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
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator import TinyShield
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.references.abbreviations import code_to_state
from usaspending_api.search.v2.elasticsearch_helper import (
    get_number_of_unique_terms_for_awards,
)


class GeoLayer(Enum):
    COUNTY = "county"
    DISTRICT = "district"
    STATE = "state"


class SpendingByGeographyViewSet(DisasterBase):
    """Spending by Recipient Location"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/spending_by_geography.md"

    required_filters = ["def_codes", "award_type_codes"]

    agg_key: Optional[str]  # name of ES index field whose term value will be used for grouping the agg
    geo_layer: GeoLayer
    geo_layer_filters: Optional[List[str]]
    spending_type: str  # which type of disaster spending to get data for (obligation, outlay, face_value_of_loan)
    loc_lookup: str
    metric_field: str  # field in ES index whose value will be summed across matching docs

    @cache_response()
    def post(self, request: Request) -> Response:
        models = [
            {
                "key": "geo_layer",
                "name": "geo_layer",
                "type": "enum",
                "enum_values": sorted([geo_layer.value for geo_layer in list(GeoLayer)]),
                "text_type": "search",
                "allow_nulls": False,
                "optional": False,
            },
            {
                "key": "geo_layer_filters",
                "name": "geo_layer_filters",
                "type": "array",
                "array_type": "text",
                "text_type": "search",
            },
            {
                "key": "spending_type",
                "name": "spending_type",
                "type": "enum",
                "enum_values": ["obligation", "outlay", "face_value_of_loan"],
                "allow_nulls": False,
                "optional": False,
            },
            {
                "name": "scope",
                "key": "scope",
                "type": "enum",
                "optional": True,
                "enum_values": ["place_of_performance", "recipient_location"],
                "default": "recipient_location",
            },
        ]

        # NOTE: filter object in request handled in base class: see self.filters
        json_request = TinyShield(models).block(request.data)

        agg_key_dict = {
            "county": "county_agg_key",
            "district": "congressional_agg_key",
            "state": "state_agg_key",
        }
        scope_dict = {"place_of_performance": "pop", "recipient_location": "recipient_location"}
        location_dict = {"county": "county_code", "district": "congressional_code", "state": "state_code"}

        self.geo_layer = GeoLayer(json_request["geo_layer"])

        scope_field_name = scope_dict[json_request["scope"]]
        loc_field_name = location_dict[self.geo_layer.value]

        self.agg_key = f"{scope_field_name}_{agg_key_dict[json_request['geo_layer']]}"
        self.geo_layer_filters = json_request.get("geo_layer_filters")
        self.spending_type = json_request.get("spending_type")
        self.loc_lookup = f"{scope_field_name}_{loc_field_name}"

        # Set which field will be the aggregation amount
        if self.spending_type == "obligation":
            self.metric_field = "total_covid_obligation"
            self.metric_agg = A("sum", field="covid_spending_by_defc.obligation", script="_value * 100")
        elif self.spending_type == "outlay":
            self.metric_field = "total_covid_outlay"
            self.metric_agg = A("sum", field="covid_spending_by_defc.outlay", script="_value * 100")
        elif self.spending_type == "face_value_of_loan":
            self.metric_field = "total_loan_value"
            self.metric_agg = A("sum", field="total_loan_value", script="_value * 100")
        else:
            raise UnprocessableEntityException(
                f"Unrecognized value '{self.spending_type}' for field " f"'spending_type'"
            )

        filter_query = QueryWithFilters.generate_awards_elasticsearch_query(self.filters)
        result = self.query_elasticsearch(filter_query)

        return Response(
            {
                "geo_layer": self.geo_layer.value,
                "spending_type": self.spending_type,
                "scope": json_request["scope"],
                "results": result,
            }
        )

    def build_elasticsearch_search_with_aggregation(self, filter_query: ES_Q) -> Optional[AwardSearch]:
        # Create the initial search using filters
        search = AwardSearch().filter(filter_query)

        # Check number of unique terms (buckets) for performance and restrictions on maximum buckets allowed
        bucket_count = get_number_of_unique_terms_for_awards(filter_query, f"{self.agg_key}.hash")

        if bucket_count == 0:
            return None
        else:
            # Add 1 to handle null case since murmur3 doesn't support "null_value" property
            bucket_count += 1

        # Add 100 to make sure that we consider enough records in each shard for accurate results
        group_by_agg_key = A("terms", field=self.agg_key, size=bucket_count, shard_size=bucket_count + 100)
        filter_agg_query = ES_Q("terms", **{"covid_spending_by_defc.defc": self.filters.get("def_codes")})

        search.aggs.bucket("group_by_agg_key", group_by_agg_key).bucket(
            "nested", A("nested", path="covid_spending_by_defc")
        ).bucket("filtered_aggs", A("filter", filter_agg_query)).metric(self.spending_type, self.metric_agg)
        # Set size to 0 since we don't care about documents returned
        search.update_from_dict({"size": 0})

        return search

    def build_elasticsearch_result(self, response: dict) -> Dict[str, dict]:
        results = {}
        geo_info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        for bucket in geo_info_buckets:
            if bucket.get("key") == "NULL":
                display_name = None
                shape_code = None
                population = None
            else:
                geo_info = json.loads(bucket.get("key"))
                state_code = geo_info["state_code"] or ""
                population = int(geo_info["population"]) if geo_info["population"] else None

                if self.geo_layer == GeoLayer.STATE:
                    shape_code = state_code.upper()
                    display_name = geo_info["state_name"] or code_to_state.get(state_code, {}).get("name", "")
                    display_name = display_name.title()
                elif self.geo_layer == GeoLayer.COUNTY:
                    state_fips = geo_info["state_fips"] or code_to_state.get(state_code, {}).get("fips", "")
                    display_name = (geo_info["county_name"] or "").title()
                    shape_code = f"{state_fips}{geo_info['county_code']}"
                else:
                    state_fips = geo_info["state_fips"] or code_to_state.get(state_code, {}).get("fips", "")
                    display_name = f"{state_code}-{geo_info['congressional_code']}".upper()
                    shape_code = f"{state_fips}{geo_info['congressional_code']}"

            per_capita = None
            amount = int(
                bucket.get("nested", {}).get("filtered_aggs", {}).get(f"{self.spending_type}", {"value": 0})["value"]
            ) / Decimal("100")

            if population:
                per_capita = (Decimal(amount) / Decimal(population)).quantize(Decimal(".01"))

            results[shape_code] = {
                "amount": amount,
                "display_name": display_name or None,
                "shape_code": shape_code or None,
                "population": population,
                "per_capita": per_capita,
                "award_count": int(bucket.get("doc_count", 0)),
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
