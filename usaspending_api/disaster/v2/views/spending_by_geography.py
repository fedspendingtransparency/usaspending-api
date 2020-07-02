from decimal import Decimal
from enum import Enum
from rest_framework.request import Request
from rest_framework.response import Response
from typing import List

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.validator import TinyShield
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.references.abbreviations import code_to_state, pad_codes
from usaspending_api.references.models import PopCounty, PopCongressionalDistrict


class GeoLayer(Enum):
    COUNTY = "county"
    DISTRICT = "district"
    STATE = "state"


class SpendingByGeographyViewSet(DisasterBase):
    """Spending by Recipient Location"""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/disaster/spending_by_geography.md"

    @cache_response()
    def post(self, request: Request) -> Response:

        models = [
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
                "key": "geo_layer",
                "name": "geo_layer",
                "type": "enum",
                "enum_values": ["state", "county", "district"],
                "text_type": "search",
                "allow_nulls": False,
                "optional": True,
            },
            {
                "key": "spending_type",
                "name": "spending_type",
                "type": "enum",
                "enum_values": ["per-capita", "award", "loan"],
                "allow_nulls": False,
                "optional": False,
            },
        ]

        self.filters.update(TinyShield(models).block(self.request.data))

        # ======================================================================
        # Advanced Search's Spending By Geography
        #     usaspending-api/usaspending_api/search/v2/views/spending_by_geography.py
        # Elasticsearch helpers
        #     usaspending-api/usaspending_api/common/query_with_filters.py

        # self.source_fields contains which ES document fields are necessary for the API response

        # ======================================================================

        # Set which field will be the aggregation amount
        if self.filters["spending_type"] == "face_value_of_loan":
            pass
        elif self.filters["spending_type"] == "obligation":
            pass
        else:  # spending_type == "outlay"
            pass

        return Response({"results": self.assemble_results()})

    @property
    def source_fields(self):
        if self.filters["geo_layer"] == GeoLayer.STATE.value:
            if self.filters["geo_layer_filters"]:
                pass
            fields = ["recipient_location_state_code"]

        elif self.filters["geo_layer"] == GeoLayer.COUNTY.value:
            if self.filters["geo_layer_filters"]:
                pass
            fields = [
                "recipient_location_state_code",
                "recipient_location_county_code",
                "recipient_location_county_name",
            ]

        else:  # GeoLayer.DISTRICT.value
            if self.filters["geo_layer_filters"]:
                pass
            fields = ["recipient_location_state_code", "recipient_location_congressional_code"]

        return fields

    def assemble_results(self):
        if self.filters["geo_layer"] == GeoLayer.STATE.value:
            results = self.state_results("recipient_location_state_code")
        elif self.filters["geo_layer"] == GeoLayer.COUNTY.value:
            results = self.county_results("recipient_location_state_code", "recipient_location_county_name")
        else:
            results = self.district_results("recipient_location_state_code")

        return results

    def state_results(self, loc_lookup: str) -> List[dict]:
        state_pop_rows = PopCounty.objects.filter(county_number="000").values()
        populations = {row["state_name"].lower(): row["latest_population"] for row in state_pop_rows}

        # State names are inconsistent in database (upper, lower, null)
        # Used lookup instead to be consistent
        results = []
        for x in self.queryset:
            shape_code = x[loc_lookup]
            per_capita = None
            population = populations.get(code_to_state.get(shape_code, {"name": "None"}).get("name").lower())
            if population:
                per_capita = (Decimal(x["amount"]) / Decimal(population)).quantize(Decimal(".01"))

            results.append(
                {
                    "shape_code": shape_code,
                    "aggregated_amount": x["amount"],
                    "display_name": code_to_state.get(shape_code, {"name": "None"}).get("name").title(),
                    "population": population,
                    "per_capita": per_capita,
                }
            )

        return results

    def county_results(self, state_lookup: str, county_name: str) -> List[dict]:
        # Returns county results formatted for map
        state_pop_rows = PopCounty.objects.exclude(county_number="000").values()
        populations = {f"{row['state_code']}{row['county_number']}": row["latest_population"] for row in state_pop_rows}

        results = []
        for x in self.queryset:
            shape_code = code_to_state.get(x[state_lookup])["fips"] + pad_codes(
                GeoLayer.COUNTY.value, x["recipient_location_county_code"]
            )
            per_capita = None
            population = populations.get(shape_code)
            if population:
                per_capita = (Decimal(x["amount"]) / Decimal(population)).quantize(Decimal(".01"))

            results.append(
                {
                    "shape_code": shape_code,
                    "aggregated_amount": x["amount"],
                    "display_name": x[county_name].title() if x[county_name] is not None else x[county_name],
                    "population": population,
                    "per_capita": per_capita,
                }
            )

        return results

    def district_results(self, state_lookup: str) -> List[dict]:
        # Returns congressional district results formatted for map

        state_pop_rows = PopCongressionalDistrict.objects.all().values()
        populations = {
            f"{row['state_code']}{row['congressional_district']}": row["latest_population"] for row in state_pop_rows
        }

        results = []
        for x in self.queryset:
            shape_code = code_to_state.get(x[state_lookup])["fips"] + pad_codes(
                GeoLayer.DISTRICT.value, x["recipient_location_congressional_code"]
            )
            per_capita = None
            population = populations.get(shape_code)
            if population:
                per_capita = (Decimal(x["amount"]) / Decimal(population)).quantize(Decimal(".01"))

            results.append(
                {
                    "shape_code": shape_code,
                    "aggregated_amount": x["amount"],
                    "display_name": x[state_lookup]
                    + "-"
                    + pad_codes(GeoLayer.DISTRICT.value, x["recipient_location_congressional_code"]),
                    "population": population,
                    "per_capita": per_capita,
                }
            )

        return results
