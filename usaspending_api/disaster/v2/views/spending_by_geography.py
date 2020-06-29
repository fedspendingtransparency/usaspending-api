from typing import Optional, List, Dict
from django.db.models import Q, Sum, Count, F, Value, DecimalField, Case, When, OuterRef, Exists
from django.db.models.functions import Coalesce
from rest_framework.response import Response
from rest_framework.request import Request
from enum import Enum
from decimal import Decimal

from usaspending_api.references.models import PopCounty, PopCongressionalDistrict
from usaspending_api.references.abbreviations import code_to_state, fips_to_code, pad_codes
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.disaster.v2.views.disaster_base import DisasterBase
from usaspending_api.search.models import LoanAwardSearchMatview
from usaspending_api.common.validator import TinyShield
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations


def submission_window_cutoff(min_date, monthly_sub, quarterly_sub):
    return [
        Q(submission__reporting_period_start__gte=min_date),
        Q(
            Q(
                Q(submission__quarter_format_flag=False)
                & Q(submission__reporting_period_end__lte=monthly_sub["submission_reveal_date"])
            )
            | Q(
                Q(submission__quarter_format_flag=True)
                & Q(submission__reporting_period_end__lte=quarterly_sub["submission_reveal_date"])
            )
        ),
    ]


class GeoLayer(Enum):
    COUNTY = "county"
    DISTRICT = "district"
    STATE = "state"


# class LoanQuery:
#     @property
#     def queryset(self):
#         pass


# class StateResults:

#     def queryset(self):
#         pass

#     def finalize_response(self):
#         pass


# class GeographyView:
#     def __init__(self, type: GeoLayer):
#         if self.type == GeoLayer.STATE:
#             pass


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

        geo_filters = Q()

        if self.filters["geo_layer"] == GeoLayer.STATE.value:
            if self.filters["geo_layer_filters"]:
                geo_filters = Q(recipient_location_state_code__in=self.filters["geo_layer_filters"])
            values = ["recipient_location_state_code"]

        elif self.filters["geo_layer"] == GeoLayer.COUNTY.value:
            if self.filters["geo_layer_filters"]:
                geo_layers_list = [
                    {"state": fips_to_code.get(x[:2]), self.filters["geo_layer"]: x[2:], "country": "USA"}
                    for x in self.filters["geo_layer_filters"]
                ]
            geo_filters = geocode_filter_locations("recipient_location", geo_layers_list)
            values = [
                "recipient_location_state_code",
                "recipient_location_county_code",
                "recipient_location_county_name",
            ]

        else:  # GeoLayer.DISTRICT.value
            if self.filters["geo_layer_filters"]:
                geo_layers_list = [
                    {"state": fips_to_code.get(x[:2]), self.filters["geo_layer"]: x[2:], "country": "USA"}
                    for x in self.filters["geo_layer_filters"]
                ]
                geo_filters = geocode_filter_locations("recipient_location", geo_layers_list)
            values = ["recipient_location_state_code", "recipient_location_congressional_code"]

        if self.filters["spending_type"] == "loan":
            self.queryset = self.loan_queryset.filter(geo_filters).values(*values, "amount")
        else:
            raise NotImplementedError

        from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query

        print(f"=======================================\nQueryset: {generate_raw_quoted_query(self.queryset)}")
        self.results = list(self.queryset)

        if self.filters["geo_layer"] == GeoLayer.STATE.value:
            results = self.state_results("recipient_location_state_code")
        elif self.filters["geo_layer"] == GeoLayer.COUNTY.value:
            results = self.county_results("recipient_location_state_code", "recipient_location_county_name")
        else:
            results = self.district_results("recipient_location_state_code")

        return Response({"results": results})

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

    @property
    def loan_queryset(self):
        filters = [
            Q(award_id=OuterRef("award_id")),
            Q(disaster_emergency_fund__in=self.def_codes),
            Q(award_id__isnull=False),
        ]
        filters.extend(
            submission_window_cutoff(
                self.reporting_period_min,
                self.last_closed_monthly_submission_dates,
                self.last_closed_quarterly_submission_dates,
            )
        )

        annotations = {
            "amount": Coalesce(Sum("face_value_loan_guarantee"), 0),
            "district": F("recipient_location_congressional_code"),
            "county": F("recipient_location_county_code"),
            "state": F("recipient_location_state_code"),
        }
        # fields = [
        #     "recipient_location_congressional_code",
        #     "recipient_location_country_code",
        #     "recipient_location_state_code",
        #     "recipient_location_county_code",
        #     "recipient_location_county_name",
        # ]

        return (
            LoanAwardSearchMatview.objects.annotate(
                include=Exists(FinancialAccountsByAwards.objects.filter(*filters).values("award_id"))
            )
            .filter(include=True, recipient_location_country_code="USA")
            .values(
                "recipient_location_congressional_code",
                "recipient_location_county_code",
                "recipient_location_state_code",
            )
            .annotate(**annotations)
            .values(*annotations.keys())
        )
