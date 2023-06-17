import copy
import logging

from decimal import Decimal
from enum import Enum

from django.conf import settings
from django.db.models import Sum, FloatField, QuerySet, F, Value, TextField
from django.db.models.functions import Cast, Concat
from elasticsearch_dsl import A, Q as ES_Q
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView
from typing import Optional, List, Dict

from usaspending_api.awards.v2.filters.sub_award import geocode_filter_subaward_locations, subaward_filter
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import TransactionSearch
from usaspending_api.common.helpers.generic_helper import (
    deprecated_district_field_in_location_object,
    get_generic_filters_message,
)
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.abbreviations import code_to_state, fips_to_code, pad_codes
from usaspending_api.references.models import PopCounty, PopCongressionalDistrict
from usaspending_api.search.models import SubawardSearch
from usaspending_api.search.v2.elasticsearch_helper import (
    get_scaled_sum_aggregations,
    get_number_of_unique_terms_for_transactions,
)
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod
from usaspending_api.search.filters.elasticsearch.filter import _QueryType
from usaspending_api.search.filters.time_period.query_types import TransactionSearchTimePeriod

logger = logging.getLogger(__name__)
API_VERSION = settings.API_VERSION


class GeoLayer(Enum):
    COUNTY = "county"
    DISTRICT = "district"
    STATE = "state"


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByGeographyVisualizationViewSet(APIView):
    """
    This route takes award filters, and returns spending by state code, county code, or congressional district code.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_geography.md"

    agg_key: Optional[str]
    filters: dict
    geo_layer: GeoLayer
    geo_layer_filters: Optional[List[str]]
    loc_field_name: str
    loc_lookup: str
    model_name: Optional[str]
    obligation_column: str
    queryset: Optional[QuerySet]
    scope_field_name: str
    subawards: bool

    @cache_response()
    def post(self, request: Request) -> Response:
        models = [
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
            {
                "name": "scope",
                "key": "scope",
                "type": "enum",
                "optional": False,
                "enum_values": ["place_of_performance", "recipient_location"],
            },
            {
                "name": "geo_layer",
                "key": "geo_layer",
                "type": "enum",
                "optional": False,
                "enum_values": ["state", "county", "district"],
            },
            {
                "name": "geo_layer_filters",
                "key": "geo_layer_filters",
                "type": "array",
                "array_type": "text",
                "text_type": "search",
            },
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        original_filters = request.data.get("filters")
        json_request = TinyShield(models).block(request.data)

        agg_key_dict = {
            "county": "county_agg_key",
            "district": "congressional_cur_agg_key",
            "state": "state_agg_key",
        }
        model_dict = {
            "place_of_performance": {"prime": "pop", "sub": "sub_place_of_perform"},
            "recipient_location": {"prime": "recipient_location", "sub": "sub_legal_entity"},
        }
        # Most of these are the same but some of slightly off, so we can track all the nuances here
        self.location_dict = {
            "code": {
                "country": {
                    "prime": {"pop": "country_co", "recipient_location": "country_code"},
                    "sub": {"sub_place_of_perform": "country_co", "sub_legal_entity": "country_code"},
                },
                "county": {
                    "prime": {"pop": "county_code", "recipient_location": "county_code"},
                    "sub": {"sub_place_of_perform": "county_code", "sub_legal_entity": "county_code"},
                },
                "district": {
                    "prime": {"pop": "congressional_code_current", "recipient_location": "congressional_code_current"},
                    "sub": {
                        "sub_place_of_perform": "sub_place_of_performance_congressional_current",
                        "sub_legal_entity": "congressional_current",
                    },
                },
                "state": {
                    "prime": {"pop": "state_code", "recipient_location": "state_code"},
                    "sub": {"sub_place_of_perform": "state_code", "sub_legal_entity": "state_code"},
                },
            },
            "name": {
                "country": {
                    "prime": {"pop": "country_na", "recipient_location": "country_name"},
                    "sub": {"sub_place_of_perform": "country_na", "sub_legal_entity": "country_name"},
                },
                "county": {
                    "sub": {"sub_place_of_perform": "county_name", "sub_legal_entity": "county_name"},
                },
                "state": {
                    "prime": {"pop": "state_name", "recipient_location": "state_name"},
                    "sub": {"sub_place_of_perform": "state_name", "sub_legal_entity": "state_name"},
                },
            },
        }

        self.subawards = json_request["subawards"]
        self.award_or_sub_str = "sub" if self.subawards else "prime"
        self.scope_field_name = model_dict[json_request["scope"]][self.award_or_sub_str]
        self.agg_key = f"{self.scope_field_name}_{agg_key_dict[json_request['geo_layer']]}"
        self.filters = json_request.get("filters")
        self.geo_layer = GeoLayer(json_request["geo_layer"])
        self.geo_layer_filters = json_request.get("geo_layer_filters")
        self.loc_field_name = self.location_dict["code"][self.geo_layer.value][self.award_or_sub_str][
            self.scope_field_name
        ]
        self.loc_lookup = f"{self.scope_field_name}_{self.loc_field_name}"

        if self.subawards:
            if self.geo_layer == GeoLayer.DISTRICT:
                self.loc_lookup = f"{self.loc_field_name}"
            # We do not use matviews for Subaward filtering, just the Subaward download filters
            self.model_name = SubawardSearch
            self.queryset = subaward_filter(self.filters)
            self.obligation_column = "subaward_amount"
            result = self.query_django()
        else:
            if self.scope_field_name == "pop":
                scope_filter_name = "place_of_performance_scope"
            else:
                scope_filter_name = "recipient_scope"

            # Only search for values within USA, but don't overwrite a user's search
            if scope_filter_name not in self.filters:
                self.filters[scope_filter_name] = "domestic"

            self.obligation_column = "generated_pragmatic_obligation"
            filter_options = {}
            time_period_obj = TransactionSearchTimePeriod(
                default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
            )
            new_awards_only_decorator = NewAwardsOnlyTimePeriod(
                time_period_obj=time_period_obj, query_type=_QueryType.TRANSACTIONS
            )
            filter_options["time_period_obj"] = new_awards_only_decorator
            filter_query = QueryWithFilters.generate_transactions_elasticsearch_query(self.filters, **filter_options)
            result = self.query_elasticsearch(filter_query)

        raw_response = {
            "scope": json_request["scope"],
            "geo_layer": self.geo_layer.value,
            "results": result,
            "messages": get_generic_filters_message(original_filters.keys(), [elem["name"] for elem in AWARD_FILTER]),
        }

        # Add filter field deprecation notices

        # TODO: To be removed in DEV-9966
        messages = raw_response.get("messages", [])
        deprecated_district_field_in_location_object(messages, original_filters)
        raw_response["messages"] = messages

        return Response(raw_response)

    def query_django(self) -> dict:
        fields_list = []  # fields to include in the aggregate query

        if self.geo_layer == GeoLayer.STATE:
            # State will have one field (state_code) containing letter A-Z
            column_isnull = f"{self.obligation_column}__isnull"

            cc_col = self.location_dict["code"]["country"][self.award_or_sub_str][self.scope_field_name]
            kwargs = {f"{self.scope_field_name}_{cc_col}": "USA", column_isnull: False}

            # Only state scope will add its own state code
            # State codes are consistent in database i.e. AL, AK
            fields_list.append(self.loc_lookup)

            return self.state_results(kwargs, fields_list, self.loc_lookup)

        else:
            # County and district scope will need to select multiple fields
            # State code is needed for county/district aggregation
            state_col = self.location_dict["code"]["state"][self.award_or_sub_str][self.scope_field_name]
            state_lookup = f"{self.scope_field_name}_{state_col}"
            fields_list.append(state_lookup)

            # Adding regex to county/district codes to remove entries with letters since can't be surfaced by map
            kwargs = {f"{self.obligation_column}__isnull": False}

            if self.geo_layer == GeoLayer.COUNTY:
                # County name added to aggregation since consistent in db

                county_col = self.location_dict["name"]["county"][self.award_or_sub_str][self.scope_field_name]
                county_name_lookup = f"{self.scope_field_name}_{county_col}"
                fields_list.append(county_name_lookup)
                geo_queryset = self.county_district_queryset(
                    kwargs, fields_list, self.loc_lookup, state_lookup, self.scope_field_name
                )

                return self.county_results(state_lookup, county_name_lookup, geo_queryset)

            else:
                geo_queryset = self.county_district_queryset(
                    kwargs, fields_list, self.loc_lookup, state_lookup, self.scope_field_name
                )

                return self.district_results(state_lookup, geo_queryset)

    def state_results(self, filter_args: Dict[str, str], lookup_fields: List[str], loc_lookup: str) -> List[dict]:
        # Adding additional state filters if specified
        if self.geo_layer_filters:
            self.queryset = self.queryset.filter(**{f"{loc_lookup}__in": self.geo_layer_filters})
        else:
            # Adding null filter for state for specific partial index
            # when not using geocode_filter
            filter_args[f"{loc_lookup}__isnull"] = False

        geo_queryset = self.queryset.filter(**filter_args).values(*lookup_fields)

        if self.subawards:
            geo_queryset = geo_queryset.annotate(transaction_amount=Sum("subaward_amount"))
        else:
            geo_queryset = geo_queryset.annotate(transaction_amount=Sum("generated_pragmatic_obligation")).values(
                "transaction_amount", *lookup_fields
            )

        state_pop_rows = PopCounty.objects.filter(county_number="000").values()
        populations = {row["state_name"].lower(): row["latest_population"] for row in state_pop_rows}

        # State names are inconsistent in database (upper, lower, null)
        # Used lookup instead to be consistent
        results = []
        for x in geo_queryset:
            shape_code = x[loc_lookup]
            per_capita = None
            population = populations.get(code_to_state.get(shape_code, {"name": "None"}).get("name").lower())
            if population:
                per_capita = (Decimal(x["transaction_amount"]) / Decimal(population)).quantize(Decimal(".01"))

            results.append(
                {
                    "shape_code": shape_code,
                    "aggregated_amount": x["transaction_amount"],
                    "display_name": code_to_state.get(shape_code, {"name": "None"}).get("name").title(),
                    "population": population,
                    "per_capita": per_capita,
                }
            )

        return results

    def county_district_queryset(
        self, kwargs: Dict[str, str], fields_list: List[str], loc_lookup: str, state_lookup: str, scope_field_name: str
    ) -> QuerySet:
        # Filtering queryset to specific county/districts if requested
        # Since geo_layer_filters comes as concat of state fips and county/district codes
        # need to split for the geocode_filter
        if self.geo_layer_filters:
            geo_layers_list = [
                {"state": fips_to_code.get(x[:2]), self.geo_layer.value: x[2:], "country": "USA"}
                for x in self.geo_layer_filters
            ]
            self.queryset = self.queryset.filter(geocode_filter_subaward_locations(scope_field_name, geo_layers_list))
        else:
            # Adding null, USA, not number filters for specific partial index when not using geocode_filter
            kwargs[f"{loc_lookup}__isnull"] = False
            kwargs[f"{state_lookup}__isnull"] = False
            kwargs[f"{scope_field_name}_country_code"] = "USA"
            kwargs[f"{loc_lookup}__iregex"] = r"^[0-9]*(\.\d+)?$"

        # Turn county/district codes into float since inconsistent in database
        # Codes in location table ex: '01', '1', '1.0'
        # Cast will group codes as a float and will combine inconsistent codes
        geo_queryset = (
            self.queryset.filter(**kwargs).values(*fields_list).annotate(code_as_float=Cast(loc_lookup, FloatField()))
        )

        if self.subawards:
            geo_queryset = geo_queryset.annotate(transaction_amount=Sum("subaward_amount"))
        else:
            geo_queryset = geo_queryset.annotate(transaction_amount=Sum("generated_pragmatic_obligation")).values(
                "transaction_amount", "code_as_float", *fields_list
            )

        return geo_queryset

    def county_results(self, state_lookup: str, county_name: str, geo_queryset: QuerySet) -> List[dict]:
        # Returns county results formatted for map
        state_pop_rows = PopCounty.objects.exclude(county_number="000").values()
        populations = {f"{row['state_code']}{row['county_number']}": row["latest_population"] for row in state_pop_rows}

        results = []
        for x in geo_queryset:
            shape_code = code_to_state.get(x[state_lookup])["fips"] + pad_codes(
                self.geo_layer.value, x["code_as_float"]
            )
            per_capita = None
            population = populations.get(shape_code)
            if population:
                per_capita = (Decimal(x["transaction_amount"]) / Decimal(population)).quantize(Decimal(".01"))

            results.append(
                {
                    "shape_code": shape_code,
                    "aggregated_amount": x["transaction_amount"],
                    "display_name": x[county_name].title() if x[county_name] is not None else x[county_name],
                    "population": population,
                    "per_capita": per_capita,
                }
            )

        return results

    def district_results(self, state_lookup: str, geo_queryset: QuerySet) -> List[dict]:
        # Returns congressional district results formatted for map

        state_pop_rows = PopCongressionalDistrict.objects.all().values()
        populations = {
            f"{row['state_code']}{row['congressional_district']}": row["latest_population"] for row in state_pop_rows
        }

        results = []
        for x in geo_queryset:
            shape_code = code_to_state.get(x[state_lookup])["fips"] + pad_codes(
                self.geo_layer.value, x["code_as_float"]
            )
            per_capita = None
            population = populations.get(shape_code)
            if population:
                per_capita = (Decimal(x["transaction_amount"]) / Decimal(population)).quantize(Decimal(".01"))

            results.append(
                {
                    "shape_code": shape_code,
                    "aggregated_amount": x["transaction_amount"],
                    "display_name": x[state_lookup] + "-" + pad_codes(self.geo_layer.value, x["code_as_float"]),
                    "population": population,
                    "per_capita": per_capita,
                }
            )

        return results

    def build_elasticsearch_search_with_aggregation(self, filter_query: ES_Q) -> Optional[TransactionSearch]:
        # Create the initial search using filters
        search = TransactionSearch().filter(filter_query)

        # Check number of unique terms (buckets) for performance and restrictions on maximum buckets allowed
        bucket_count = get_number_of_unique_terms_for_transactions(filter_query, f"{self.agg_key}.hash")

        if bucket_count == 0:
            return None

        # Add 100 to make sure that we consider enough records in each shard for accurate results
        group_by_agg_key = A("terms", field=self.agg_key, size=bucket_count, shard_size=bucket_count + 100)
        sum_aggregations = get_scaled_sum_aggregations(self.obligation_column)
        sum_field = sum_aggregations["sum_field"]

        search.aggs.bucket("group_by_agg_key", group_by_agg_key).metric("sum_field", sum_field)

        # Set size to 0 since we don't care about documents returned
        search.update_from_dict({"size": 0})

        return search

    def build_elasticsearch_result(self, response: dict) -> Dict[str, dict]:
        def _key_to_geo_code(key):
            return f"{code_to_state[key[:2]]['fips']}{key[2:]}" if (key and key[:2] in code_to_state) else None

        # Get the codes
        geo_info_buckets = response.get("group_by_agg_key", {}).get("buckets", [])
        geo_codes = [_key_to_geo_code(bucket["key"]) for bucket in geo_info_buckets if bucket.get("key")]

        # Get the current geo info
        current_geo_info = {}
        if self.geo_layer == GeoLayer.STATE:
            geo_info_query = (
                PopCounty.objects.filter(state_code__in=geo_codes, county_number="000")
                .annotate(
                    geo_code=F("state_code"),
                    display_name=F("state_name"),
                    population=F("latest_population"),
                    shape_code=F("state_code"),
                )
                .values("geo_code", "display_name", "population", "shape_code")
            )
        elif self.geo_layer == GeoLayer.COUNTY:
            geo_info_query = (
                PopCounty.objects.annotate(shape_code=Concat("state_code", "county_number", output_field=TextField()))
                .filter(shape_code__in=geo_codes)
                .annotate(
                    geo_code=F("county_number"),
                    display_name=F("county_name"),
                    population=F("latest_population"),
                )
                .values("geo_code", "display_name", "shape_code", "population")
            )
        else:
            geo_info_query = (
                PopCongressionalDistrict.objects.annotate(
                    shape_code=Concat("state_code", "congressional_district", output_field=TextField())
                )
                .filter(shape_code__in=geo_codes)
                .annotate(
                    geo_code=F("congressional_district"),
                    display_name=Concat(
                        "state_abbreviation", Value("-"), "congressional_district", output_field=TextField()
                    ),
                    population=F("latest_population"),
                )
                .values("geo_code", "display_name", "shape_code", "population")
            )
        for geo_info in geo_info_query.all():
            current_geo_info[geo_info["shape_code"]] = geo_info

        # Build out the results
        results = {}
        for bucket in geo_info_buckets:
            bucket_shape_code = _key_to_geo_code(bucket.get("key"))
            geo_info = current_geo_info.get(bucket_shape_code) or {"shape_code": ""}

            if geo_info["shape_code"]:
                if self.geo_layer == GeoLayer.STATE:
                    geo_info["display_name"] = geo_info["display_name"].title()
                    geo_info["shape_code"] = fips_to_code[geo_info["shape_code"]].upper()
                elif self.geo_layer == GeoLayer.COUNTY:
                    geo_info["display_name"] = geo_info["display_name"].title()
                else:
                    geo_info["display_name"] = geo_info["display_name"].upper()

            per_capita = None
            aggregated_amount = int(bucket.get("sum_field", {"value": 0})["value"]) / Decimal("100")
            population = geo_info.get("population")
            if population:
                per_capita = (Decimal(aggregated_amount) / Decimal(population)).quantize(Decimal(".01"))

            results[geo_info["shape_code"]] = {
                "shape_code": geo_info["shape_code"],
                "display_name": geo_info.get("display_name"),
                "aggregated_amount": aggregated_amount,
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
