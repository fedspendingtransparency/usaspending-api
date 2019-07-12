import logging
import copy

from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from django.db.models import Sum, FloatField
from django.db.models.functions import Cast
from django.conf import settings

from usaspending_api.awards.models_matviews import SubawardView
from usaspending_api.awards.v2.filters.location_filter_geocode import geocode_filter_locations
from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.filters.view_selector import spending_by_geography
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.abbreviations import code_to_state, fips_to_code, pad_codes


logger = logging.getLogger(__name__)
API_VERSION = settings.API_VERSION


@api_transformations(api_version=API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByGeographyVisualizationViewSet(APIView):
    """
        This route takes award filters, and returns spending by state code, county code, or congressional district code.
        endpoint_doc: /advanced_award_search/spending_by_geography.md
    """

    geo_layer = None  # State, county or District
    geo_layer_filters = None  # Specific geo_layers to filter on
    queryset = None  # Transaction queryset
    geo_queryset = None  # Aggregate queryset based on scope

    @cache_response()
    def post(self, request):
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
        models.extend(copy.deepcopy(PAGINATION))
        json_request = TinyShield(models).block(request.data)

        self.subawards = json_request["subawards"]
        self.scope = json_request["scope"]
        self.filters = json_request.get("filters", None)
        self.geo_layer = json_request["geo_layer"]
        self.geo_layer_filters = json_request.get("geo_layer_filters", None)

        fields_list = []  # fields to include in the aggregate query

        loc_dict = {"state": "state_code", "county": "county_code", "district": "congressional_code"}

        model_dict = {
            "place_of_performance": "pop",
            "recipient_location": "recipient_location",
            # 'subawards_place_of_performance': 'pop',
            # 'subawards_recipient_location': 'recipient_location'
        }

        # Build the query based on the scope fields and geo_layers
        # Fields not in the reference objects above then request is invalid

        scope_field_name = model_dict.get(self.scope)
        loc_field_name = loc_dict.get(self.geo_layer)
        loc_lookup = "{}_{}".format(scope_field_name, loc_field_name)

        if self.subawards:
            # We do not use matviews for Subaward filtering, just the Subaward download filters
            self.queryset = subaward_filter(self.filters)
            self.model_name = SubawardView
        else:
            self.queryset, self.model_name = spending_by_geography(self.filters)

        if self.geo_layer == "state":
            # State will have one field (state_code) containing letter A-Z
            column_isnull = "generated_pragmatic_obligation__isnull"
            if self.subawards:
                column_isnull = "amount__isnull"
            kwargs = {"{}_country_code".format(scope_field_name): "USA", column_isnull: False}

            # Only state scope will add its own state code
            # State codes are consistent in database i.e. AL, AK
            fields_list.append(loc_lookup)

            state_response = {
                "scope": self.scope,
                "geo_layer": self.geo_layer,
                "results": self.state_results(kwargs, fields_list, loc_lookup),
            }

            return Response(state_response)

        else:
            # County and district scope will need to select multiple fields
            # State code is needed for county/district aggregation
            state_lookup = "{}_{}".format(scope_field_name, loc_dict["state"])
            fields_list.append(state_lookup)

            # Adding regex to county/district codes to remove entries with letters since can't be surfaced by map
            kwargs = {"{}__isnull".format("amount" if self.subawards else "generated_pragmatic_obligation"): False}

            if self.geo_layer == "county":
                # County name added to aggregation since consistent in db
                county_name_lookup = "{}_county_name".format(scope_field_name)
                fields_list.append(county_name_lookup)
                self.county_district_queryset(kwargs, fields_list, loc_lookup, state_lookup, scope_field_name)

                county_response = {
                    "scope": self.scope,
                    "geo_layer": self.geo_layer,
                    "results": self.county_results(state_lookup, county_name_lookup),
                }

                return Response(county_response)
            else:
                self.county_district_queryset(kwargs, fields_list, loc_lookup, state_lookup, scope_field_name)

                district_response = {
                    "scope": self.scope,
                    "geo_layer": self.geo_layer,
                    "results": self.district_results(state_lookup),
                }

                return Response(district_response)

    def state_results(self, filter_args, lookup_fields, loc_lookup):
        # Adding additional state filters if specified
        if self.geo_layer_filters:
            self.queryset = self.queryset.filter(**{"{}__{}".format(loc_lookup, "in"): self.geo_layer_filters})
        else:
            # Adding null filter for state for specific partial index
            # when not using geocode_filter
            filter_args["{}__isnull".format(loc_lookup)] = False

        self.geo_queryset = self.queryset.filter(**filter_args).values(*lookup_fields)

        if self.subawards:
            self.geo_queryset = self.geo_queryset.annotate(transaction_amount=Sum("amount"))
        else:
            self.geo_queryset = self.geo_queryset.annotate(
                transaction_amount=Sum("generated_pragmatic_obligation")
            ).values("transaction_amount", *lookup_fields)
        # State names are inconsistent in database (upper, lower, null)
        # Used lookup instead to be consistent
        results = [
            {
                "shape_code": x[loc_lookup],
                "aggregated_amount": x["transaction_amount"],
                "display_name": code_to_state.get(x[loc_lookup], {"name": "None"}).get("name").title(),
            }
            for x in self.geo_queryset
        ]

        return results

    def county_district_queryset(self, kwargs, fields_list, loc_lookup, state_lookup, scope_field_name):
        # Filtering queryset to specific county/districts if requested
        # Since geo_layer_filters comes as concat of state fips and county/district codes
        # need to split for the geocode_filter
        if self.geo_layer_filters:
            geo_layers_list = [
                {"state": fips_to_code.get(x[:2]), self.geo_layer: x[2:], "country": "USA"}
                for x in self.geo_layer_filters
            ]
            self.queryset = self.queryset.filter(geocode_filter_locations(scope_field_name, geo_layers_list, True))
        else:
            # Adding null, USA, not number filters for specific partial index when not using geocode_filter
            kwargs["{}__{}".format(loc_lookup, "isnull")] = False
            kwargs["{}__{}".format(state_lookup, "isnull")] = False
            kwargs["{}_country_code".format(scope_field_name)] = "USA"
            kwargs["{}__{}".format(loc_lookup, "iregex")] = r"^[0-9]*(\.\d+)?$"

        # Turn county/district codes into float since inconsistent in database
        # Codes in location table ex: '01', '1', '1.0'
        # Cast will group codes as a float and will combine inconsistent codes
        self.geo_queryset = (
            self.queryset.filter(**kwargs).values(*fields_list).annotate(code_as_float=Cast(loc_lookup, FloatField()))
        )

        if self.subawards:
            self.geo_queryset = self.geo_queryset.annotate(transaction_amount=Sum("amount"))
        else:
            self.geo_queryset = self.geo_queryset.annotate(
                transaction_amount=Sum("generated_pragmatic_obligation")
            ).values("transaction_amount", "code_as_float", *fields_list)

        return self.geo_queryset

    def county_results(self, state_lookup, county_name):
        # Returns county results formatted for map
        results = [
            {
                "shape_code": code_to_state.get(x[state_lookup])["fips"]
                + pad_codes(self.geo_layer, x["code_as_float"]),
                "aggregated_amount": x["transaction_amount"],
                "display_name": x[county_name].title() if x[county_name] is not None else x[county_name],
            }
            for x in self.geo_queryset
        ]

        return results

    def district_results(self, state_lookup):
        # Returns congressional district results formatted for map
        results = [
            {
                "shape_code": code_to_state.get(x[state_lookup])["fips"]
                + pad_codes(self.geo_layer, x["code_as_float"]),
                "aggregated_amount": x["transaction_amount"],
                "display_name": x[state_lookup] + "-" + pad_codes(self.geo_layer, x["code_as_float"]),
            }
            for x in self.geo_queryset
        ]

        return results
