import copy
import json
import logging
from ast import literal_eval
from sys import maxsize
from typing import (
    Any,
    List,
)

from django.conf import settings
from django.db.models import F
from django.utils.text import slugify
from elasticsearch_dsl import Q as ES_Q
from elasticsearch_dsl.response import Response as ES_Response
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.models import Award
from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import (
    CONTRACT_SOURCE_LOOKUP,
    IDV_SOURCE_LOOKUP,
    LOAN_SOURCE_LOOKUP,
    NON_LOAN_ASST_SOURCE_LOOKUP,
    contracts_mapping,
    idv_mapping,
    loan_mapping,
    non_loan_assist_mapping,
)
from usaspending_api.awards.v2.lookups.lookups import (
    SUBAWARD_MAPPING_LOOKUP,
    contract_type_mapping,
    idv_type_mapping,
    loan_type_mapping,
    non_loan_assistance_type_mapping,
    subaward_mapping,
)
from usaspending_api.common.api_versioning import API_TRANSFORM_FUNCTIONS, api_transformations
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch, SubawardSearch
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.common.helpers.api_helper import raise_if_award_types_not_valid_subset, raise_if_sort_key_not_valid
from usaspending_api.common.helpers.data_constants import state_name_from_code
from usaspending_api.common.helpers.generic_helper import (
    get_generic_filters_message,
)
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.recipient_lookups import annotate_prime_award_recipient_id
from usaspending_api.common.validator.award_filter import AWARD_FILTER_NO_RECIPIENT_ID
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.recipient.models import RecipientProfile
from usaspending_api.references.helpers import get_def_codes_by_group
from usaspending_api.references.models import Agency
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.filters.time_period.decorators import NewAwardsOnlyTimePeriod
from usaspending_api.search.filters.time_period.query_types import AwardSearchTimePeriod, SubawardSearchTimePeriod
from usaspending_api.search.v2.views.enums import SpendingLevel
from usaspending_api.submissions.models import SubmissionAttributes

logger = logging.getLogger(__name__)

GLOBAL_MAP = {
    "awards": {
        "award_semaphore": "type",
        "internal_id_fields": {"internal_id": "award_id"},
        "elasticsearch_type_code_to_field_map": {
            **dict.fromkeys(contract_type_mapping, CONTRACT_SOURCE_LOOKUP),
            **dict.fromkeys(idv_type_mapping, IDV_SOURCE_LOOKUP),
            **dict.fromkeys(loan_type_mapping, LOAN_SOURCE_LOOKUP),
            **dict.fromkeys(non_loan_assistance_type_mapping, NON_LOAN_ASST_SOURCE_LOOKUP),
        },
    },
    "subawards": {
        "minimum_db_fields": {"subaward_number", "piid", "fain", "prime_award_group", "award_id"},
        "api_to_db_mapping_list": [SUBAWARD_MAPPING_LOOKUP],
        "award_semaphore": "prime_award_group",
        "award_id_fields": ["award__piid", "award__fain"],
        "internal_id_fields": {"internal_id": "subaward_number", "prime_award_internal_id": "award_id"},
        "generated_award_field": ("prime_award_generated_internal_id", "prime_award_internal_id"),
        "type_code_to_field_map": {"procurement": SUBAWARD_MAPPING_LOOKUP, "grant": SUBAWARD_MAPPING_LOOKUP},
        "annotations": {"_prime_award_recipient_id": annotate_prime_award_recipient_id},
        "filter_queryset_func": subaward_filter,
        "elasticsearch_type_code_to_field_map": SUBAWARD_MAPPING_LOOKUP,
    },
}


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByAwardVisualizationViewSet(APIView):
    """
    This route takes award filters and fields, and returns the fields of the filtered awards.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/search/spending_by_award.md"

    @cache_response()
    def post(self, request):
        """Return all awards matching the provided filters and limits"""
        self.original_filters = request.data.get("filters")
        json_request, models = self.validate_request_data(request.data)
        self.spending_level = SpendingLevel(json_request["spending_level"])
        self.constants = GLOBAL_MAP[self.spending_level.value]
        filters = json_request.get("filters", {})
        self.filters = filters
        self.fields = json_request["fields"]
        self.pagination = {
            "limit": json_request["limit"],
            "lower_bound": (json_request["page"] - 1) * json_request["limit"],
            "page": json_request["page"],
            "sort_key": json_request.get("sort") or self.fields[0],
            "sort_order": json_request["order"],
            "upper_bound": json_request["page"] * json_request["limit"] + 1,
        }

        if self.if_no_intersection():  # Like an exception, but API response is a HTTP 200 with a JSON payload
            return Response(self.populate_response(results=[], has_next=False, models=models))

        raise_if_award_types_not_valid_subset(self.filters["award_type_codes"], self.spending_level)

        # These are the objects returned rather than a single field
        if self.pagination["sort_key"] not in [
            "NAICS",
            "PSC",
            "Recipient Location",
            "Primary Place of Performance",
            "Assistance Listing",
            "Sub-Recipient Location",
            "Sub-Award Primary Place of Performance",
        ]:
            raise_if_sort_key_not_valid(
                self.pagination["sort_key"], self.fields, self.filters["award_type_codes"], self.spending_level
            )

        self.last_record_unique_id = json_request.get("last_record_unique_id")
        self.last_record_sort_value = json_request.get("last_record_sort_value")

        self.spending_by_defc_lookup = {
            "COVID-19 Obligations": {"group_name": "covid_19", "field_type": "obligation"},
            "COVID-19 Outlays": {"group_name": "covid_19", "field_type": "outlay"},
            "Infrastructure Obligations": {"group_name": "infrastructure", "field_type": "obligation"},
            "Infrastructure Outlays": {"group_name": "infrastructure", "field_type": "outlay"},
        }
        self.def_codes_by_group = (
            get_def_codes_by_group(list({val["group_name"] for val in self.spending_by_defc_lookup.values()}))
            if set(self.fields) & set(self.spending_by_defc_lookup)
            else {}
        )
        if self.filters.get("def_codes"):
            self.def_codes_by_group = {
                group_name: list(set(def_codes) & set(self.filters["def_codes"]))
                for group_name, def_codes in self.def_codes_by_group.items()
            }

        if self.spending_level == SpendingLevel.SUBAWARD:
            return Response(self.construct_es_response_for_subawards(self.query_elasticsearch_subawards()))
        else:
            return Response(self.construct_es_response_for_prime_awards(self.query_elasticsearch_awards()))

    @staticmethod
    def validate_request_data(request_data):
        spending_type_models = [
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
            {
                "name": "spending_level",
                "key": "spending_level",
                "type": "enum",
                "enum_values": [SpendingLevel.AWARD.value, SpendingLevel.SUBAWARD.value],
                "optional": True,
                "default": SpendingLevel.AWARD.value,
            },
        ]

        subaward_ts = TinyShield(spending_type_models)
        tiny_shield_response = subaward_ts.block(request_data)
        if tiny_shield_response["subawards"]:
            request_data["spending_level"] = SpendingLevel.SUBAWARD.value
        else:
            # In the case of the user not supplying the spending_level we grab the default defined by TinyShield
            request_data["spending_level"] = tiny_shield_response["spending_level"]

        program_activities_rule = {
            "name": "program_activities",
            "type": "array",
            "key": "filters|program_activities",
            "array_type": "object",
            "object_keys_min": 1,
            "object_keys": {
                "name": {"type": "text", "text_type": "search"},
                "code": {
                    "type": "integer",
                },
            },
        }
        models = [
            {"name": "fields", "key": "fields", "type": "array", "array_type": "text", "text_type": "search", "min": 1},
            {
                "name": "program_activity",
                "key": "filter|program_activity",
                "type": "array",
                "array_type": "integer",
                "array_max": maxsize,
            },
            {
                "name": "last_record_unique_id",
                "key": "last_record_unique_id",
                "type": "integer",
                "required": False,
                "allow_nulls": True,
            },
            {
                "name": "last_record_sort_value",
                "key": "last_record_sort_value",
                "type": "text",
                "text_type": "search",
                "required": False,
                "allow_nulls": True,
            },
            program_activities_rule,
        ]
        models.extend(copy.deepcopy(AWARD_FILTER_NO_RECIPIENT_ID))
        models.extend(copy.deepcopy(PAGINATION))
        models.extend(copy.deepcopy(spending_type_models))
        for m in models:
            if m["name"] in ("award_type_codes", "fields"):
                m["optional"] = False
            elif request_data["spending_level"] == SpendingLevel.SUBAWARD.value and m["name"] == "time_period":
                m["object_keys"]["date_type"]["enum_values"] = [
                    "action_date",
                    "last_modified_date",
                    "date_signed",
                    "sub_action_date",
                ]
        tiny_shield = TinyShield(models)
        if "filters" in request_data and "program_activities" in request_data["filters"]:
            tiny_shield.enforce_object_keys_min(request_data, program_activities_rule)
        return tiny_shield.block(request_data), models

    def if_no_intersection(self):
        # "Special case" behavior: there will never be results when the website provides this value
        return "no intersection" in self.filters["award_type_codes"]

    def create_response_for_subawards(self, queryset, models):
        results = []
        rows = list(queryset)
        for record in rows[: self.pagination["limit"]]:
            row = {k: record[v] for k, v in self.constants["internal_id_fields"].items()}

            for field in self.fields:
                row[field] = record.get(
                    self.constants["type_code_to_field_map"][record[self.constants["award_semaphore"]]].get(field)
                )

            if "Award ID" in self.fields:
                for id_type in self.constants["award_id_fields"]:
                    if record[id_type]:
                        row["Award ID"] = record[id_type]
                        break
            results.append(row)

        results = self.add_award_generated_id_field(results)

        return self.populate_response(results=results, has_next=len(rows) > self.pagination["limit"], models=models)

    def add_award_generated_id_field(self, records):
        """Obtain the generated_unique_award_id and add to response"""
        dest, source = self.constants["generated_award_field"]
        internal_ids = [record[source] for record in records]
        award_ids = Award.objects.filter(id__in=internal_ids).values_list("id", "generated_unique_award_id")
        award_ids = {internal_id: guai for internal_id, guai in award_ids}
        for record in records:
            record[dest] = award_ids.get(record[source])  # defensive, in case there is a discrepancy
        return records

    def get_elastic_sort_by_fields(self):
        match self.pagination["sort_key"]:
            case "Award ID" | "Sub-Award ID":
                sort_by_fields = ["display_award_id"]
            case "NAICS":
                sort_by_fields = (
                    [contracts_mapping["sub_naics_code"], contracts_mapping["naics_description"]]
                    if self.spending_level == SpendingLevel.SUBAWARD
                    else [contracts_mapping["naics_code"], contracts_mapping["naics_description"]]
                )
            case "PSC":
                sort_by_fields = [contracts_mapping["psc_code"], contracts_mapping["psc_description"]]
            case "Recipient Location":
                sort_by_fields = [
                    contracts_mapping["recipient_location_city_name"],
                    contracts_mapping["recipient_location_state_code"],
                    contracts_mapping["recipient_location_country_name"],
                    contracts_mapping["recipient_location_address_line1"],
                    contracts_mapping["recipient_location_address_line2"],
                    contracts_mapping["recipient_location_address_line3"],
                ]
            case "Primary Place of Performance":
                sort_by_fields = [
                    contracts_mapping["pop_city_name"],
                    contracts_mapping["pop_state_code"],
                    contracts_mapping["pop_country_name"],
                ]
            case "Assistance Listings":
                sort_by_fields = [contracts_mapping["cfda_number"], contracts_mapping["cfda_program_title"]]
            case "Assistance Listing":
                sort_by_fields = [contracts_mapping["cfda_number"], contracts_mapping["sub_cfda_program_titles"]]
            case "Sub-Recipient Location":
                sort_by_fields = [
                    contracts_mapping["sub_recipient_location_city_name"],
                    contracts_mapping["sub_recipient_location_state_code"],
                    contracts_mapping["sub_recipient_location_country_name"],
                    contracts_mapping["sub_recipient_location_address_line1"],
                ]
            case "Sub-Award Primary Place of Performance":
                sort_by_fields = [
                    contracts_mapping["sub_pop_city_name"],
                    contracts_mapping["sub_pop_state_code"],
                    contracts_mapping["sub_pop_country_name"],
                ]
            # TODO: Add additional field for Award Descriptions in case they exceed the keyword limit like subawards
            case "Sub-Award Description":
                sort_by_fields = [subaward_mapping["subaward_description_sorted"]]
            case _:
                if self.spending_level == SpendingLevel.SUBAWARD:
                    sort_by_fields = [subaward_mapping[self.pagination["sort_key"]]]
                elif set(self.filters["award_type_codes"]) <= set(contract_type_mapping):
                    sort_by_fields = [contracts_mapping[self.pagination["sort_key"]]]
                elif set(self.filters["award_type_codes"]) <= set(loan_type_mapping):
                    sort_by_fields = [loan_mapping[self.pagination["sort_key"]]]
                elif set(self.filters["award_type_codes"]) <= set(idv_type_mapping):
                    sort_by_fields = [idv_mapping[self.pagination["sort_key"]]]
                elif set(self.filters["award_type_codes"]) <= set(non_loan_assistance_type_mapping):
                    sort_by_fields = [non_loan_assist_mapping[self.pagination["sort_key"]]]

        sort_by_fields.append("award_id")

        return sort_by_fields

    def get_database_fields(self):
        values = copy.copy(self.constants["minimum_db_fields"])
        for field in self.fields:
            for mapping in self.constants["api_to_db_mapping_list"]:
                if mapping.get(field):
                    values.add(mapping.get(field))
        return values

    def annotate_queryset(self, queryset):
        for field, function in self.constants["annotations"].items():
            queryset = function(field, queryset)
        return queryset

    def custom_queryset_order_by(self, queryset, sort_field_names, order):
        """Explicitly set NULLS LAST in the ordering to encourage the usage of the indexes."""
        if order == "desc":
            order_by_list = [F(field).desc(nulls_last=True) for field in sort_field_names]
        else:
            order_by_list = [F(field).asc(nulls_last=True) for field in sort_field_names]

        return queryset.order_by(*order_by_list)

    def populate_response(self, results: list, has_next: bool, models: List[dict]) -> dict:
        return {
            "limit": self.pagination["limit"],
            "results": results,
            "page_metadata": {"page": self.pagination["page"], "hasNext": has_next},
            "messages": get_generic_filters_message(self.original_filters.keys(), [elem["name"] for elem in models]),
        }

    def query_elasticsearch(
        self, base_search: AwardSearch | SubawardSearch, filter_query: ES_Q, sorts: list[dict[str, str]]
    ) -> ES_Response:
        record_num = (self.pagination["page"] - 1) * self.pagination["limit"]
        # random page jumping was removed due to performance concerns
        if (self.last_record_sort_value is None and self.last_record_unique_id is not None) or (
            self.last_record_sort_value is not None and self.last_record_unique_id is None
        ):
            # malformed request
            raise Exception(
                "Using search_after functionality in Elasticsearch requires both"
                " last_record_sort_value and last_record_unique_id."
            )
        if record_num >= settings.ES_AWARDS_MAX_RESULT_WINDOW and (
            self.last_record_unique_id is None and self.last_record_sort_value is None
        ):
            raise UnprocessableEntityException(
                f"Page #{self.pagination['page']} with limit {self.pagination['limit']} is over the maximum result"
                f" limit {settings.ES_AWARDS_MAX_RESULT_WINDOW}. Please provide the 'last_record_sort_value' and"
                " 'last_record_unique_id' to paginate sequentially."
            )
        # Search_after values are provided in the API request - use search after
        if self.last_record_sort_value is not None and self.last_record_unique_id is not None:
            search = (
                base_search.filter(filter_query)
                .sort(*sorts)
                .extra(search_after=[self.last_record_sort_value, self.last_record_unique_id])[
                    : self.pagination["limit"] + 1
                ]  # add extra result to check for next page
            )
        # no values, within result window, use regular elasticsearch
        else:
            search = base_search.filter(filter_query).sort(*sorts)[record_num : record_num + self.pagination["limit"]]

        response = search.handle_execute()

        return response

    def query_elasticsearch_awards(self) -> ES_Response:
        filter_options = {}
        time_period_obj = AwardSearchTimePeriod(
            default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
        )
        new_awards_only_decorator = NewAwardsOnlyTimePeriod(
            time_period_obj=time_period_obj, query_type=QueryType.AWARDS
        )
        filter_options["time_period_obj"] = new_awards_only_decorator
        query_with_filters = QueryWithFilters(QueryType.AWARDS)
        filter_query = query_with_filters.generate_elasticsearch_query(self.filters, **filter_options)
        sort_field = self.get_elastic_sort_by_fields()

        if "spending_by_defc" in sort_field:
            sort_field.remove("spending_by_defc")
            group_name = self.spending_by_defc_lookup[self.pagination["sort_key"]]["group_name"]
            field_type = self.spending_by_defc_lookup[self.pagination["sort_key"]]["field_type"]
            defc_for_group = self.def_codes_by_group[group_name]
            sorts = [
                {
                    f"spending_by_defc.{field_type}": {
                        "mode": "sum",
                        "order": self.pagination["sort_order"],
                        "nested": {
                            "path": "spending_by_defc",
                            "filter": {
                                "terms": {
                                    "spending_by_defc.defc": defc_for_group,
                                }
                            },
                        },
                    }
                }
            ]
            sorts.extend([{field: self.pagination["sort_order"]} for field in sort_field])
        elif "Recipient Location" in self.pagination["sort_key"]:
            sorts = []
            for field in sort_field:
                if "recipient_location_address" in field:
                    sorts.append({field: {"order": self.pagination["sort_order"], "unmapped_type": "keyword"}})
                else:
                    sorts.append({field: self.pagination["sort_order"]})
        else:
            sorts = [{field: self.pagination["sort_order"]} for field in sort_field]

        return self.query_elasticsearch(AwardSearch(), filter_query, sorts)

    def query_elasticsearch_subawards(self) -> ES_Response:
        filter_options = {}
        time_period_obj = SubawardSearchTimePeriod(
            default_end_date=settings.API_MAX_DATE, default_start_date=settings.API_SEARCH_MIN_DATE
        )
        filter_options["time_period_obj"] = time_period_obj

        query_with_filters = QueryWithFilters(QueryType.SUBAWARDS)
        filter_query = query_with_filters.generate_elasticsearch_query(self.filters, **filter_options)

        sort_field = self.get_elastic_sort_by_fields()

        sorts = [{field: self.pagination["sort_order"]} for field in sort_field]

        return self.query_elasticsearch(SubawardSearch(), filter_query, sorts)

    def _get_lookup_data(self) -> dict[str, dict[str, str | int]]:
        # { <toptier_code>: {"id": <id>, "slug": <slug>} }
        agency_lookup: dict[str, dict[str, str | int]] = {
            ag["code"]: {"id": ag["id"], "slug": slugify(ag["name"])}
            for ag in (
                Agency.objects.filter(
                    toptier_flag=True,
                    toptier_agency__toptier_code__in=SubmissionAttributes.objects.values("toptier_code"),
                )
                .select_related("toptieragencypublisheddabsview")
                .annotate(code=F("toptier_agency__toptier_code"), name=F("toptieragencypublisheddabsview__name"))
                .values("id", "code", "name")
                .all()
            )
        }

        return agency_lookup

    def construct_es_response_for_prime_awards(self, response: ES_Response) -> dict:
        results = []

        if len(response) == 0:
            return self.construct_es_response(results, response)

        agency_lookup = self._get_lookup_data()
        should_return_display_award_id = "Award ID" in self.fields
        should_return_recipient_id = "recipient_id" in self.fields
        for res in response:
            hit = res.to_dict()
            row = {k: hit[v] for k, v in self.constants["internal_id_fields"].items()}

            # Parsing API response values from ES query result JSON
            # We parse the `hit` (result from elasticsearch) to get the award type, use the type to determine
            # which lookup dict to use, and then use that lookup to retrieve the correct value requested from `fields`
            for field in self.fields:
                row[field] = hit.get(
                    self.constants["elasticsearch_type_code_to_field_map"][hit[self.constants["award_semaphore"]]].get(
                        field
                    )
                )
                if "Awarding Agency" in self.fields:
                    row["agency_code"] = hit["awarding_toptier_agency_code"]

            row["internal_id"] = int(row["internal_id"])
            if row.get("Loan Value"):
                row["Loan Value"] = float(row["Loan Value"])
            if row.get("Subsidy Cost"):
                row["Subsidy Cost"] = float(row["Subsidy Cost"])
            if row.get("Award Amount"):
                row["Award Amount"] = float(row["Award Amount"])
            if row.get("Total Outlays"):
                row["Total Outlays"] = float(row["Total Outlays"])
            if row.get("Awarding Agency"):
                code = str(row.pop("agency_code")).zfill(3)
                row["awarding_agency_id"] = agency_lookup.get(code, {}).get("id", None)
                row["agency_slug"] = agency_lookup.get(code, {}).get("slug", None)
            if row.get("def_codes"):
                if self.filters.get("def_codes"):
                    row["def_codes"] = list(filter(lambda x: x in self.filters.get("def_codes"), row["def_codes"]))

            if row.get("Assistance Listings"):
                row["Assistance Listings"] = list(map(json.loads, row["Assistance Listings"]))

            if "Recipient Location" in self.fields:
                row["Recipient Location"] = {
                    "location_country_code": hit.get("recipient_location_country_code"),
                    "country_name": hit.get("recipient_location_country_name"),
                    "state_code": hit.get("recipient_location_state_code"),
                    "state_name": state_name_from_code(hit.get("recipient_location_state_code")),
                    "city_name": hit.get("recipient_location_city_name"),
                    "county_code": hit.get("recipient_location_county_code"),
                    "county_name": hit.get("recipient_location_county_name"),
                    "address_line1": hit.get("recipient_location_address_line1"),
                    "address_line2": hit.get("recipient_location_address_line2"),
                    "address_line3": hit.get("recipient_location_address_line3"),
                    "congressional_code": hit.get("recipient_location_congressional_code"),
                    "zip4": hit.get("recipient_location_zip4"),
                    "zip5": hit.get("recipient_location_zip5"),
                    "foreign_postal_code": hit.get("recipient_location_foreign_postal_code"),
                    "foreign_province": hit.get("recipient_location_foreign_province"),
                }

            if "Primary Place of Performance" in self.fields:
                row["Primary Place of Performance"] = {
                    "location_country_code": hit.get("pop_country_code"),
                    "country_name": hit.get("pop_country_name"),
                    "state_code": hit.get("pop_state_code"),
                    "state_name": state_name_from_code(hit.get("pop_state_code")),
                    "city_name": hit.get("pop_city_name"),
                    "county_code": hit.get("pop_county_code"),
                    "county_name": hit.get("pop_county_name"),
                    "congressional_code": hit.get("pop_congressional_code"),
                    "zip4": hit.get("pop_zip4"),
                    "zip5": hit.get("pop_zip5"),
                }

            if "NAICS" in self.fields:
                row["NAICS"] = {
                    "code": hit.get("naics_code"),
                    "description": hit.get("naics_description"),
                }

            if "PSC" in self.fields:
                row["PSC"] = {
                    "code": hit.get("product_or_service_code"),
                    "description": hit.get("product_or_service_description"),
                }

            if "primary_assistance_listing" in self.fields:
                row["primary_assistance_listing"] = {
                    "cfda_number": hit.get("cfda_number"),
                    "cfda_program_title": hit.get("cfda_title"),
                }

            row["generated_internal_id"] = hit["generated_unique_award_id"]

            if should_return_display_award_id:
                row["Award ID"] = hit["display_award_id"]

            if should_return_recipient_id:
                row["recipient_id"] = self.get_recipient_hash_with_level(hit)

            row.update(self.get_defc_row_values(row))

            results.append(row)

        return self.construct_es_response(results, response)

    def construct_es_response_for_subawards(self, response: ES_Response) -> dict[str, Any]:
        results = []
        for res in response:
            hit = res.to_dict()
            row = {k: hit[v] for k, v in self.constants["internal_id_fields"].items()}

            # Parsing API response values from ES query result JSON
            # We parse the `hit` (result from elasticsearch) to get the award type, use the type to determine
            # which lookup dict to use, and then use that lookup to retrieve the correct value requested from `fields`
            for field in self.fields:
                row[field] = hit.get(self.constants["elasticsearch_type_code_to_field_map"].get(field))

            results.append(self.calculate_complex_fields(row, hit))

        return self.construct_es_response(results, response)

    def calculate_complex_fields(self, row, hit):
        if row.get("Sub-Award Amount"):
            row["Sub-Award Amount"] = float(row["Sub-Award Amount"])

        if "NAICS" in self.fields:
            row["NAICS"] = {
                "code": hit.get("naics"),
                "description": hit.get("naics_description"),
            }

        if "PSC" in self.fields:
            row["PSC"] = {
                "code": hit.get("product_or_service_code"),
                "description": hit.get("product_or_service_description"),
            }

        if "Assistance Listing" in self.fields:
            row["Assistance Listing"] = {
                "cfda_number": hit.get("cfda_number"),
                "cfda_program_title": hit.get("cfda_titles"),
            }

        if "Sub-Recipient Location" in self.fields:
            if hit.get("sub_recipient_location_zip"):
                match len(hit.get("sub_recipient_location_zip")):
                    case 9:
                        zip4 = hit.get("sub_recipient_location_zip")[5:]
                    case _:
                        zip4 = None
            else:
                zip4 = None

            row["Sub-Recipient Location"] = {
                "location_country_code": hit.get("sub_recipient_location_country_code"),
                "country_name": hit.get("sub_recipient_location_country_name"),
                "state_code": hit.get("sub_recipient_location_state_code"),
                "state_name": state_name_from_code(hit.get("sub_recipient_location_state_code")),
                "city_name": hit.get("sub_recipient_location_city_name"),
                "county_code": hit.get("sub_recipient_location_county_code"),
                "county_name": hit.get("sub_recipient_location_county_name"),
                "address_line1": hit.get("sub_recipient_location_address_line1"),
                "congressional_code": hit.get("sub_recipient_location_congressional_code"),
                "zip4": zip4,
                "zip5": hit.get("sub_recipient_location_zip5"),
                "foreign_postal_code": hit.get("sub_recipient_location_foreign_posta"),
            }

        if "Sub-Award Primary Place of Performance" in self.fields:
            if hit.get("sub_pop_zip"):
                match len(hit.get("sub_pop_zip")):
                    case 9:
                        zip4 = hit.get("sub_pop_zip")[5:]
                        zip5 = hit.get("sub_pop_zip")[0:5]
                    case 5:
                        zip4 = None
                        zip5 = hit.get("sub_pop_zip")
                    case _:
                        zip4 = None
                        zip5 = None
            else:
                zip4 = None
                zip5 = None

            row["Sub-Award Primary Place of Performance"] = {
                "location_country_code": hit.get("sub_pop_country_code"),
                "country_name": hit.get("sub_pop_country_name"),
                "state_code": hit.get("sub_pop_state_code"),
                "state_name": state_name_from_code(hit.get("sub_pop_state_code")),
                "city_name": hit.get("sub_pop_city_name"),
                "county_code": hit.get("sub_pop_county_code"),
                "county_name": hit.get("sub_pop_county_name"),
                "congressional_code": hit.get("sub_pop_congressional_code"),
                "zip4": zip4,
                "zip5": zip5,
            }

        if (
            "sub_award_recipient_id" in self.fields
            and hit.get("subaward_recipient_hash") is not None
            and hit.get("subaward_recipient_level") is not None
        ):
            row["sub_award_recipient_id"] = f'{hit["subaward_recipient_hash"]}-{hit["subaward_recipient_level"]}'

        row["prime_award_generated_internal_id"] = hit["unique_award_key"]

        return row

    def construct_es_response(self, results: list[dict[str, Any]] | list, response: ES_Response) -> dict[str, Any]:
        last_record_unique_id = None
        last_record_sort_value = None
        offset = 1
        if self.last_record_unique_id is not None:
            has_next = len(results) > self.pagination["limit"]
            offset = 2
        else:
            has_next = (
                response.hits.total.value - (self.pagination["page"] - 1) * self.pagination["limit"]
                > self.pagination["limit"]
            )

        if len(response) > 0 and has_next:
            last_record_unique_id = response[len(response) - offset].meta.sort[1]
            last_record_sort_value = response[len(response) - offset].meta.sort[0]

        return {
            "spending_level": self.spending_level.value,
            "limit": self.pagination["limit"],
            "results": results[: self.pagination["limit"]],
            "page_metadata": {
                "page": self.pagination["page"],
                "hasNext": has_next,
                "last_record_unique_id": last_record_unique_id,
                "last_record_sort_value": str(last_record_sort_value),
            },
            "messages": get_generic_filters_message(
                self.original_filters.keys(), [elem["name"] for elem in AWARD_FILTER_NO_RECIPIENT_ID]
            ),
        }

    def get_recipient_hash_with_level(self, award_doc):
        recipient_info = award_doc.get("recipient_agg_key").split("/")
        recipient_hash = recipient_info[0] if recipient_info else None
        if len(recipient_info) > 1 and recipient_info[1].upper() != "NONE":
            recipient_levels = literal_eval(recipient_info[1])
        else:
            recipient_levels = []

        if recipient_hash is None or len(recipient_levels) == 0:
            return None

        recipient_level = RecipientProfile.return_one_level(recipient_levels)

        return f"{recipient_hash}-{recipient_level}"

    def get_defc_row_values(self, row: dict[str, any]) -> dict[str, any]:
        """
        Fields powered by DEFC outlays and obligations all come from the same spending_by_defc field. The final outlay
        and obligation total for each field depends on the DEFC that fall under the specific group represented by each
        field. If the specific DEFC based fields are part of the row then they will be returned for the row to update.
        """
        result = {
            field: sum(
                [
                    spending_by_defc[lookup_values["field_type"]]
                    for spending_by_defc in row[field]
                    if spending_by_defc["defc"] in self.def_codes_by_group[lookup_values["group_name"]]
                ]
            )
            for field, lookup_values in self.spending_by_defc_lookup.items()
            if field in row and row[field] is not None
        }
        return result
