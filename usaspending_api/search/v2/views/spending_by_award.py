import copy

from sys import maxsize
from django.conf import settings
from django.db.models import F
from rest_framework.response import Response
from rest_framework.views import APIView
from elasticsearch_dsl import A, Search

from usaspending_api.awards.models import Award
from usaspending_api.awards.v2.filters.filter_helpers import add_date_range_comparison_types
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter_determine_award_matview_model
from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.lookups.lookups import (
    assistance_type_mapping,
    contract_subaward_mapping,
    contract_type_mapping,
    grant_subaward_mapping,
    idv_type_mapping,
    loan_type_mapping,
    non_loan_assistance_type_mapping,
    procurement_type_mapping,
)
from usaspending_api.awards.v2.lookups.matview_lookups import (
    award_contracts_mapping,
    award_idv_mapping,
    loan_award_mapping,
    non_loan_assistance_award_mapping,
)
from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.elasticsearch.client import es_client_query
from usaspending_api.common.experimental_api_flags import is_experimental_elasticsearch_api
from usaspending_api.common.helpers.api_helper import raise_if_award_types_not_valid_subset, raise_if_sort_key_not_valid
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.common.recipient_lookups import annotate_recipient_id, annotate_prime_award_recipient_id


GLOBAL_MAP = {
    "award": {
        "minimum_db_fields": {"award_id", "piid", "fain", "uri", "type"},
        "api_to_db_mapping_list": [
            award_contracts_mapping,
            award_idv_mapping,
            loan_award_mapping,
            non_loan_assistance_award_mapping,
        ],
        "award_semaphore": "type",
        "award_id_fields": ["piid", "fain", "uri"],
        "internal_id_fields": {"internal_id": "award_id"},
        "generated_award_field": ("generated_internal_id", "internal_id"),
        "type_code_to_field_map": {
            **{award_type: award_contracts_mapping for award_type in contract_type_mapping},
            **{award_type: award_idv_mapping for award_type in idv_type_mapping},
            **{award_type: loan_award_mapping for award_type in loan_type_mapping},
            **{award_type: non_loan_assistance_award_mapping for award_type in non_loan_assistance_type_mapping},
        },
        "annotations": {"_recipient_id": annotate_recipient_id},
        "filter_queryset_func": matview_search_filter_determine_award_matview_model,
    },
    "subaward": {
        "minimum_db_fields": {"subaward_number", "piid", "fain", "award_type", "award_id"},
        "api_to_db_mapping_list": [contract_subaward_mapping, grant_subaward_mapping],
        "award_semaphore": "award_type",
        "award_id_fields": ["award__piid", "award__fain"],
        "internal_id_fields": {"internal_id": "subaward_number", "prime_award_internal_id": "award_id"},
        "generated_award_field": ("prime_award_generated_internal_id", "prime_award_internal_id"),
        "type_code_to_field_map": {"procurement": contract_subaward_mapping, "grant": grant_subaward_mapping},
        "annotations": {"_prime_award_recipient_id": annotate_prime_award_recipient_id},
        "filter_queryset_func": subaward_filter,
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
        json_request = self.validate_request_data(request.data)
        self.is_subaward = json_request["subawards"]
        self.constants = GLOBAL_MAP["subaward"] if self.is_subaward else GLOBAL_MAP["award"]
        self.filters = add_date_range_comparison_types(
            json_request.get("filters"), self.is_subaward, gte_date_type="action_date", lte_date_type="date_signed"
        )
        self.fields = json_request["fields"]
        self.pagination = {
            "limit": json_request["limit"],
            "lower_bound": (json_request["page"] - 1) * json_request["limit"],
            "page": json_request["page"],
            "sort_key": json_request.get("sort") or self.fields[0],
            "sort_order": json_request["order"],
            "upper_bound": json_request["page"] * json_request["limit"] + 1,
        }
        self.elasticsearch = is_experimental_elasticsearch_api(request)

        if self.elasticsearch and not self.subawards:
            logger.info("Using experimental Elasticsearch functionality for 'spending_over_time'")
            results = self.query_elasticsearch()


        if self.if_no_intersection():  # Like an exception, but API response is a HTTP 200 with a JSON payload
            return Response(self.populate_response(results=[], has_next=False))
        raise_if_award_types_not_valid_subset(self.filters["award_type_codes"], self.is_subaward)
        raise_if_sort_key_not_valid(self.pagination["sort_key"], self.fields, self.is_subaward)

        return Response(self.create_response(self.construct_queryset()))

    @staticmethod
    def validate_request_data(request_data):
        models = [
            {"name": "fields", "key": "fields", "type": "array", "array_type": "text", "text_type": "search", "min": 1},
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
            {
                "name": "object_class",
                "key": "filter|object_class",
                "type": "array",
                "array_type": "text",
                "text_type": "search",
            },
            {
                "name": "program_activity",
                "key": "filter|program_activity",
                "type": "array",
                "array_type": "integer",
                "array_max": maxsize,
            },
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        for m in models:
            if m["name"] in ("award_type_codes", "fields"):
                m["optional"] = False

        return TinyShield(models).block(request_data)

    def if_no_intersection(self):
        # "Special case" behavior: there will never be results when the website provides this value
        return "no intersection" in self.filters["award_type_codes"]

    def construct_queryset(self):
        sort_by_fields = self.get_sort_by_fields()
        database_fields = self.get_database_fields()
        base_queryset = self.constants["filter_queryset_func"](self.filters)
        queryset = self.annotate_queryset(base_queryset)
        queryset = self.custom_queryset_order_by(queryset, sort_by_fields, self.pagination["sort_order"])
        return queryset.values(*list(database_fields))[self.pagination["lower_bound"] : self.pagination["upper_bound"]]

    def create_response(self, queryset):
        results = []
        for record in queryset[: self.pagination["limit"]]:
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

        return self.populate_response(results=results, has_next=len(queryset) > self.pagination["limit"])

    def add_award_generated_id_field(self, records):
        """Obtain the generated_unique_award_id and add to response"""
        dest, source = self.constants["generated_award_field"]
        internal_ids = [record[source] for record in records]
        award_ids = Award.objects.filter(id__in=internal_ids).values_list("id", "generated_unique_award_id")
        award_ids = {internal_id: guai for internal_id, guai in award_ids}
        for record in records:
            record[dest] = award_ids.get(record[source])  # defensive, in case there is a discrepancy
        return records

    def get_sort_by_fields(self):
        if self.pagination["sort_key"] == "Award ID":
            sort_by_fields = self.constants["award_id_fields"]
        elif self.is_subaward:
            if set(self.filters["award_type_codes"]) <= set(procurement_type_mapping):
                sort_by_fields = [contract_subaward_mapping[self.pagination["sort_key"]]]
            elif set(self.filters["award_type_codes"]) <= set(assistance_type_mapping):
                sort_by_fields = [grant_subaward_mapping[self.pagination["sort_key"]]]
        else:
            if set(self.filters["award_type_codes"]) <= set(contract_type_mapping):
                sort_by_fields = [award_contracts_mapping[self.pagination["sort_key"]]]
            elif set(self.filters["award_type_codes"]) <= set(loan_type_mapping):
                sort_by_fields = [loan_award_mapping[self.pagination["sort_key"]]]
            elif set(self.filters["award_type_codes"]) <= set(idv_type_mapping):
                sort_by_fields = [award_idv_mapping[self.pagination["sort_key"]]]
            elif set(self.filters["award_type_codes"]) <= set(non_loan_assistance_type_mapping):
                sort_by_fields = [non_loan_assistance_award_mapping[self.pagination["sort_key"]]]

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
        """ Explicitly set NULLS LAST in the ordering to encourage the usage of the indexes."""
        if order == "desc":
            order_by_list = [F(field).desc(nulls_last=True) for field in sort_field_names]
        else:
            order_by_list = [F(field).asc(nulls_last=True) for field in sort_field_names]

        return queryset.order_by(*order_by_list)

    def populate_response(self, results: list, has_next: bool) -> dict:
        return {
            "limit": self.pagination["limit"],
            "results": results,
            "page_metadata": {"page": self.pagination["page"], "hasNext": has_next},
        }

    def query_elasticsearch(self, time_periods: list) -> list:
        filter_query = QueryWithFilters.generate_elasticsearch_query(self.filters)
        search = Search(index=f"{settings.ES_AWARDS_QUERY_ALIAS_PREFIX}*").filter(filter_query)
        self.apply_elasticsearch_aggregations(search)
        response = es_client_query(search=search)
        return self.build_elasticsearch_result(response.aggs, time_periods)