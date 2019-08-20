import copy
import json

from django.conf import settings
from django.db.models import F
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter_determine_award_matview_model
from usaspending_api.awards.v2.filters.sub_award import subaward_filter
from usaspending_api.awards.v2.lookups.lookups import (
    assistance_type_mapping,
    contract_subaward_mapping,
    contract_type_mapping,
    direct_payment_type_mapping,
    grant_subaward_mapping,
    grant_type_mapping,
    idv_type_mapping,
    loan_type_mapping,
    non_loan_assistance_type_mapping,
    other_type_mapping,
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
from usaspending_api.common.exceptions import (
    InvalidParameterException,
    NoIntersectionException,
    UnprocessableEntityException,
)
from usaspending_api.common.helpers.orm_helpers import award_types_are_valid_groups, subaward_types_are_valid_groups
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield


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
        "internal_id_field": "award_id",
        "type_code_to_field_map": {
            **{award_type: award_contracts_mapping for award_type in contract_type_mapping},
            **{award_type: award_idv_mapping for award_type in award_idv_mapping},
            **{award_type: loan_award_mapping for award_type in loan_type_mapping},
            **{award_type: non_loan_assistance_award_mapping for award_type in non_loan_assistance_type_mapping},
        },
        "filter_queryset_func": matview_search_filter_determine_award_matview_model,
    },
    "subaward": {
        "minimum_db_fields": {"subaward_number", "piid", "fain", "award_type"},
        "api_to_db_mapping_list": [contract_subaward_mapping, grant_subaward_mapping],
        "award_semaphore": "award_type",
        "award_id_fields": ["award__piid", "award__fain"],
        "internal_id_field": "subaward_number",
        "type_code_to_field_map": {"procurement": contract_subaward_mapping, "grant": grant_subaward_mapping},
        "filter_queryset_func": subaward_filter,
    },
}


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByAwardVisualizationViewSet(APIView):
    """
    This route takes award filters and fields, and returns the fields of the filtered awards.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/search/AdvancedSearch.md"

    @cache_response()
    def post(self, request):
        """Return all awards matching the provided filters and limits"""
        json_request = self.validate_request_data(request.data)
        self.is_subaward = json_request["subawards"]
        self.constants = GLOBAL_MAP["subaward"] if self.is_subaward else GLOBAL_MAP["award"]
        self.filters = json_request["filters"]
        self.fields = json_request["fields"]
        self.pagination = {
            "limit": json_request["limit"],
            "lower_bound": (json_request["page"] - 1) * json_request["limit"],
            "page": json_request["page"],
            "sort_key": json_request.get("sort") or self.fields[0],
            "sort_order": json_request["order"],
            "upper_bound": json_request["page"] * json_request["limit"] + 1,
        }

        self.raise_if_no_intersection()  # Raises exception, but API response is a HTTP 200 with a JSON payload
        self.raise_if_award_types_not_valid_subset(self.filters["award_type_codes"], self.is_subaward)
        self.raise_if_sort_key_not_valid(self.pagination["sort_key"], self.fields, self.is_subaward)

        return Response(self.create_response(self.construct_queryset()))

    @staticmethod
    def validate_request_data(request_data):
        models = [
            {"name": "fields", "key": "fields", "type": "array", "array_type": "text", "text_type": "search", "min": 1},
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        for m in models:
            if m["name"] in ("award_type_codes", "fields"):
                m["optional"] = False

        return TinyShield(models).block(request_data)

    @staticmethod
    def raise_if_award_types_not_valid_subset(award_type_codes, is_subaward=False):
        """Test to ensure the award types are a subset of only one group.

        For Awards: contracts, idvs, direct_payments, loans, grants, other assistance
        For Sub-Awards: procurement, assistance

        If the types are not a valid subset:
            Raise API exception with a JSON response describing the award type groupings
        """
        if is_subaward:
            if not subaward_types_are_valid_groups(award_type_codes):
                # Return a JSON response describing the award type groupings
                error_msg = (
                    '{{"message": "\'award_type_codes\' array can only contain types codes of one group.",'
                    '"award_type_groups": {{'
                    '"procurement": {},'
                    '"assistance": {}}}}}'
                ).format(json.dumps(procurement_type_mapping), json.dumps(assistance_type_mapping))
                raise UnprocessableEntityException(json.loads(error_msg))

        else:
            if not award_types_are_valid_groups(award_type_codes):
                error_msg = (
                    '{{"message": "\'award_type_codes\' array can only contain types codes of one group.",'
                    '"award_type_groups": {{'
                    '"contracts": {},'
                    '"loans": {},'
                    '"idvs": {},'
                    '"grants": {},'
                    '"other_financial_assistance": {},'
                    '"direct_payments": {}}}}}'
                ).format(
                    json.dumps(contract_type_mapping),
                    json.dumps(loan_type_mapping),
                    json.dumps(idv_type_mapping),
                    json.dumps(grant_type_mapping),
                    json.dumps(direct_payment_type_mapping),
                    json.dumps(other_type_mapping),
                )
                raise UnprocessableEntityException(json.loads(error_msg))

    @staticmethod
    def raise_if_sort_key_not_valid(sort_key, field_list, is_subaward=False):
        """Test to ensure sort key is present for the group of Awards or Sub-Awards

        Raise API exception if sort key is not present
        """
        msg_prefix = ""
        if is_subaward:
            msg_prefix = "Sub-"
            field_external_name_list = list(contract_subaward_mapping.keys()) + list(grant_subaward_mapping.keys())
        else:
            field_external_name_list = (
                list(award_contracts_mapping.keys())
                + list(loan_award_mapping.keys())
                + list(non_loan_assistance_award_mapping.keys())
                + list(award_idv_mapping.keys())
            )

        if sort_key not in field_external_name_list:
            raise InvalidParameterException(
                "Sort value '{}' not found in {}Award mappings: {}".format(
                    sort_key, msg_prefix, field_external_name_list
                )
            )

        if sort_key not in field_list:
            raise InvalidParameterException(
                "Sort value '{}' not found in requested fields: {}".format(sort_key, field_list)
            )

    def raise_if_no_intersection(self):
        # "Special case" behavior: there will never be results when the website provides this value
        if "no intersection" in self.filters["award_type_codes"]:
            raise NoIntersectionException(json.loads(json.dumps(self.populate_response(results=[], has_next=False))))

    def construct_queryset(self):
        sort_by_fields = self.get_sort_by_fields()
        database_fields = self.get_database_fields()
        base_queryset = self.constants["filter_queryset_func"](self.filters)
        queryset = self.custom_queryset_order_by(base_queryset, sort_by_fields, self.pagination["sort_order"])
        return queryset.values(*list(database_fields))[self.pagination["lower_bound"] : self.pagination["upper_bound"]]

    def create_response(self, queryset):
        results = []
        for record in queryset[: self.pagination["limit"]]:
            row = {"internal_id": record[self.constants["internal_id_field"]]}

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

        return self.populate_response(results=results, has_next=len(queryset) > self.pagination["limit"])

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
