import copy
import json

from django.conf import settings
from django.db.models import F
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
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
from usaspending_api.common.exceptions import InvalidParameterException, UnprocessableEntityException
from usaspending_api.common.helpers.orm_helpers import (
    obtain_view_from_award_group,
    award_types_are_valid_groups,
    subaward_types_are_valid_groups,
)
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield


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
        filters = json_request["filters"]
        subawards = json_request["subawards"]
        self.fields = json_request["fields"]
        self.pagination = {
            "limit": json_request["limit"],
            "lower_bound": (json_request["page"] - 1) * json_request["limit"],
            "page": json_request["page"],
            "sort_key": json_request.get("sort") or self.fields[0],
            "sort_order": json_request["order"],
            "upper_bound": json_request["page"] * json_request["limit"] + 1,
        }

        self._response_dict = {
            "limit": self.pagination["limit"],
            "results": None,
            "page_metadata": {"page": self.pagination["page"], "hasNext": None},
        }

        if "no intersection" in filters["award_type_codes"]:
            # "Special case": there will never be results when the website provides this value
            self.populate_response(results=[], has_next=False)
            return Response(self._response_dict)

        self.raise_if_award_types_not_valid_subset(filters["award_type_codes"], subawards)
        self.raise_if_sort_key_not_valid(self.pagination["sort_key"], self.fields, subawards)

        if subawards:
            self._handle_subawards(filters)
        else:
            self._handle_awards(filters)

        return Response(self._response_dict)

    def _handle_awards(self, filters):
        """ Award logic for SpendingByAward"""

        # build sql query filters
        model = obtain_view_from_award_group(filters["award_type_codes"])
        queryset = matview_search_filter(filters, model).values()
        values = {"award_id", "piid", "fain", "uri", "type"}

        for field in self.fields:
            if award_contracts_mapping.get(field):
                values.add(award_contracts_mapping.get(field))
            if loan_award_mapping.get(field):
                values.add(loan_award_mapping.get(field))
            if non_loan_assistance_award_mapping.get(field):
                values.add(non_loan_assistance_award_mapping.get(field))
            if award_idv_mapping.get(field):
                values.add(award_idv_mapping.get(field))

        # Modify queryset to be ordered by requested "sort" in the request or default value(s)
        if self.pagination["sort_key"] == "Award ID":
            sort_by_fields = ["piid", "fain", "uri"]
        else:
            if set(filters["award_type_codes"]) <= set(contract_type_mapping):
                sort_filter = award_contracts_mapping[self.pagination["sort_key"]]
            elif set(filters["award_type_codes"]) <= set(loan_type_mapping):
                sort_filter = loan_award_mapping[self.pagination["sort_key"]]
            elif set(filters["award_type_codes"]) <= set(idv_type_mapping):
                sort_filter = award_idv_mapping[self.pagination["sort_key"]]
            else:  # assistance data
                sort_filter = non_loan_assistance_award_mapping[self.pagination["sort_key"]]
            sort_by_fields = [sort_filter]

        queryset = self.custom_queryset_order_by(queryset, sort_by_fields, self.pagination["sort_order"])
        queryset = queryset.values(*list(values))

        limited_queryset = queryset[self.pagination["lower_bound"] : self.pagination["upper_bound"]]
        has_next = len(limited_queryset) > self.pagination["limit"]

        results = []
        for award in limited_queryset[: self.pagination["limit"]]:
            row = {"internal_id": award["award_id"]}

            if award["type"] in loan_type_mapping:
                for field in self.fields:
                    row[field] = award.get(loan_award_mapping.get(field))
            elif award["type"] in non_loan_assistance_type_mapping:
                for field in self.fields:
                    row[field] = award.get(non_loan_assistance_award_mapping.get(field))
            elif award["type"] in idv_type_mapping:
                for field in self.fields:
                    row[field] = award.get(award_idv_mapping.get(field))
            elif award["type"] in contract_type_mapping:
                for field in self.fields:
                    row[field] = award.get(award_contracts_mapping.get(field))

            if "Award ID" in self.fields:
                for id_type in ["piid", "fain", "uri"]:
                    if award[id_type]:
                        row["Award ID"] = award[id_type]
                        break

            results.append(row)

        self.populate_response(results=results, has_next=has_next)

    def _handle_subawards(self, filters):
        """ sub-award logic for SpendingByAward"""

        queryset = subaward_filter(filters)
        values = {"subaward_number", "piid", "fain", "award_type"}

        for field in self.fields:
            if contract_subaward_mapping.get(field):
                values.add(contract_subaward_mapping.get(field))
            if grant_subaward_mapping.get(field):
                values.add(grant_subaward_mapping.get(field))

        # Modify queryset to be ordered by requested "sort" in the request or default value(s)
        if self.pagination["sort_key"] == "Award ID":
            sort_by_fields = ["award__piid", "award__fain"]
        else:
            if set(filters["award_type_codes"]) <= set(procurement_type_mapping):
                sort_filter = contract_subaward_mapping[self.pagination["sort_key"]]
            elif set(filters["award_type_codes"]) <= set(assistance_type_mapping):
                sort_filter = grant_subaward_mapping[self.pagination["sort_key"]]
            else:
                msg = "Sort key '{}' doesn't exist for the selected Award group".format(self.pagination["sort_key"])
                raise InvalidParameterException(msg)

            sort_by_fields = [sort_filter]

        queryset = self.custom_queryset_order_by(queryset, sort_by_fields, self.pagination["sort_order"])
        queryset = queryset.values(*list(values))

        limited_queryset = queryset[self.pagination["lower_bound"] : self.pagination["upper_bound"]]
        has_next = len(limited_queryset) > self.pagination["limit"]

        results = []
        for award in limited_queryset[: self.pagination["limit"]]:
            row = {"internal_id": award["subaward_number"]}

            if award["award_type"] == "procurement":
                for field in self.fields:
                    row[field] = award.get(contract_subaward_mapping[field])
            elif award["award_type"] == "grant":
                for field in self.fields:
                    row[field] = award.get(grant_subaward_mapping[field])
            results.append(row)

        self.populate_response(results=results, has_next=has_next)

    def populate_response(self, results, has_next):
        self._response_dict["results"] = results
        self._response_dict["page_metadata"]["hasNext"] = has_next

    def custom_queryset_order_by(self, queryset, sort_field_names, order):
        """ Explicitly set NULLS LAST in the ordering to encourage the usage of the indexes."""

        if order == "desc":
            order_by_list = [F(field).desc(nulls_last=True) for field in sort_field_names]
        else:
            order_by_list = [F(field).asc(nulls_last=True) for field in sort_field_names]

        return queryset.order_by(*order_by_list)

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
        """
            Test to ensure the award types are a subset of only one group.
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
        """
            Test to ensure sort key is present for the group of Awards or Sub-Awards
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
