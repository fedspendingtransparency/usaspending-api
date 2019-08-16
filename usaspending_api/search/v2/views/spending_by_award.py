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
from usaspending_api.common.helpers.orm_helpers import obtain_view_from_award_group
from usaspending_api.common.validator.award_filter import AWARD_FILTER
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield


@api_transformations(api_version=settings.API_VERSION, function_list=API_TRANSFORM_FUNCTIONS)
class SpendingByAwardVisualizationViewSet(APIView):
    """
    This route takes award filters and fields, and returns the fields of the filtered awards.
    """

    endpoint_doc = "usaspending_api/api_docs/api_documentation/advanced_award_search/spending_by_award.md"

    @cache_response()
    def post(self, request):
        """Return all awards matching the provided filters and limits"""
        models = [
            {"name": "fields", "key": "fields", "type": "array", "array_type": "text", "text_type": "search", "min": 1},
            {"name": "subawards", "key": "subawards", "type": "boolean", "default": False},
        ]
        models.extend(copy.deepcopy(AWARD_FILTER))
        models.extend(copy.deepcopy(PAGINATION))
        for m in models:
            if m["name"] in ("award_type_codes", "fields"):
                m["optional"] = False

        json_request = TinyShield(models).block(request.data)
        self.fields = json_request["fields"]
        filters = json_request.get("filters", {})
        subawards = json_request["subawards"]
        self.order = json_request["order"]
        self.limit = json_request["limit"]
        self.page = json_request["page"]
        self.lower_bound = (self.page - 1) * self.limit
        self.upper_bound = self.page * self.limit + 1

        if "no intersection" in filters["award_type_codes"]:
            # "Special case": there will never be results when the website provides this value
            return Response(
                {"limit": self.limit, "results": [], "page_metadata": {"page": self.page, "hasNext": False}}
            )

        self.sort = json_request.get("sort") or self.fields[0]
        if self.sort not in self.fields:
            raise InvalidParameterException(
                "Sort value '{}' not found in requested fields: {}".format(self.sort, self.fields)
            )

        if subawards:
            return Response(self._handle_subawards(filters))

        return Response(self._handle_awards(filters))

    def _handle_awards(self, filters):
        """ Award logic for SpendingByAward"""
        try:
            obtain_view_from_award_group(filters["award_type_codes"])
        except Exception:

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

        awards_values = (
            list(award_contracts_mapping.keys())
            + list(loan_award_mapping.keys())
            + list(non_loan_assistance_award_mapping.keys())
            + list(award_idv_mapping.keys())
        )

        if self.sort not in awards_values:
            raise InvalidParameterException(
                "Sort value '{}' not found in Award mappings: {}".format(self.sort, awards_values)
            )

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
        if set(filters["award_type_codes"]) <= set(contract_type_mapping):  # contracts
            sort_filters = [award_contracts_mapping[self.sort]]
        elif set(filters["award_type_codes"]) <= set(loan_type_mapping):  # loans
            sort_filters = [loan_award_mapping[self.sort]]
        elif set(filters["award_type_codes"]) <= set(idv_type_mapping):  # idvs
            sort_filters = [award_idv_mapping[self.sort]]
        else:  # assistance data
            sort_filters = [non_loan_assistance_award_mapping[self.sort]]

        # Explictly set NULLS LAST in the ordering to encourage the usage of the indexes
        if self.sort == "Award ID":
            if self.order == "desc":
                queryset = queryset.order_by(
                    F("piid").desc(nulls_last=True), F("fain").desc(nulls_last=True), F("uri").desc(nulls_last=True)
                ).values(*list(values))
            else:
                queryset = queryset.order_by(
                    F("piid").asc(nulls_last=True), F("fain").asc(nulls_last=True), F("uri").asc(nulls_last=True)
                ).values(*list(values))
        elif self.order == "desc":
            queryset = queryset.order_by(F(sort_filters[0]).desc(nulls_last=True)).values(*list(values))
        else:
            queryset = queryset.order_by(F(sort_filters[0]).asc(nulls_last=True)).values(*list(values))

        limited_queryset = queryset[self.lower_bound : self.upper_bound]
        has_next = len(limited_queryset) > self.limit

        results = []
        for award in limited_queryset[: self.limit]:
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

        return {"limit": self.limit, "results": results, "page_metadata": {"page": self.page, "hasNext": has_next}}

    def _handle_subawards(self, filters):
        """ sub-award logic for SpendingByAward"""
        # Test to ensure the award types are a proper subset of at least one group (contracts or grants)
        if set(filters["award_type_codes"]) - set(procurement_type_mapping.keys()) and set(
            filters["award_type_codes"]
        ) - set(assistance_type_mapping.keys()):
            error_msg = (
                '{{"message": "\'award_type_codes\' array can only contain types codes of one group.",'
                '"award_type_groups": {{'
                '"procurement": {},'
                '"assistance": {}}}}}'
            ).format(json.dumps(procurement_type_mapping), json.dumps(assistance_type_mapping))
            raise UnprocessableEntityException(json.loads(error_msg))

        subawards_values = list(contract_subaward_mapping.keys()) + list(grant_subaward_mapping.keys())
        if self.sort not in subawards_values:
            raise InvalidParameterException(
                "Sort value '{}' not found in Sub-Award mappings: {}".format(self.sort, subawards_values)
            )

        queryset = subaward_filter(filters)
        values = {"subaward_number", "piid", "fain", "award_type"}

        for field in self.fields:
            if contract_subaward_mapping.get(field):
                values.add(contract_subaward_mapping.get(field))
            if grant_subaward_mapping.get(field):
                values.add(grant_subaward_mapping.get(field))

        if set(filters["award_type_codes"]) <= set(procurement_type_mapping):
            sort_filters = [contract_subaward_mapping[self.sort]]
        elif set(filters["award_type_codes"]) <= set(assistance_type_mapping):
            sort_filters = [grant_subaward_mapping[self.sort]]
        else:
            msg = "Sort key '{}' doesn't exist for the selected Award group".format(self.sort)
            raise InvalidParameterException(msg)

        if self.sort == "Award ID":
            if self.order == "desc":
                queryset = queryset.order_by(
                    F("award__piid").desc(nulls_last=True), F("award__fain").desc(nulls_last=True)
                ).values(*list(values))
            else:
                queryset = queryset.order_by(
                    F("award__piid").asc(nulls_last=True), F("award__fain").asc(nulls_last=True)
                ).values(*list(values))
        elif self.order == "desc":
            queryset = queryset.order_by(F(sort_filters[0]).desc(nulls_last=True)).values(*list(values))
        else:
            queryset = queryset.order_by(F(sort_filters[0]).asc(nulls_last=True)).values(*list(values))

        limited_queryset = queryset[self.lower_bound : self.upper_bound]
        has_next = len(limited_queryset) > self.limit

        results = []
        for award in limited_queryset[: self.limit]:
            row = {"internal_id": award["subaward_number"]}

            if award["award_type"] == "procurement":
                for field in self.fields:
                    row[field] = award.get(contract_subaward_mapping[field])
            elif award["award_type"] == "grant":
                for field in self.fields:
                    row[field] = award.get(grant_subaward_mapping[field])
            results.append(row)

        return {"limit": self.limit, "results": results, "page_metadata": {"page": self.page, "hasNext": has_next}}
