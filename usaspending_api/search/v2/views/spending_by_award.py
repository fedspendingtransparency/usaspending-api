import copy

from sys import maxsize
from django.conf import settings
from django.db.models import F
from rest_framework.response import Response
from rest_framework.views import APIView

import logging
from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency
from usaspending_api.awards.v2.filters.filter_helpers import add_date_range_comparison_types
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
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import (
    contracts_mapping,
    idv_mapping,
    loan_mapping,
    non_loan_assist_mapping,
    CONTRACT_SOURCE_LOOKUP,
    IDV_SOURCE_LOOKUP,
    NON_LOAN_ASST_SOURCE_LOOKUP,
    LOAN_SOURCE_LOOKUP,
)
from usaspending_api.common.elasticsearch.search_wrappers import AwardSearch
from usaspending_api.recipient.v2.lookups import SPECIAL_CASES


from usaspending_api.common.api_versioning import api_transformations, API_TRANSFORM_FUNCTIONS
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.api_helper import raise_if_award_types_not_valid_subset, raise_if_sort_key_not_valid
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.common.query_with_filters import QueryWithFilters
from usaspending_api.common.helpers.generic_helper import get_generic_filters_message
from usaspending_api.common.validator.award_filter import AWARD_FILTER_NO_RECIPIENT_ID
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.common.recipient_lookups import annotate_prime_award_recipient_id
from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.submissions.models import SubmissionAttributes

logger = logging.getLogger(__name__)

GLOBAL_MAP = {
    "award": {
        "award_semaphore": "type",
        "internal_id_fields": {"internal_id": "award_id"},
        "elasticsearch_type_code_to_field_map": {
            **{award_type: CONTRACT_SOURCE_LOOKUP for award_type in contract_type_mapping},
            **{award_type: IDV_SOURCE_LOOKUP for award_type in idv_type_mapping},
            **{award_type: LOAN_SOURCE_LOOKUP for award_type in loan_type_mapping},
            **{award_type: NON_LOAN_ASST_SOURCE_LOOKUP for award_type in non_loan_assistance_type_mapping},
        },
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
        self.original_filters = request.data.get("filters")
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

        if self.if_no_intersection():  # Like an exception, but API response is a HTTP 200 with a JSON payload
            return Response(self.populate_response(results=[], has_next=False))

        raise_if_award_types_not_valid_subset(self.filters["award_type_codes"], self.is_subaward)
        raise_if_sort_key_not_valid(self.pagination["sort_key"], self.fields, self.is_subaward)

        if self.is_subaward:
            response = Response(self.create_response_for_subawards(self.construct_queryset()))
        else:
            self.last_record_unique_id = json_request.get("last_record_unique_id")
            self.last_record_sort_value = json_request.get("last_record_sort_value")
            response = Response(self.construct_es_response_for_prime_awards(self.query_elasticsearch()))
        return response

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
        ]
        models.extend(copy.deepcopy(AWARD_FILTER_NO_RECIPIENT_ID))
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

    def create_response_for_subawards(self, queryset):
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

        return self.populate_response(results=results, has_next=len(rows) > self.pagination["limit"])

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
        else:
            if set(self.filters["award_type_codes"]) <= set(procurement_type_mapping):
                sort_by_fields = [contract_subaward_mapping[self.pagination["sort_key"]]]
            elif set(self.filters["award_type_codes"]) <= set(assistance_type_mapping):
                sort_by_fields = [grant_subaward_mapping[self.pagination["sort_key"]]]

        return sort_by_fields

    def get_elastic_sort_by_fields(self):
        if self.pagination["sort_key"] == "Award ID":
            sort_by_fields = ["display_award_id"]
        else:
            if set(self.filters["award_type_codes"]) <= set(contract_type_mapping):
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
            "messages": get_generic_filters_message(
                self.original_filters.keys(), [elem["name"] for elem in AWARD_FILTER_NO_RECIPIENT_ID]
            ),
        }

    def query_elasticsearch(self) -> list:
        filter_query = QueryWithFilters.generate_awards_elasticsearch_query(self.filters)
        sort_field = self.get_elastic_sort_by_fields()
        sorts = [{field: self.pagination["sort_order"]} for field in sort_field]
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
                AwardSearch()
                .filter(filter_query)
                .sort(*sorts)
                .extra(search_after=[self.last_record_sort_value, self.last_record_unique_id])[
                    : self.pagination["limit"] + 1
                ]  # add extra result to check for next page
            )
        # no values, within result window, use regular elasticsearch
        else:
            search = AwardSearch().filter(filter_query).sort(*sorts)[record_num : record_num + self.pagination["limit"]]

        response = search.handle_execute()

        return response

    # For an unknown reason, ES tends to return the awarding agency toptier codes as integers or floats, instead of as
    # text. This function casts the code back to a string and appends any leading zeroes that were lost.
    def get_agency_database_id(self, code):
        if len(str(int(code))) < 3:
            code = "{zeroes}{code}".format(zeroes=("0" * (3 - len(str(int(code))))), code=int(code))
        agency_id = Agency.objects.filter(toptier_agency__toptier_code=code, toptier_flag=True).first()
        submission = SubmissionAttributes.objects.filter(toptier_code=code).first()
        if submission is None or agency_id is None:
            return None
        return agency_id.id

    def construct_es_response_for_prime_awards(self, response) -> dict:
        results = []
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
            if row.get("Awarding Agency"):
                code = row.pop("agency_code")
                row["awarding_agency_id"] = self.get_agency_database_id(code)
            if row.get("COVID-19 Obligations"):
                row["COVID-19 Obligations"] = sum(
                    [
                        x["obligation"]
                        if (self.filters.get("def_codes") is not None and x["defc"] in self.filters["def_codes"])
                        or self.filters.get("def_codes") is None
                        else 0
                        for x in row.get("COVID-19 Obligations")
                    ]
                )
            if row.get("COVID-19 Outlays"):
                row["COVID-19 Outlays"] = sum(
                    [
                        x["outlay"]
                        if (self.filters.get("def_codes") is not None and x["defc"] in self.filters["def_codes"])
                        or self.filters.get("def_codes") is None
                        else 0
                        for x in row.get("COVID-19 Outlays")
                    ]
                )
            row["generated_internal_id"] = hit["generated_unique_award_id"]
            row["recipient_id"] = hit.get("recipient_unique_id")
            row["parent_recipient_unique_id"] = hit.get("parent_recipient_unique_id")

            if "Award ID" in self.fields:
                row["Award ID"] = hit["display_award_id"]
            row = self.append_recipient_hash_level(row)
            row.pop("parent_recipient_unique_id")
            results.append(row)

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
            "limit": self.pagination["limit"],
            "results": results[: self.pagination["limit"]],
            "page_metadata": {
                "page": self.pagination["page"],
                "hasNext": has_next,
                "last_record_unique_id": last_record_unique_id,
                "last_record_sort_value": str(last_record_sort_value),
            },
            "messages": [
                get_generic_filters_message(
                    self.original_filters.keys(), [elem["name"] for elem in AWARD_FILTER_NO_RECIPIENT_ID]
                )
            ],
        }

    def append_recipient_hash_level(self, result) -> dict:
        if "recipient_id" not in self.fields:
            result.pop("recipient_id")
            return result

        id = result.get("recipient_id")
        parent_id = result.get("parent_recipient_unique_id")
        if id:
            sql = """(
                    select
                        rp.recipient_hash || '-' ||  rp.recipient_level as hash
                    from
                        recipient_profile rp
                        inner join recipient_lookup rl on rl.recipient_hash = rp.recipient_hash
                    where
                        rl.duns = {recipient_id} and
                        rp.recipient_level = case
                            when {parent_recipient_unique_id} is null then 'R'
                            else 'C'
                        end and
                    rp.recipient_name not in {special_cases}
            )"""

            special_cases = ["'" + case + "'" for case in SPECIAL_CASES]
            SQL = sql.format(
                recipient_id="'" + id + "'",
                parent_recipient_unique_id=parent_id if parent_id else "null",
                special_cases="(" + ", ".join(special_cases) + ")",
            )
            row = execute_sql_to_ordered_dictionary(SQL)
            if len(row) > 0:
                result["recipient_id"] = row[0].get("hash")
            else:
                result["recipient_id"] = None
        return result
