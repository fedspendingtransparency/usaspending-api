from copy import deepcopy

from django.db.models import F
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.validator import (
    customize_pagination_with_sort_columns,
    get_internal_or_generated_award_id_model,
    TinyShield,
    update_model_in_list,
)


class TransactionViewSet(APIView):
    """
    This route sends a request to the backend to retrieve transactions related to
    a specific parent award.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/transactions.md"

    transaction_lookup = {
        # "Display Name": "database_column"
        "id": "transaction_unique_id",
        "type": "type",
        "type_description": "type_description",
        "action_date": "action_date",
        "action_type": "action_type",
        "action_type_description": "action_type_description",
        "modification_number": "modification_number",
        "description": "description",
        "federal_action_obligation": "federal_action_obligation",
        "face_value_loan_guarantee": "face_value_loan_guarantee",
        "original_loan_subsidy_cost": "original_loan_subsidy_cost",
        "is_fpds": "is_fpds",
        "cfda_number": "assistance_data__cfda_number",
    }

    def __init__(self):
        models = customize_pagination_with_sort_columns(
            list(TransactionViewSet.transaction_lookup.keys()), "action_date"
        )
        models.extend(
            [
                get_internal_or_generated_award_id_model(),
                {"key": "idv", "name": "idv", "type": "boolean", "default": True, "optional": True},
            ]
        )

        self._tiny_shield_models = update_model_in_list(model_list=models, model_name="limit", new_dict={"max": 5000})
        super(TransactionViewSet, self).__init__()

    def _parse_and_validate_request(self, request_dict: dict) -> dict:
        return TinyShield(deepcopy(self._tiny_shield_models)).block(request_dict)

    def _business_logic(self, request_data: dict) -> list:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data["award_id"]
        award_id_column = "award_id" if type(award_id) is int else "award__generated_unique_award_id"
        filter = {award_id_column: award_id}
        if request_data["sort"] == "cfda_number":
            request_data["sort"] = "assistance_data__cfda_number"
        lower_limit = (request_data["page"] - 1) * request_data["limit"]
        upper_limit = request_data["page"] * request_data["limit"]

        queryset = (
            TransactionNormalized.objects.all()
            .filter(**filter)
            .select_related("assistance_data")
            .values(*list(self.transaction_lookup.values()))
        )
        if request_data["order"] == "desc":
            queryset = queryset.order_by(F(request_data["sort"]).desc(nulls_last=True))
        else:
            queryset = queryset.order_by(F(request_data["sort"]).asc(nulls_first=True))

        rows = list(queryset[lower_limit : upper_limit + 1])
        return self._format_results(rows)

    def _format_results(self, rows):
        results = []
        for row in rows:
            unique_prefix = "ASST_TX"
            result = {k: row.get(v) for k, v in self.transaction_lookup.items() if k != "award_id"}
            if result["is_fpds"]:
                unique_prefix = "CONT_TX"
                del result["cfda_number"]
            result["id"] = f"{unique_prefix}_{result['id']}"
            del result["is_fpds"]
            results.append(result)
        return results

    @cache_response()
    def post(self, request: Request) -> Response:
        request_data = self._parse_and_validate_request(request.data)
        results = self._business_logic(request_data)
        page_metadata = get_simple_pagination_metadata(len(results), request_data["limit"], request_data["page"])

        response = {"page_metadata": page_metadata, "results": results[: request_data["limit"]]}

        return Response(response)
