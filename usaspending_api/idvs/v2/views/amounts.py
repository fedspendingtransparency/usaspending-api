import logging

from collections import OrderedDict

from django.db.models import Sum, Max

from rest_framework.exceptions import NotFound
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.awards.models import ParentAward, FinancialAccountsByAwards
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.tinyshield import TinyShield


logger = logging.getLogger("console")


def fetch_account_details_idv(award_id, award_id_column) -> dict:
    child_award_sql = """
    select
        ac.id as award_id
    from
        parent_award pap
        inner join awards ap on ap.id = pap.award_id
        inner join awards ac on ac.fpds_parent_agency_id = ap.fpds_agency_id and ac.parent_award_piid = ap.piid and
            ac.type not like 'IDV%'
    where
        pap.{award_id_column} = '{award_id}'
    """
    grandchild_award_sql = """
    select
        ac.id as award_id
    from
        parent_award pap
        inner join parent_award pac on pac.parent_award_id = pap.award_id
        inner join awards ap on ap.id = pac.award_id
        inner join awards ac on ac.fpds_parent_agency_id = ap.fpds_agency_id and ac.parent_award_piid = ap.piid and
            ac.type not like 'IDV%'

    where
        pap.{award_id_column} = '{award_id}'
    """
    children = execute_sql_to_ordered_dictionary(
        child_award_sql.format(award_id=award_id, award_id_column=award_id_column)
    )
    grandchildren = execute_sql_to_ordered_dictionary(
        grandchild_award_sql.format(award_id=award_id, award_id_column=award_id_column)
    )
    child_award_ids = []
    grandchild_award_ids = []
    child_award_ids.extend([x["award_id"] for x in children])
    grandchild_award_ids.extend([x["award_id"] for x in grandchildren])
    child_queryset = (
        FinancialAccountsByAwards.objects.filter(award_id__in=child_award_ids)
        .values("disaster_emergency_fund", "submission__reporting_fiscal_year")
        .annotate(
            fiscal_period=Max("submission__reporting_fiscal_period"),
            gross_outlay_amount_by_award_cpe=Max("gross_outlay_amount_by_award_cpe"),
            obligated_amount=Sum("transaction_obligated_amount"),
        )
    )
    grandchild_queryset = (
        FinancialAccountsByAwards.objects.filter(award_id__in=grandchild_award_ids)
        .values("disaster_emergency_fund", "submission__reporting_fiscal_year")
        .annotate(
            fiscal_period=Max("submission__reporting_fiscal_period"),
            gross_outlay_amount_by_award_cpe=Max("gross_outlay_amount_by_award_cpe"),
            obligated_amount=Sum("transaction_obligated_amount"),
        )
    )
    child_total_outlay = 0
    child_total_obligations = 0
    child_outlay_results = {}
    child_obligated_results = {}
    for row in child_queryset:
        child_total_outlay += (
            row["gross_outlay_amount_by_award_cpe"] if row["gross_outlay_amount_by_award_cpe"] is not None else 0
        )
        child_total_obligations += row["obligated_amount"] if row["obligated_amount"] is not None else 0
        child_outlay_results.update(
            {
                row["disaster_emergency_fund"]: row["gross_outlay_amount_by_award_cpe"]
                + (
                    child_outlay_results.get(row["disaster_emergency_fund"])
                    if child_outlay_results.get(row["disaster_emergency_fund"]) is not None
                    else 0
                )
            }
        )
        child_obligated_results.update(
            {
                row["disaster_emergency_fund"]: row["obligated_amount"]
                + (
                    child_obligated_results.get(row["disaster_emergency_fund"])
                    if child_obligated_results.get(row["disaster_emergency_fund"]) is not None
                    else 0
                )
            }
        )
    child_outlay_by_code = [{"code": x, "amount": y} for x, y in child_outlay_results.items()]
    child_obligation_by_code = [{"code": x, "amount": y} for x, y in child_obligated_results.items()]

    grandchild_total_outlay = 0
    grandchild_total_obligations = 0
    grandchild_outlay_results = {}
    grandchild_obligated_results = {}
    for row in grandchild_queryset:
        grandchild_total_outlay += (
            row["gross_outlay_amount_by_award_cpe"] if row["gross_outlay_amount_by_award_cpe"] is not None else 0
        )
        grandchild_total_obligations += row["obligated_amount"] if row["obligated_amount"] is not None else 0
        grandchild_outlay_results.update(
            {
                row["disaster_emergency_fund"]: row["gross_outlay_amount_by_award_cpe"]
                + (
                    grandchild_outlay_results.get(row["disaster_emergency_fund"])
                    if grandchild_outlay_results.get(row["disaster_emergency_fund"]) is not None
                    else 0
                )
            }
        )
        child_obligated_results.update(
            {
                row["disaster_emergency_fund"]: row["obligated_amount"]
                + (
                    grandchild_obligated_results.get(row["disaster_emergency_fund"])
                    if grandchild_obligated_results.get(row["disaster_emergency_fund"]) is not None
                    else 0
                )
            }
        )
    grandchild_outlay_by_code = [{"code": x, "amount": y} for x, y in child_outlay_results.items()]
    grandchild_obligation_by_code = [{"code": x, "amount": y} for x, y in child_obligated_results.items()]
    results = {
        "child_total_account_outlay": child_total_outlay,
        "child_total_account_obligation": child_total_obligations,
        "child_account_outlays_by_defc": child_outlay_by_code,
        "child_account_obligations_by_defc": child_obligation_by_code,
        "grandchild_total_account_outlay": grandchild_total_outlay,
        "grandchild_total_account_obligation": grandchild_total_obligations,
        "grandchild_account_outlays_by_defc": grandchild_outlay_by_code,
        "grandchild_account_obligations_by_defc": grandchild_obligation_by_code,
    }
    return results


class IDVAmountsViewSet(APIView):
    """
    Returns counts and dollar figures for a specific IDV.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/idvs/amounts/award_id.md"

    @staticmethod
    def _parse_and_validate_request(requested_award: str) -> dict:
        return TinyShield([get_internal_or_generated_award_id_model()]).block({"award_id": requested_award})

    @staticmethod
    def _business_logic(request_data: dict) -> OrderedDict:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data["award_id"]
        award_id_column = "award_id" if type(award_id) is int else "generated_unique_award_id"

        try:
            parent_award = ParentAward.objects.get(**{award_id_column: award_id})
            account_data = fetch_account_details_idv(award_id, award_id_column)
            return OrderedDict(
                (
                    ("award_id", parent_award.award_id),
                    ("generated_unique_award_id", parent_award.generated_unique_award_id),
                    ("child_idv_count", parent_award.direct_idv_count),
                    ("child_award_count", parent_award.direct_contract_count),
                    ("child_award_total_obligation", parent_award.direct_total_obligation),
                    ("child_award_base_and_all_options_value", parent_award.direct_base_and_all_options_value),
                    ("child_award_base_exercised_options_val", parent_award.direct_base_exercised_options_val),
                    ("child_total_account_outlay", account_data["child_total_account_outlay"]),
                    ("child_total_account_obligation", account_data["child_total_account_obligation"]),
                    ("child_account_outlays_by_defc", account_data["child_account_outlays_by_defc"]),
                    ("child_account_obligations_by_defc", account_data["child_account_obligations_by_defc"]),
                    ("grandchild_award_count", parent_award.rollup_contract_count - parent_award.direct_contract_count),
                    (
                        "grandchild_award_total_obligation",
                        parent_award.rollup_total_obligation - parent_award.direct_total_obligation,
                    ),
                    (
                        "grandchild_award_base_and_all_options_value",
                        parent_award.rollup_base_and_all_options_value - parent_award.direct_base_and_all_options_value,
                    ),
                    (
                        "grandchild_award_base_exercised_options_val",
                        parent_award.rollup_base_exercised_options_val - parent_award.direct_base_exercised_options_val,
                    ),
                    ("grandchild_total_account_outlay", account_data["grandchild_total_account_outlay"]),
                    ("grandchild_total_account_obligation", account_data["grandchild_total_account_obligation"]),
                    ("grandchild_account_outlays_by_defc", account_data["grandchild_account_outlays_by_defc"]),
                    ("grandchild_account_obligations_by_defc", account_data["grandchild_account_obligations_by_defc"]),
                )
            )
        except ParentAward.DoesNotExist:
            logger.info("No IDV Award found where '%s' is '%s'" % next(iter(request_data.items())))
            raise NotFound("No IDV award found with this id")

    @cache_response()
    def get(self, request: Request, requested_award: str) -> Response:
        request_data = self._parse_and_validate_request(requested_award)
        return Response(self._business_logic(request_data))
