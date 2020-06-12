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


defc_sql = """
    with closed_covid_periods as (
        select distinct concat(p.reporting_fiscal_year::text, lpad(p.reporting_fiscal_period::text, 2, '0')) as fyp, p.reporting_fiscal_year, p.reporting_fiscal_period
        from submission_attributes p
        order by p.reporting_fiscal_year desc, p.reporting_fiscal_period desc
    ),
    eligible_submissions as (
        select *
        from submission_attributes s
        where concat(s.reporting_fiscal_year::text, lpad(s.reporting_fiscal_period::text, 2, '0')) in (
            select fyp from closed_covid_periods
        )
    ),
    eligible_file_c_records as (
        select
            faba.award_id,
            faba.gross_outlay_amount_by_award_cpe,
            faba.transaction_obligated_amount,
            faba.disaster_emergency_fund_code,
            s.reporting_fiscal_year,
            s.reporting_fiscal_period
        from financial_accounts_by_awards faba
        inner join eligible_submissions s on s.submission_id = faba.submission_id
        and faba.award_id is not null
    ),
    fy_final_balances as (
        -- Rule: If a balance is not zero at the end of the year, it must be reported in the
        -- final period's submission (month or quarter), otherwise assume it to be zero
        select faba.award_id, sum(faba.gross_outlay_amount_by_award_cpe) as prior_fys_outlay
        from eligible_file_c_records faba
        group by
            faba.award_id,
            faba.reporting_fiscal_period
        having faba.reporting_fiscal_period = 12
        and sum(faba.gross_outlay_amount_by_award_cpe) > 0
    ),
    current_fy_balance as (
        select
            faba.award_id,
            faba.reporting_fiscal_year,
            faba.reporting_fiscal_period,
            sum(faba.gross_outlay_amount_by_award_cpe) as current_fy_outlay
        from eligible_file_c_records faba
        group by
            faba.award_id,
            faba.reporting_fiscal_year,
            faba.reporting_fiscal_period
        having concat(faba.reporting_fiscal_year::text, lpad(faba.reporting_fiscal_period::text, 2, '0')) in
            (select max(fyp) from closed_covid_periods)
        and sum(faba.gross_outlay_amount_by_award_cpe) > 0
    )
    select faba.award_id, coalesce(ffy.prior_fys_outlay, 0) + coalesce(cfy.current_fy_outlay, 0) as total_outlay, sum(faba.transaction_obligated_amount) as obligated_amount, faba.disaster_emergency_fund_code
    from eligible_file_c_records faba
    left join fy_final_balances ffy on ffy.award_id = faba.award_id
    left join current_fy_balance cfy
        on cfy.reporting_fiscal_period != 12 -- don't duplicate the year-end period's value if in unclosed period 01
        and cfy.award_id = faba.award_id
    where faba.award_id in {award_ids}
    group by faba.disaster_emergency_fund_code, faba.award_id, total_outlay;
    """

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


def fetch_account_details_idv(award_id, award_id_column) -> dict:
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
    child_results = (
        execute_sql_to_ordered_dictionary(defc_sql.format(award_ids=child_award_ids)) if child_award_ids != [] else {}
    )
    grandchild_results = (
        execute_sql_to_ordered_dictionary(defc_sql.format(award_ids=grandchild_award_ids))
        if grandchild_award_ids != []
        else {}
    )
    child_outlay_by_code = []
    child_obligation_by_code = []
    child_total_outlay = 0
    child_total_obligations = 0
    for row in child_results:
        child_total_outlay += row["total_outlay"] if row["total_outlay"] is not None else 0
        child_total_obligations += row["obligated_amount"] if row["obligated_amount"] is not None else 0
        child_outlay_by_code.append({"code": row["disaster_emergency_fund_code"], "amount": row["total_outlay"]})
        child_obligation_by_code.append(
            {"code": row["disaster_emergency_fund_code"], "amount": row["obligated_amount"]}
        )
    grandchild_outlay_by_code = []
    grandchild_obligation_by_code = []
    grandchild_total_outlay = 0
    grandchild_total_obligations = 0
    for row in grandchild_results:
        grandchild_total_outlay += row["total_outlay"] if row["total_outlay"] is not None else 0
        grandchild_total_obligations += row["obligated_amount"] if row["obligated_amount"] is not None else 0
        grandchild_outlay_by_code.append({"code": row["disaster_emergency_fund_code"], "amount": row["total_outlay"]})
        grandchild_obligation_by_code.append(
            {"code": row["disaster_emergency_fund_code"], "amount": row["obligated_amount"]}
        )

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
