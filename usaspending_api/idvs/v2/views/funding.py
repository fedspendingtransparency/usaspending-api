from collections import OrderedDict
from copy import deepcopy
from itertools import chain

from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.helpers.sql_helpers import build_composable_order_by, execute_sql_to_ordered_dictionary
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.pagination import customize_pagination_with_sort_columns
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.helpers import generate_agency_slugs_for_agency_list

SORTABLE_COLUMNS = {
    "account_title": ["fa.account_title"],
    "awarding_agency_name": ["awarding_agency_name"],
    "disaster_emergency_fund_code": ["disaster_emergency_fund_code"],
    "funding_agency_name": ["funding_agency_name"],
    "gross_outlay_amount": ["gross_outlay_amount"],
    "object_class": ["oc.object_class_name", "oc.object_class"],
    "piid": ["gfaba.piid"],
    "program_activity": ["rpa.program_activity_code", "rpa.program_activity_name"],
    "reporting_fiscal_date": ["sa.reporting_fiscal_year", "sa.reporting_fiscal_quarter"],
    "transaction_obligated_amount": ["gfaba.transaction_obligated_amount"],
}

# Add a unique id to every sort key so results are deterministic.
for k, v in SORTABLE_COLUMNS.items():
    v.append("gfaba.financial_accounts_by_awards_id")

DEFAULT_SORT_COLUMN = "reporting_fiscal_date"

# Get funding information for the entire IDV hierarchy.  As per direction from
# the product owner, agency data is to be retrieved from the File D (awards)
# data not File C (financial_accounts_by_awards).
GET_FUNDING_SQL = SQL(
    """
    with gather_award_ids as (
        select  award_id
        from    parent_award
        where   {award_id_column} = {award_id}
        union all
        select  cpa.award_id
        from    parent_award ppa
                inner join parent_award cpa on cpa.parent_award_id = ppa.award_id
        where   ppa.{award_id_column} = {award_id}
    ), gather_awards as (
        select  id award_id,
                generated_unique_award_id,
                piid,
                awarding_agency_id,
                funding_agency_id
        from    vw_awards
        where   {awards_table_id_column} = {award_id} and
                (piid = {piid} or {piid} is null)
        union   all
        select  ca.id award_id,
                ca.generated_unique_award_id,
                ca.piid,
                ca.awarding_agency_id,
                ca.funding_agency_id
        from    gather_award_ids gaids
                inner join vw_awards pa on pa.id = gaids.award_id
                inner join vw_awards ca on
                    ca.parent_award_piid = pa.piid and
                    ca.fpds_parent_agency_id = pa.fpds_agency_id and
                    (ca.piid = {piid} or {piid} is null)
    ), gather_financial_accounts_by_awards as (
        select  ga.award_id,
                ga.generated_unique_award_id,
                ga.piid,
                ga.awarding_agency_id,
                ga.funding_agency_id,
                faba.transaction_obligated_amount,
                faba.gross_outlay_amount_by_award_cpe gross_outlay_amount,
                faba.disaster_emergency_fund_code,
                faba.financial_accounts_by_awards_id,
                faba.submission_id,
                faba.treasury_account_id,
                faba.program_activity_id,
                faba.object_class_id
        from    gather_awards ga
                inner join financial_accounts_by_awards faba on faba.award_id = ga.award_id
    )
    select
        gfaba.award_id,
        gfaba.generated_unique_award_id,
        sa.reporting_fiscal_year,
        sa.reporting_fiscal_quarter,
        sa.reporting_fiscal_period          reporting_fiscal_month,
        sa.quarter_format_flag              is_quarterly_submission,
        gfaba.disaster_emergency_fund_code,
        gfaba.piid,
        aa.id                               awarding_agency_id,
        aa.toptier_agency_id                awarding_toptier_agency_id,
        ata.name                            awarding_agency_name,
        faa.id                              funding_agency_id,
        faa.toptier_agency_id               funding_toptier_agency_id,
        fta.name                            funding_agency_name,
        taa.agency_id,
        taa.main_account_code,
        fa.account_title,
        rpa.program_activity_code,
        rpa.program_activity_name,
        oc.object_class,
        oc.object_class_name,
        gfaba.transaction_obligated_amount,
        gfaba.gross_outlay_amount
    from
        gather_financial_accounts_by_awards gfaba
        left outer join submission_attributes sa on sa.submission_id = gfaba.submission_id
        left outer join treasury_appropriation_account taa on
            taa.treasury_account_identifier = gfaba.treasury_account_id
        left outer join federal_account fa on fa.id = taa.federal_account_id
        left outer join ref_program_activity rpa on rpa.id = gfaba.program_activity_id
        left outer join object_class oc on oc.id = gfaba.object_class_id
        left outer join agency aa on aa.id = gfaba.awarding_agency_id
        left outer join toptier_agency ata on ata.toptier_agency_id = aa.toptier_agency_id
        left outer join agency faa on faa.id = gfaba.funding_agency_id
        left outer join toptier_agency fta on fta.toptier_agency_id = faa.toptier_agency_id
    {order_by}
    limit {limit} offset {offset}
"""
)


def _prepare_tiny_shield_models():
    models = customize_pagination_with_sort_columns(list(SORTABLE_COLUMNS.keys()), DEFAULT_SORT_COLUMN)
    models.extend(
        [
            get_internal_or_generated_award_id_model(),
            {"key": "piid", "name": "piid", "optional": True, "type": "text", "text_type": "search"},
        ]
    )
    return models


TINY_SHIELD_MODELS = _prepare_tiny_shield_models()


class IDVFundingViewSet(APIView):
    """Returns File C funding records associated with an IDV."""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/idvs/funding.md"

    @staticmethod
    def _parse_and_validate_request(request_data: dict) -> dict:
        return TinyShield(deepcopy(TINY_SHIELD_MODELS)).block(request_data)

    @staticmethod
    def _business_logic(request_data: dict) -> list:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data["award_id"]
        award_id_column = "award_id" if type(award_id) is int else "generated_unique_award_id"
        awards_table_id_column = "id" if type(award_id) is int else "generated_unique_award_id"

        sql = GET_FUNDING_SQL.format(
            award_id_column=Identifier(award_id_column),
            awards_table_id_column=Identifier(awards_table_id_column),
            award_id=Literal(award_id),
            piid=Literal(request_data.get("piid")),
            order_by=build_composable_order_by(SORTABLE_COLUMNS[request_data["sort"]], request_data["order"]),
            limit=Literal(request_data["limit"] + 1),
            offset=Literal((request_data["page"] - 1) * request_data["limit"]),
        )

        return execute_sql_to_ordered_dictionary(sql)

    @cache_response()
    def post(self, request: Request) -> Response:
        request_data = self._parse_and_validate_request(request.data)
        results = self._business_logic(request_data)
        page_metadata = get_simple_pagination_metadata(len(results), request_data["limit"], request_data["page"])
        limited_results = results[: request_data["limit"]]

        agency_slugs = generate_agency_slugs_for_agency_list(
            list(
                chain.from_iterable(
                    [(res["awarding_toptier_agency_id"], res["funding_toptier_agency_id"]) for res in limited_results]
                )
            )
        )
        for res in limited_results:
            res["awarding_agency_slug"] = agency_slugs.get(res.get("awarding_toptier_agency_id"))
            res["funding_agency_slug"] = agency_slugs.get(res.get("funding_toptier_agency_id"))

        response = OrderedDict((("results", limited_results), ("page_metadata", page_metadata)))

        return Response(response)
