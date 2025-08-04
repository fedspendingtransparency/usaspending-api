from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.tinyshield import TinyShield


GET_COUNT_SQL = SQL(
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
        select  id award_id
        from    vw_awards
        where   {awards_table_id_column} = {award_id} and
                (piid = {piid} or {piid} is null)
        union   all
        select  ca.id award_id
        from    gather_award_ids gaids
                inner join vw_awards pa on pa.id = gaids.award_id
                inner join vw_awards ca on
                    ca.parent_award_piid = pa.piid and
                    ca.fpds_parent_agency_id = pa.fpds_agency_id and
                    (ca.piid = {piid} or {piid} is null)
    ), gather_financial_accounts_by_awards as (
        select  ga.award_id,
                faba.financial_accounts_by_awards_id
        from    gather_awards ga
                inner join financial_accounts_by_awards faba on faba.award_id = ga.award_id
    )
    select
        count(distinct gfaba.financial_accounts_by_awards_id)
    from
        gather_financial_accounts_by_awards gfaba
"""
)


class IDVFederalAccountCountViewSet(APIView):
    """Returns the total number of funding transactions for an IDV's child and grandchild awards, but not the IDV itself."""

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/idvs/count/federal_account/award_id.md"

    @staticmethod
    def _parse_and_validate_request(requested_award: str, request_data: dict) -> dict:
        piid = request_data.get("piid")
        data = {"award_id": requested_award, "piid": piid}
        models = [
            get_internal_or_generated_award_id_model(),
            {
                "key": "piid",
                "name": "piid",
                "allow_nulls": True,
                "optional": True,
                "type": "text",
                "text_type": "search",
            },
        ]
        return TinyShield(models).block(data)

    @staticmethod
    def _business_logic(request_data: dict) -> list:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data["award_id"]
        award_id_column = "award_id" if type(award_id) is int else "generated_unique_award_id"
        awards_table_id_column = "id" if type(award_id) is int else "generated_unique_award_id"

        sql = GET_COUNT_SQL.format(
            award_id_column=Identifier(award_id_column),
            awards_table_id_column=Identifier(awards_table_id_column),
            award_id=Literal(award_id),
            piid=Literal(request_data.get("piid")),
        )

        return execute_sql_to_ordered_dictionary(sql)

    @cache_response()
    def get(self, request: Request, requested_award: str) -> Response:
        request_data = self._parse_and_validate_request(requested_award, request.GET)
        results = self._business_logic(request_data)
        return Response(results[0])
