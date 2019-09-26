from collections import OrderedDict

from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.tinyshield import validate_post_request


# As per direction from the product owner, agency data is to be retrieved from
# the File D (awards) data not File C (financial_accounts_by_awards).  Also,
# even though this query structure looks terrible, it managed to boost
# performance a bit.
ROLLUP_SQL = SQL(
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
        select  ca.id award_id,
                ca.awarding_agency_id,
                ca.funding_agency_id
        from    gather_award_ids gaids
                inner join awards pa on pa.id = gaids.award_id
                inner join awards ca on
                    ca.parent_award_piid = pa.piid and
                    ca.fpds_parent_agency_id = pa.fpds_agency_id and
                    ca.type not like 'IDV%'
    ), gather_financial_accounts_by_awards as (
        select  ga.awarding_agency_id,
                ga.funding_agency_id,
                nullif(faba.transaction_obligated_amount, 'NaN') transaction_obligated_amount,
                faba.treasury_account_id
        from    gather_awards ga
                inner join financial_accounts_by_awards faba on faba.award_id = ga.award_id
    )
    select
        coalesce(sum(gfaba.transaction_obligated_amount), 0.0)          total_transaction_obligated_amount,
        count(distinct aa.toptier_agency_id)                            awarding_agency_count,
        count(distinct af.toptier_agency_id)                            funding_agency_count,
        count(distinct taa.agency_id || '-' || taa.main_account_code)   federal_account_count
    from
        gather_financial_accounts_by_awards gfaba
        left outer join treasury_appropriation_account taa on
            taa.treasury_account_identifier = gfaba.treasury_account_id
        left outer join agency aa on aa.id = gfaba.awarding_agency_id
        left outer join agency af on af.id = gfaba.funding_agency_id
"""
)


@validate_post_request([get_internal_or_generated_award_id_model()])
class IDVFundingRollupViewSet(APIView):
    """
    Returns File C funding totals associated with an IDV's children.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/idvs/funding_rollup.md"

    @staticmethod
    def _business_logic(request_data: dict) -> OrderedDict:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data["award_id"]
        award_id_column = "award_id" if type(award_id) is int else "generated_unique_award_id"

        sql = ROLLUP_SQL.format(award_id_column=Identifier(award_id_column), award_id=Literal(award_id))

        return execute_sql_to_ordered_dictionary(sql)[0]

    @cache_response()
    def post(self, request: Request) -> Response:
        return Response(self._business_logic(request.data))
