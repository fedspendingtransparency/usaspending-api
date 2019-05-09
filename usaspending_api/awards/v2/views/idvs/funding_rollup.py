from collections import OrderedDict

from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.tinyshield import validate_post_request


ROLLUP_SQL = SQL("""
    with cte as (
        select      award_id
        from        parent_award
        where       {award_id_column} = {award_id}
        union all
        select      cpa.award_id
        from        parent_award ppa
                    inner join parent_award cpa on cpa.parent_award_id = ppa.award_id
        where       ppa.{award_id_column} = {award_id}
    )
    select
        coalesce(sum(nullif(faba.transaction_obligated_amount, 'NaN')), 0.0)    total_transaction_obligated_amount,
        count(distinct coalesce(taa.awarding_toptier_agency_id, taa.funding_toptier_agency_id)) awarding_agency_count,
        count(distinct taa.funding_toptier_agency_id)                           funding_agency_count,
        count(distinct taa.agency_id || '-' || taa.main_account_code)           federal_account_count
    from
        cte
        inner join awards pa on
            pa.id = cte.award_id
        inner join awards ca on
            ca.parent_award_piid = pa.piid and
            ca.fpds_parent_agency_id = pa.fpds_agency_id and
            ca.type not like 'IDV%'
        inner join financial_accounts_by_awards faba on
            faba.award_id = ca.id
        left outer join treasury_appropriation_account taa on
            taa.treasury_account_identifier = faba.treasury_account_id
""")


@validate_post_request([get_internal_or_generated_award_id_model()])
class IDVFundingRollupViewSet(APIDocumentationView):
    """
    Returns File C funding totals associated with an IDV's children.
    endpoint_doc: /awards/idvs/funding_rollup.md
    """

    @staticmethod
    def _business_logic(request_data: dict) -> OrderedDict:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data['award_id']
        award_id_column = 'award_id' if type(award_id) is int else 'generated_unique_award_id'

        sql = ROLLUP_SQL.format(
            award_id_column=Identifier(award_id_column),
            award_id=Literal(award_id),
        )

        return execute_sql_to_ordered_dictionary(sql)[0]

    @cache_response()
    def post(self, request: Request) -> Response:
        return Response(self._business_logic(request.data))
