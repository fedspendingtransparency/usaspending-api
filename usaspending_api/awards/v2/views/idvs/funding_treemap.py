import copy
from collections import OrderedDict

from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.validator.tinyshield_v2 import validate_request
from usaspending_api.common.validator.pagination import PAGINATION


FUNDING_TREEMAP_SQL = SQL("""
    with cte as (
        select    award_id
        from      parent_award
        where     {award_id_column} = {award_id}
        union all
        select    cpa.award_id
        from      parent_award ppa
                  inner join parent_award cpa on cpa.parent_award_id = ppa.award_id
        where     ppa.{award_id_column} = {award_id}
    )
    select
        {columns}
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
            taa.treasury_account_identifier = faba.treasury_account_id {group_by};""")


class IDVFundingBaseViewSet(APIDocumentationView):

    """
    Returns File C funding totals associated with an IDV's children.

    """

    @staticmethod
    def _business_logic(request_data: dict, columns: str, group_by: str) -> list:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data['award_id']
        award_id_column = 'award_id' if type(award_id) is int else 'generated_unique_award_id'

        sql = FUNDING_TREEMAP_SQL.format(
            columns=SQL(columns),
            award_id_column=Identifier(award_id_column),
            award_id=Literal(award_id),
            group_by=SQL(group_by),
        )
        return execute_sql_to_ordered_dictionary(sql)


@validate_request([get_internal_or_generated_award_id_model()]+copy.deepcopy(PAGINATION))
class IDVFundingRollupViewSet(IDVFundingBaseViewSet):

    """
    endpoint_doc: /awards/idvs/funding_rollup.md
    """

    @cache_response()
    def post(self, request: Request) -> Response:
        columns = """coalesce(sum(nullif(faba.transaction_obligated_amount, 'NaN')), 0.0) total_transaction_obligated_amount,
                     count(distinct ca.awarding_agency_id) awarding_agency_count,
                     count(distinct taa.agency_id || '-' || taa.main_account_code) federal_account_count"""
        group_by = ""
        results = self._business_logic(request.data, columns, group_by)
        return Response(results[0])


# THIS IS A STUB FOR DEV-2237

class IDVFundingTreemapViewSet(IDVFundingBaseViewSet):

    @cache_response()
    def post(self, request: Request) -> Response:

        group_by = "group by ca.awarding_agency_id, taa.agency_id || '-' || taa.main_account_code"
        columns = """sum(nullif(faba.transaction_obligated_amount, 'NaN')) total_transaction_obligated_amount,
                      taa.agency_id || '-' || taa.main_account_code federal_account,
                      ca.awarding_agency_id agency_id"""
        request_data = self._parse_and_validate_request(request.data)
        results = self._business_logic(request_data, columns, group_by)
        page_metadata = get_simple_pagination_metadata(len(results), request_data['limit'], request_data['page'])

        response = OrderedDict((
            ('results', results[:request_data['limit']]),
            ('page_metadata', page_metadata)
        ))

        return Response(response)
