from collections import OrderedDict

from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.helpers.generic_helper import get_pagination
from usaspending_api.common.validator.pagination import customize_pagination_with_sort_columns
from usaspending_api.common.validator.tinyshield import validate_post_request


SORTABLE_COLUMNS = {
    'federal_account': 'federal_account',
    'total_transaction_obligated_amount': 'total_transaction_obligated_amount',
    'agency': 'ca.awarding_agency_id',
    'account_title': 'fa.account_title'
}

DEFAULT_SORT_COLUMN = 'federal_account'

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
            taa.treasury_account_identifier = faba.treasury_account_id
        left outer join federal_account fa on
            taa.agency_id || '-' || taa.main_account_code =  fa.federal_account_code
        {group_by}
        {order_by}
        {limit}
         {offset};""")


class IDVFundingBaseViewSet(APIDocumentationView):

    """
    Returns File C funding totals associated with an IDV's children.

    """

    @staticmethod
    def _business_logic(request_data: dict, columns: object, group_by: object,
                        limit: object, order_by: object, offset: object) -> list:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data['award_id']
        award_id_column = 'award_id' if type(award_id) is int else 'generated_unique_award_id'

        sql = FUNDING_TREEMAP_SQL.format(
            columns=columns,
            award_id_column=Identifier(award_id_column),
            award_id=Literal(award_id),
            group_by=group_by,
            order_by=order_by,
            limit=limit,
            offset=offset,
        )
        return execute_sql_to_ordered_dictionary(sql)


@validate_post_request([get_internal_or_generated_award_id_model(), ])
class IDVFundingRollupViewSet(IDVFundingBaseViewSet):

    """
    endpoint_doc: /awards/idvs/funding_rollup.md
    """

    @cache_response()
    def post(self, request: Request) -> Response:
        limit = SQL("")
        order_by = SQL("")
        offset = SQL("")
        columns = SQL("""coalesce(sum(nullif(faba.transaction_obligated_amount, 'NaN')), 0.0) total_transaction_obligated_amount,
                     count(distinct ca.awarding_agency_id) awarding_agency_count,
                     count(distinct taa.agency_id || '-' || taa.main_account_code) federal_account_count""")
        group_by = SQL("")
        results = self._business_logic(request.data, columns, group_by, limit, order_by, offset)
        return Response(results[0])


TREEMAP_MODELS = customize_pagination_with_sort_columns(list(SORTABLE_COLUMNS.keys()), DEFAULT_SORT_COLUMN)
TREEMAP_MODELS.append(get_internal_or_generated_award_id_model())


@validate_post_request(TREEMAP_MODELS)
class IDVFundingTreemapViewSet(IDVFundingBaseViewSet):

    @cache_response()
    def post(self, request: Request) -> Response:
        limit = SQL("limit {}". format(request.data['limit'] + 1))
        offset = SQL("offset {}".format((request.data['page'] - 1) * request.data['limit']))
        order_by = SQL("order by {} {}".format(SORTABLE_COLUMNS[request.data['sort']], request.data['order']))
        group_by = SQL("group by federal_account, fa.account_title ")
        columns = SQL("""sum(nullif(faba.transaction_obligated_amount, 'NaN')) total_transaction_obligated_amount,
                      taa.agency_id || '-' || taa.main_account_code federal_account,
                      fa.account_title""")

        results = self._business_logic(request.data, columns, group_by, limit, order_by, offset)
        paginated_results, page_metadata = get_pagination(results, request.data['limit'], request.data['page'])

        response = OrderedDict((
            ('results', paginated_results),
            ('page_metadata', page_metadata)
        ))

        return Response(response)


# STUB FOR DEV-2238

@validate_post_request(TREEMAP_MODELS +
                       [{'key': 'account',
                         'name': 'account',
                         'optional': False,
                         'type': 'text',
                         'text_type': 'search'}])
class IDVFundingTreemapDrillDownViewSet(IDVFundingBaseViewSet):

    @cache_response()
    def post(self, request: Request) -> Response:
        limit = SQL("limit {}".format(request.data['limit'] + 1))
        offset = SQL("offset {}".format((request.data['page'] - 1) * request.data['limit']))
        group_by = SQL("group by ca.awarding_agency_id, taa.agency_id || '-' || taa.main_account_code")
        order_by = SQL("order by {} {}".format(SORTABLE_COLUMNS[request.data['sort']], request.data['order']))
        columns = SQL("""sum(nullif(faba.transaction_obligated_amount, 'NaN')) total_transaction_obligated_amount,
                      taa.agency_id || '-' || taa.main_account_code federal_account,""")

        results = self._business_logic(request.data, columns, group_by, limit, order_by, offset)
        paginated_results, age_metadata = get_pagination(results, request.data['limit'], request.data['page'])

        response = OrderedDict((
            ('results', paginated_results),
            ('page_metadata', page_metadata)
        ))

        return Response(response)
