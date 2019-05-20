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


FUNDING_TREEMAP_SQL = SQL("""with cte as (
    select      award_id
    from        parent_award
    where       {award_id_column} = {award_id}
    union all
    select      cpa.award_id
    from        parent_award ppa
                inner join parent_award cpa on cpa.parent_award_id = ppa.award_id
    where       ppa.{award_id_column} = {award_id}
)
select      {columns}
from        cte
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
            inner join toptier_agency ta on
                ta.cgac_code = taa.agency_id
            inner join agency a on
                a.toptier_agency_id = ta.toptier_agency_id and
                a.toptier_flag = TRUE
            left outer join federal_account fa on
                fa.agency_identifier = taa.agency_id and
                fa.main_account_code = taa.main_account_code
                {group_by}
                {order_by};""")


class IDVFundingBaseViewSet(APIDocumentationView):

    """
    Returns File C funding totals associated with an IDV's children.

    """

    @staticmethod
    def _business_logic(request_data: dict, columns: object, group_by: object,
                        order_by: object) -> list:
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
        )
        return execute_sql_to_ordered_dictionary(sql)


@validate_post_request([get_internal_or_generated_award_id_model(), ])
class IDVFundingRollupViewSet(IDVFundingBaseViewSet):

    """
    endpoint_doc: /awards/idvs/funding_rollup.md
    """

    @cache_response()
    def post(self, request: Request) -> Response:
        order_by = SQL("")
        # TODO: To avoid breaking the frontend, we did not change awarding_agency_count to be
        # funding_agency_count. However, the value it is reporting IS the funding agency count.
        # There is a ticket to add awarding agency at a later time, DEV-2576, when that happens,
        # the name should be switched.
        columns = SQL("""coalesce(sum(nullif(faba.transaction_obligated_amount, 'NaN')), 0.0) total_transaction_obligated_amount,
                     count(distinct ta.toptier_agency_id) awarding_agency_count,
                     count(distinct taa.agency_id || '-' || taa.main_account_code) federal_account_count""")
        group_by = SQL("")
        results = self._business_logic(request.data, columns, group_by, order_by)
        return Response(results[0])


TREEMAP_MODELS = customize_pagination_with_sort_columns(list(SORTABLE_COLUMNS.keys()), DEFAULT_SORT_COLUMN)
TREEMAP_MODELS.append(get_internal_or_generated_award_id_model())


@validate_post_request(TREEMAP_MODELS)
class IDVFundingAccountViewSet(IDVFundingBaseViewSet):

    @cache_response()
    def post(self, request: Request) -> Response:
        order_by = SQL("order by {} {}".format(SORTABLE_COLUMNS[request.data['sort']], request.data['order']))
        group_by = SQL("""group by federal_account, fa.account_title, ta.abbreviation, ta.name, a.id""")
        columns = SQL('''sum(nullif(faba.transaction_obligated_amount, 'NaN'::numeric)) total_transaction_obligated_amount,
            taa.agency_id || '-' || taa.main_account_code federal_account,
            fa.account_title,
            ta.abbreviation funding_agency_abbreviation,
            ta.name funding_agency_name,
            a.id funding_agency_id''')
        results = self._business_logic(request.data, columns, group_by, order_by)
        paginated_results, page_metadata = get_pagination(results, request.data['limit'], request.data['page'])
        response = OrderedDict((
            ('results', paginated_results),
            ('page_metadata', page_metadata)
        ))

        return Response(response)
