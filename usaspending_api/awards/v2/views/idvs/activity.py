from collections import OrderedDict
from copy import copy, deepcopy

from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield


# In gather_award_ids, if any awards are found for IDVs in the second half of
# the union, by definition, they have to be grandchildren so even though the
# grandchild boolean appears to be applying to the IDV, it will actually
# trickle down to its children.
ACTIVITY_SQL = SQL("""
    with gather_award_ids as (
        select  award_id,
                false grandchild
        from    parent_award
        where   {award_id_column} = {award_id}
        union all
        select  cpa.award_id,
                true grandchild
        from    parent_award ppa
                inner join parent_award cpa on cpa.parent_award_id = ppa.award_id
        where   ppa.{award_id_column} = {award_id}
    )
    select
        ca.id                                           award_id,
        ta.name                                         awarding_agency,
        ca.awarding_agency_id                           awarding_agency_id,
        ca.generated_unique_award_id,
        tf.period_of_perf_potential_e                   last_date_to_order,
        pa.id                                           parent_award_id,
        ca.parent_award_piid,
        ca.total_obligation                             obligated_amount,
        ca.base_and_all_options_value                   awarded_amount,
        ca.period_of_performance_start_date,
        ca.piid,
        rl.legal_business_name                          recipient_name,
        rl.recipient_hash || case
            when tf.ultimate_parent_unique_ide is null then '-R'
            else '-C'
        end                                             recipient_id,
        gaids.grandchild
    from
        gather_award_ids gaids
        inner join awards pa on pa.id = gaids.award_id
        inner join awards ca on
            ca.parent_award_piid = pa.piid and
            ca.fpds_parent_agency_id = pa.fpds_agency_id and
            ca.type not like 'IDV%'
            {hide_edges_awarded_amount}
        left outer join transaction_fpds tf on tf.transaction_id = ca.latest_transaction_id
            {hide_edges_end_date}
        left outer join recipient_lookup rl on rl.duns = tf.awardee_or_recipient_uniqu
        left outer join agency a on a.id = ca.awarding_agency_id
        left outer join toptier_agency ta on ta.toptier_agency_id = a.toptier_agency_id
    order by
        ca.base_and_all_options_value desc, ca.id desc
    limit {limit} offset {offset}
""")


# So, as it turns out, we already count all descendant contracts.  Go us!
# There's always the chance these may not 100% match the actual count for a
# myriad of reasons, but they pretty much all involve failed operations
# processes or bad data so we're going to go ahead and give the benefit of
# the doubt and assume everything works as expected.
COUNT_ACTIVITY_SQL = SQL("""
    select  rollup_contract_count
    from    parent_award
    where   {award_id_column} = {award_id}
""")


def _prepare_tiny_shield_models():
    # This endpoint has a fixed sort.  No need for "sort" or "order".
    models = [copy(p) for p in PAGINATION if p["name"] in ("page", "limit")]
    models.extend([get_internal_or_generated_award_id_model()])
    x = [
        {'name': 'hide_edge_cases', 'type': 'boolean', 'optional': True},
    ]
    for p in x:
        p['optional'] = p.get('optional', True)
        p['key'] = p['name']
    models.extend(x)

    return models


TINY_SHIELD_MODELS = _prepare_tiny_shield_models()


class IDVActivityViewSet(APIDocumentationView):
    """
    Returns award funding info for children and grandchildren of an IDV.  Used
    to power the Activity visualization on IDV Summary page.
    """

    @staticmethod
    def _parse_and_validate_request(request: dict) -> dict:
        return TinyShield(deepcopy(TINY_SHIELD_MODELS)).block(request)

    @staticmethod
    def _business_logic(request_data: dict) -> tuple:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data['award_id']
        hide_edge_cases = request_data.get('hide_edge_cases')
        hide_edges_awarded_amount = ''
        hide_edges_end_date = ''
        award_id_column = 'award_id' if type(award_id) is int else 'generated_unique_award_id'

        sql = COUNT_ACTIVITY_SQL.format(
            award_id_column=Identifier(award_id_column),
            award_id=Literal(award_id)
        )
        overall_count_results = execute_sql_to_ordered_dictionary(sql)
        overall_count = overall_count_results[0]['rollup_contract_count'] if overall_count_results else 0

        if hide_edge_cases:
            hide_edges_awarded_amount = "and ca.base_and_all_options_value > 0"
            hide_edges_end_date = 'and tf.period_of_perf_potential_e is not null'
        sql = ACTIVITY_SQL.format(
            award_id_column=Identifier(award_id_column),
            award_id=Literal(award_id),
            limit=Literal(request_data['limit'] + 1),
            offset=Literal((request_data['page'] - 1) * request_data['limit']),
            hide_edges_awarded_amount=SQL(hide_edges_awarded_amount),
            hide_edges_end_date=SQL(hide_edges_end_date)
        )

        return execute_sql_to_ordered_dictionary(sql), overall_count

    @cache_response()
    def post(self, request: Request) -> Response:
        request_data = self._parse_and_validate_request(request.data)
        results, overall_count = self._business_logic(request_data)
        page_metadata = get_pagination_metadata(overall_count, request_data['limit'], request_data['page'])

        response = OrderedDict((
            ('results', results[:request_data['limit']]),
            ('page_metadata', page_metadata)
        ))

        return Response(response)
