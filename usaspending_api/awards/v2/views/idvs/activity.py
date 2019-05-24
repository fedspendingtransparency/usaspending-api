from collections import OrderedDict
from copy import copy, deepcopy

from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_result_count_pagination_metadata
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary, get_connection
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield


# In gather_award_ids, if any awards are found for IDVs in the second half of
# the union, by definition, they have to be grandchildren so even though the
# grandchild boolean appears to be applying to the IDV, it will actually
# trickle down to its children.  agency_id_to_agency_id_for_toptier_mapping
# is used to turn agency ids into agency ids for toptier agencies.  Unfortunately
# that logic is... not 100% straightforward.  To determine if an agency is a
# toptier agency, apparently the topter name and subtier names have to match and,
# even then, there can be more than one match... or no match in the three cases
# where agencies don't have subtiers.
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
    ), agency_id_to_agency_id_for_toptier_mapping as (
        select
            a.id                            agency_id,
            t.agency_id                     agency_id_for_toptier,
            t.toptier_agency_name
        from (
                select
                    a.id                    agency_id,
                    ta.toptier_agency_id,
                    ta.name                 toptier_agency_name,
                    row_number() over(
                        partition by ta.toptier_agency_id
                        order by sa.name is not distinct from ta.name desc, a.update_date asc, a.id desc
                    ) as per_toptier_row_number
                from
                    agency a
                    inner join toptier_agency ta on ta.toptier_agency_id = a.toptier_agency_id
                    left outer join subtier_agency sa on sa.subtier_agency_id = a.subtier_agency_id
            ) t
            inner join agency a on a.toptier_agency_id = t.toptier_agency_id
        where
            t.per_toptier_row_number = 1
    )
    select
        ca.id                                           award_id,
        aamap.toptier_agency_name                       awarding_agency,
        aamap.agency_id_for_toptier                     awarding_agency_id,
        ca.generated_unique_award_id,
        tf.ordering_period_end_date                     last_date_to_order,
        ca.total_obligation                             obligated_amount,
        ca.base_and_all_options_value                   awarded_amount,
        ca.period_of_performance_start_date,
        ca.piid,
        rp.recipient_name,
        rp.recipient_hash || '-' || rp.recipient_level  recipient_id,
        gaids.grandchild
    from
        gather_award_ids gaids
        inner join awards pa on pa.id = gaids.award_id
        inner join awards ca on
            ca.parent_award_piid = pa.piid and
            ca.fpds_parent_agency_id = pa.fpds_agency_id and
            ca.type not like 'IDV%'
        left outer join transaction_fpds tf on tf.transaction_id = ca.latest_transaction_id
        left outer join recipient_profile rp on rp.recipient_unique_id = tf.awardee_or_recipient_uniqu
        left outer join agency_id_to_agency_id_for_toptier_mapping aamap on aamap.agency_id = ca.awarding_agency_id
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
    return models


TINY_SHIELD_MODELS = _prepare_tiny_shield_models()


class IDVActivityViewSet(APIDocumentationView):
    """Returns the direct children of an IDV.
    endpoint_doc: /awards/idvs/awards.md
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
        award_id_column = 'award_id' if type(award_id) is int else 'generated_unique_award_id'

        sql = COUNT_ACTIVITY_SQL.format(
            award_id_column=Identifier(award_id_column),
            award_id=Literal(award_id)
        )

        with get_connection().cursor() as cursor:
            cursor.execute(sql)
            results = cursor.fetchall()
        overall_count = results[0][0] if results else 0

        sql = ACTIVITY_SQL.format(
            award_id_column=Identifier(award_id_column),
            award_id=Literal(award_id),
            limit=Literal(request_data['limit'] + 1),
            offset=Literal((request_data['page'] - 1) * request_data['limit']),
        )

        return execute_sql_to_ordered_dictionary(sql), overall_count

    @cache_response()
    def post(self, request: Request) -> Response:
        request_data = self._parse_and_validate_request(request.data)
        results, overall_count = self._business_logic(request_data)
        page_metadata = get_result_count_pagination_metadata(request_data['page'], overall_count)

        response = OrderedDict((
            ('results', results[:request_data['limit']]),
            ('page_metadata', page_metadata)
        ))

        return Response(response)
