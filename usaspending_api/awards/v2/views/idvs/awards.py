from collections import OrderedDict
from copy import deepcopy

from django.db import connections, router
from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.awards.models import Award  # We only use this model to get a connection
from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.core.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.core.validator.pagination import customize_pagination_with_sort_columns
from usaspending_api.core.validator.tinyshield import TinyShield
from usaspending_api.etl.broker_etl_helpers import dictfetchall


# Columns upon which the client is allowed to sort.
SORTABLE_COLUMNS = (
    'award_type',
    'description',
    'funding_agency',
    'last_date_to_order',
    'obligated_amount',
    'period_of_performance_current_end_date',
    'period_of_performance_start_date',
    'piid',
)

DEFAULT_SORT_COLUMN = 'period_of_performance_start_date'

GET_IDVS_SQL = SQL("""
    select
        ac.id                                      award_id,
        ac.type_description                        award_type,
        ac.description,
        tf.funding_agency_name                     funding_agency,
        ac.generated_unique_award_id,
        tf.ordering_period_end_date                last_date_to_order,
        pac.rollup_total_obligation                obligated_amount,
        ac.period_of_performance_current_end_date,
        ac.period_of_performance_start_date,
        ac.piid
    from
        parent_award pap
        inner join parent_award pac on pac.parent_award_id = pap.award_id
        inner join awards ac on ac.id = pac.award_id
        inner join transaction_fpds tf on tf.transaction_id = ac.latest_transaction_id
    where
        pap.{award_id_column} = {award_id}
    order by
        {sort_column} {sort_direction}, ac.id {sort_direction}
    limit {limit} offset {offset}
""")

GET_CONTRACTS_SQL = SQL("""
    select
        ac.id                                      award_id,
        ac.type_description                        award_type,
        ac.description,
        tf.funding_agency_name                     funding_agency,
        ac.generated_unique_award_id,
        tf.ordering_period_end_date                last_date_to_order,
        ac.total_obligation                        obligated_amount,
        ac.period_of_performance_current_end_date,
        ac.period_of_performance_start_date,
        ac.piid
    from
        parent_award pap
        inner join awards ap on ap.id = pap.award_id
        inner join awards ac on ac.fpds_parent_agency_id = ap.fpds_agency_id and ac.parent_award_piid = ap.piid and
            ac.type not like 'IDV\_%%'
        inner join transaction_fpds tf on tf.transaction_id = ac.latest_transaction_id
    where
        pap.{award_id_column} = {award_id}
    order by
        {sort_column} {sort_direction}, ac.id {sort_direction}
    limit {limit} offset {offset}
""")


def _prepare_tiny_shield_models():
    models = customize_pagination_with_sort_columns(SORTABLE_COLUMNS, DEFAULT_SORT_COLUMN)
    models.extend([
        get_internal_or_generated_award_id_model(),
        {'key': 'idv', 'name': 'idv', 'type': 'boolean', 'default': True, 'optional': True}
    ])
    return models


TINY_SHIELD_MODELS = _prepare_tiny_shield_models()


class IDVAwardsViewSet(APIDocumentationView):
    """Returns the direct children of an IDV.
    endpoint_doc: /awards/idvs/awards.md
    """

    @staticmethod
    def _parse_and_validate_request(request: Request) -> dict:
        return TinyShield(deepcopy(TINY_SHIELD_MODELS)).block(request)

    @staticmethod
    def _business_logic(request_data: dict) -> list:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data['award_id']
        award_id_column = 'award_id' if type(award_id) is int else 'generated_unique_award_id'

        sql = GET_IDVS_SQL if request_data['idv'] else GET_CONTRACTS_SQL
        sql = sql.format(
            award_id_column=Identifier(award_id_column),
            award_id=Literal(award_id),
            sort_column=Identifier(request_data['sort']),
            sort_direction=SQL(request_data['order']),
            limit=Literal(request_data['limit'] + 1),
            offset=Literal((request_data['page'] - 1) * request_data['limit']),
        )
        # Because we use multiple read connections, we need to actually choose
        # a connection.  Using the default connection won't work in production.
        # A model is a required parameter for db_for_read.
        connection = connections[router.db_for_read(Award)]
        with connection.cursor() as cursor:
            # We must convert this to an actual query string else
            # django-debug-toolbar will blow up since it is assuming a string
            # instead of a SQL object.
            cursor.execute(sql.as_string(connection.connection))
            return dictfetchall(cursor)

    @cache_response()
    def post(self, request: Request) -> Response:
        request_data = self._parse_and_validate_request(request.data)
        results = self._business_logic(request_data)
        page_metadata = get_simple_pagination_metadata(len(results), request_data['limit'], request_data['page'])

        response = OrderedDict((
            ('results', results[:request_data['limit']]),
            ('page_metadata', page_metadata)
        ))

        return Response(response)
