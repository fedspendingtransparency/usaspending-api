from collections import OrderedDict
from copy import deepcopy

from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.pagination import customize_pagination_with_sort_columns
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.helpers import generate_agency_slugs_for_agency_list


# Columns upon which the client is allowed to sort.
SORTABLE_COLUMNS = (
    "award_type",
    "description",
    "funding_agency",
    "awarding_agency",
    "last_date_to_order",
    "obligated_amount",
    "period_of_performance_current_end_date",
    "period_of_performance_start_date",
    "piid",
)


DEFAULT_SORT_COLUMN = "period_of_performance_start_date"


# Justification for having three queries that are very similar: ease of
# understanding.  Yes, a change to the queries might require three updates,
# but believe me when I tell you that this is a billion times more comprehensible
# than the version where I broke the queries down into unique bits and
# grafted them back together.  If we go beyond three versions, though... perhaps
# something a little more sophisticated is in order.
GET_CHILD_IDVS_SQL = SQL(
    """
    select
        ac.id                                      award_id,
        ac.type_description                        award_type,
        ac.description,
        tta.name                                   funding_agency,
        tta.toptier_agency_id                      funding_toptier_agency_id,
        ttb.name                                   awarding_agency,
        ttb.toptier_agency_id                      awarding_toptier_agency_id,
        ac.funding_agency_id,
        ac.awarding_agency_id,
        ac.generated_unique_award_id,
        tf.ordering_period_end_date                last_date_to_order,
        pac.rollup_total_obligation                obligated_amount,
        ac.period_of_performance_current_end_date,
        ac.period_of_performance_start_date,
        ac.piid
    from
        parent_award pap
        inner join parent_award pac on pac.parent_award_id = pap.award_id
        inner join vw_awards ac on ac.id = pac.award_id
        inner join vw_transaction_fpds tf on tf.transaction_id = ac.latest_transaction_id
        left outer join agency a on a.id = ac.funding_agency_id
        left outer join agency b on b.id = ac.awarding_agency_id
        left outer join toptier_agency tta on tta.toptier_agency_id = a.toptier_agency_id
        left outer join toptier_agency ttb on ttb.toptier_agency_id = b.toptier_agency_id
    where
        pap.{award_id_column} = {award_id}
    order by
        {sort_column} {sort_direction}, ac.id {sort_direction}
    limit {limit} offset {offset}
"""
)


GET_CHILD_AWARDS_SQL = SQL(
    """
    select
        ac.id                                      award_id,
        ac.type_description                        award_type,
        ac.description,
        tta.name                                   funding_agency,
        tta.toptier_agency_id                      funding_toptier_agency_id,
        ttb.name                                   awarding_agency,
        ttb.toptier_agency_id                      awarding_toptier_agency_id,
        ac.funding_agency_id,
        ac.awarding_agency_id,
        ac.generated_unique_award_id,
        tf.ordering_period_end_date                last_date_to_order,
        ac.total_obligation                        obligated_amount,
        ac.period_of_performance_current_end_date,
        ac.period_of_performance_start_date,
        ac.piid
    from
        parent_award pap
        inner join vw_awards ap on ap.id = pap.award_id
        inner join vw_awards ac on ac.fpds_parent_agency_id = ap.fpds_agency_id and ac.parent_award_piid = ap.piid and
            ac.type not like 'IDV%'
        inner join vw_transaction_fpds tf on tf.transaction_id = ac.latest_transaction_id
        left outer join agency a on a.id = ac.funding_agency_id
        left outer join agency b on b.id = ac.awarding_agency_id
        left outer join toptier_agency tta on tta.toptier_agency_id = a.toptier_agency_id
        left outer join toptier_agency ttb on ttb.toptier_agency_id = b.toptier_agency_id
    where
        pap.{award_id_column} = {award_id}
    order by
        {sort_column} {sort_direction}, ac.id {sort_direction}
    limit {limit} offset {offset}
"""
)


GET_GRANDCHILD_AWARDS_SQL = SQL(
    """
    select
        ac.id                                      award_id,
        ac.type_description                        award_type,
        ac.description,
        tta.name                                   funding_agency,
        tta.toptier_agency_id                      funding_toptier_agency_id,
        ttb.name                                   awarding_agency,
        ttb.toptier_agency_id                      awarding_toptier_agency_id,
        ac.funding_agency_id,
        ac.awarding_agency_id,
        ac.generated_unique_award_id,
        tf.ordering_period_end_date                last_date_to_order,
        ac.total_obligation                        obligated_amount,
        ac.period_of_performance_current_end_date,
        ac.period_of_performance_start_date,
        ac.piid
    from
        parent_award pap
        inner join parent_award pac on pac.parent_award_id = pap.award_id
        inner join vw_awards ap on ap.id = pac.award_id
        inner join vw_awards ac on ac.fpds_parent_agency_id = ap.fpds_agency_id and ac.parent_award_piid = ap.piid and
            ac.type not like 'IDV%'
        inner join vw_transaction_fpds tf on tf.transaction_id = ac.latest_transaction_id
        left outer join agency a on a.id = ac.funding_agency_id
        left outer join agency b on b.id = ac.awarding_agency_id
        left outer join toptier_agency tta on tta.toptier_agency_id = a.toptier_agency_id
        left outer join toptier_agency ttb on ttb.toptier_agency_id = b.toptier_agency_id

    where
        pap.{award_id_column} = {award_id}
    order by
        {sort_column} {sort_direction}, ac.id {sort_direction}
    limit {limit} offset {offset}
"""
)


TYPE_TO_SQL_MAPPING = {
    "child_idvs": GET_CHILD_IDVS_SQL,
    "child_awards": GET_CHILD_AWARDS_SQL,
    "grandchild_awards": GET_GRANDCHILD_AWARDS_SQL,
}


def _prepare_tiny_shield_models():
    models = customize_pagination_with_sort_columns(SORTABLE_COLUMNS, DEFAULT_SORT_COLUMN)
    models.extend(
        [
            get_internal_or_generated_award_id_model(),
            {
                "key": "type",
                "name": "type",
                "type": "enum",
                "enum_values": tuple(TYPE_TO_SQL_MAPPING),
                "default": "child_idvs",
                "optional": True,
            },
        ]
    )
    return models


TINY_SHIELD_MODELS = _prepare_tiny_shield_models()


class IDVAwardsViewSet(APIView):
    """
    Returns the direct children of an IDV.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/idvs/awards.md"

    @staticmethod
    def _parse_and_validate_request(request: dict) -> dict:
        return TinyShield(deepcopy(TINY_SHIELD_MODELS)).block(request)

    @staticmethod
    def _business_logic(request_data: dict) -> list:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data["award_id"]
        award_id_column = "award_id" if type(award_id) is int else "generated_unique_award_id"

        sql = TYPE_TO_SQL_MAPPING[request_data["type"]]
        sql = sql.format(
            award_id_column=Identifier(award_id_column),
            award_id=Literal(award_id),
            sort_column=Identifier(request_data["sort"]),
            sort_direction=SQL(request_data["order"]),
            limit=Literal(request_data["limit"] + 1),
            offset=Literal((request_data["page"] - 1) * request_data["limit"]),
        )
        results = execute_sql_to_ordered_dictionary(sql)

        toptier_ids = set(
            [res["funding_toptier_agency_id"] for res in results]
            + [res["awarding_toptier_agency_id"] for res in results]
        )
        agency_slugs = generate_agency_slugs_for_agency_list(toptier_ids)

        for res in results:
            # Set Agency Slugs
            res["funding_agency_slug"] = agency_slugs.get(res.get("funding_toptier_agency_id"))
            res["awarding_agency_slug"] = agency_slugs.get(res.get("awarding_toptier_agency_id"))

            # Remove extra fields
            del res["funding_toptier_agency_id"]
            del res["awarding_toptier_agency_id"]

        return results

    @cache_response()
    def post(self, request: Request) -> Response:
        request_data = self._parse_and_validate_request(request.data)
        results = self._business_logic(request_data)
        page_metadata = get_simple_pagination_metadata(len(results), request_data["limit"], request_data["page"])

        response = OrderedDict((("results", results[: request_data["limit"]]), ("page_metadata", page_metadata)))

        return Response(response)
