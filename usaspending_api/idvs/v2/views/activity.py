from collections import OrderedDict
from copy import copy, deepcopy

from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination_metadata
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.pagination import PAGINATION
from usaspending_api.common.validator.tinyshield import TinyShield
from usaspending_api.references.helpers import generate_agency_slugs_for_agency_list


# In gather_award_ids, if any awards are found for IDVs in the second half of
# the union, by definition, they have to be grandchildren so even though the
# grandchild boolean appears to be applying to the IDV, it will actually
# trickle down to its children.
ACTIVITY_SQL = SQL(
    """
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
        ta.toptier_agency_id                            toptier_agency_id,
        ca.awarding_agency_id                           awarding_agency_id,
        ca.generated_unique_award_id,
        tf.period_of_perf_potential_e                   period_of_performance_potential_end_date,
        pa.id                                           parent_award_id,
        pa.generated_unique_award_id                    parent_generated_unique_award_id,
        ca.parent_award_piid,
        ca.total_obligation                             obligated_amount,
        ca.base_and_all_options_value                   awarded_amount,
        ca.period_of_performance_start_date,
        ca.piid,
        rl.legal_business_name                          recipient_name,
        rp.recipient_hash || '-' || rp.recipient_level  recipient_id,
        gaids.grandchild
    from
        gather_award_ids gaids
        inner join vw_awards pa on pa.id = gaids.award_id
        inner join vw_awards ca on
            ca.parent_award_piid = pa.piid and
            ca.fpds_parent_agency_id = pa.fpds_agency_id and
            ca.type not like 'IDV%'
            {hide_edges_awarded_amount}
        left outer join vw_transaction_fpds tf on tf.transaction_id = ca.latest_transaction_id
        left outer join recipient_lookup rl on (
            (rl.uei is not null and rl.uei = tf.awardee_or_recipient_uei)
            OR (rl.duns is not null and rl.duns = tf.awardee_or_recipient_uniqu)

        )
        left outer join recipient_profile rp on (
            rp.recipient_hash = rl.recipient_hash and
            rp.recipient_level = case
                when tf.ultimate_parent_uei is null
                then 'R' else 'C'
            end
        )
        left outer join agency a on a.id = ca.awarding_agency_id
        left outer join toptier_agency ta on ta.toptier_agency_id = a.toptier_agency_id
    {hide_edges_end_date}
    order by
        ca.total_obligation desc, ca.id desc
    limit {limit} offset {offset}
"""
)


# So, as it turns out, we already count all descendant contracts.  Go us!
# There's always the chance these may not 100% match the actual count for a
# myriad of reasons, but they pretty much all involve failed operations
# processes or bad data so we're going to go ahead and give the benefit of
# the doubt and assume everything works as expected.
COUNT_ACTIVITY_SQL = SQL(
    """
    select  rollup_contract_count
    from    parent_award
    where   {award_id_column} = {award_id}
"""
)

COUNT_ACTIVITY_HIDDEN_SQL = SQL(
    """
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
        count(*) rollup_contract_count
    from
        gather_award_ids gaids
        inner join vw_awards pa on pa.id = gaids.award_id
        inner join vw_awards ca on
            ca.parent_award_piid = pa.piid and
            ca.fpds_parent_agency_id = pa.fpds_agency_id and
            ca.type not like 'IDV%'
            {hide_edges_awarded_amount}
        left outer join vw_transaction_fpds tf on tf.transaction_id = ca.latest_transaction_id
    {hide_edges_end_date}
"""
)


def _prepare_tiny_shield_models():
    # This endpoint has a fixed sort.  No need for "sort" or "order".
    models = [copy(p) for p in PAGINATION if p["name"] in ("page", "limit")]
    models.extend([get_internal_or_generated_award_id_model()])
    models.extend(
        [{"key": "hide_edge_cases", "name": "hide_edge_cases", "type": "boolean", "optional": True, "default": False}]
    )

    return models


TINY_SHIELD_MODELS = _prepare_tiny_shield_models()


class IDVActivityViewSet(APIView):
    """
    Returns award funding info for children and grandchildren of an IDV.  Used
    to power the Activity visualization on IDV Summary page.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/idvs/activity.md"

    @staticmethod
    def _parse_and_validate_request(request: dict) -> dict:
        return TinyShield(deepcopy(TINY_SHIELD_MODELS)).block(request)

    @staticmethod
    def _business_logic(request_data: dict) -> tuple:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data["award_id"]
        hide_edge_cases = request_data.get("hide_edge_cases")
        hide_edges_awarded_amount = ""
        hide_edges_end_date = ""
        award_id_column = "award_id" if type(award_id) is int else "generated_unique_award_id"
        if hide_edge_cases:
            hide_edges_awarded_amount = "and ca.base_and_all_options_value > 0 and ca.total_obligation > 0"
            hide_edges_end_date = "where tf.period_of_perf_potential_e is not null"
            sql = COUNT_ACTIVITY_HIDDEN_SQL.format(
                award_id_column=Identifier(award_id_column),
                award_id=Literal(award_id),
                hide_edges_awarded_amount=SQL(hide_edges_awarded_amount),
                hide_edges_end_date=SQL(hide_edges_end_date),
            )
        else:
            sql = COUNT_ACTIVITY_SQL.format(award_id_column=Identifier(award_id_column), award_id=Literal(award_id))
        overall_count_results = execute_sql_to_ordered_dictionary(sql)
        overall_count = overall_count_results[0]["rollup_contract_count"] if overall_count_results else 0
        sql = ACTIVITY_SQL.format(
            award_id_column=Identifier(award_id_column),
            award_id=Literal(award_id),
            limit=Literal(request_data["limit"] + 1),
            offset=Literal((request_data["page"] - 1) * request_data["limit"]),
            hide_edges_awarded_amount=SQL(hide_edges_awarded_amount),
            hide_edges_end_date=SQL(hide_edges_end_date),
        )

        results = execute_sql_to_ordered_dictionary(sql)

        agency_slugs = generate_agency_slugs_for_agency_list([res["toptier_agency_id"] for res in results])

        for res in results:
            # Set Agency Slug
            res["awarding_agency_slug"] = agency_slugs.get(res.get("toptier_agency_id"))

            # Remove extra fields
            del res["toptier_agency_id"]

        return results, overall_count

    @cache_response()
    def post(self, request: Request) -> Response:
        request_data = self._parse_and_validate_request(request.data)
        results, overall_count = self._business_logic(request_data)
        page_metadata = get_pagination_metadata(overall_count, request_data["limit"], request_data["page"])

        response = OrderedDict((("results", results[: request_data["limit"]]), ("page_metadata", page_metadata)))

        return Response(response)
