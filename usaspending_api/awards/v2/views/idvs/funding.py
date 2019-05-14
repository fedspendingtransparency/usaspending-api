from collections import OrderedDict
from copy import deepcopy

from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.helpers.sql_helpers import build_composable_order_by, execute_sql_to_ordered_dictionary
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.pagination import customize_pagination_with_sort_columns
from usaspending_api.common.validator.tinyshield import TinyShield


SORTABLE_COLUMNS = {
    'account_title': ['fa.account_title'],
    'awarding_agency_name': ['awarding_agency_name'],
    'funding_agency_name': ['funding_agency_name'],
    'object_class': ['oc.object_class_name', 'oc.object_class'],
    'piid': ['gfaba.piid'],
    'program_activity': ['rpa.program_activity_code', 'rpa.program_activity_name'],
    'reporting_fiscal_date': ['sa.reporting_fiscal_year', 'sa.reporting_fiscal_quarter'],
    'transaction_obligated_amount': ['gfaba.transaction_obligated_amount']
}

# Add a unique id to every sort key so results are deterministic.
for k, v in SORTABLE_COLUMNS.items():
    v.append('gfaba.financial_accounts_by_awards_id')

DEFAULT_SORT_COLUMN = 'reporting_fiscal_date'

# Get funding information for child and grandchild contracts of an IDV but
# not the IDVs themselves.  As per direction from the product owner, agency
# data is to be retrieved from the File D (awards) data not File C
# (financial_accounts_by_awards).  Also, even though this query structure looks
# terrible, it managed to boost performance a bit over straight joins.
# agency_id_to_agency_id_for_toptier_mapping is used to turn agency ids into
# agency ids for toptier agencies.  Unfortunately that logic is... not 100%
# straightforward.  To determine if an agency is a toptier agency, apparently
# the topter name and subtier names have to match and, even then, there can be
# more than one match... or no match in the three cases where agencies don't
# have subtiers.
GET_FUNDING_SQL = SQL("""
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
                ca.generated_unique_award_id,
                ca.piid,
                ca.awarding_agency_id,
                ca.funding_agency_id
        from    gather_award_ids gaids
                inner join awards pa on pa.id = gaids.award_id
                inner join awards ca on
                    ca.parent_award_piid = pa.piid and
                    ca.fpds_parent_agency_id = pa.fpds_agency_id and
                    ca.type not like 'IDV%' and
                    (ca.piid = {piid} or {piid} is null)
    ), gather_financial_accounts_by_awards as (
        select  ga.award_id,
                ga.generated_unique_award_id,
                ga.piid,
                ga.awarding_agency_id,
                ga.funding_agency_id,
                nullif(faba.transaction_obligated_amount, 'NaN') transaction_obligated_amount,
                faba.financial_accounts_by_awards_id,
                faba.submission_id,
                faba.treasury_account_id,
                faba.program_activity_id,
                faba.object_class_id
        from    gather_awards ga
                inner join financial_accounts_by_awards faba on faba.award_id = ga.award_id
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
        gfaba.award_id,
        gfaba.generated_unique_award_id,
        sa.reporting_fiscal_year,
        sa.reporting_fiscal_quarter,
        gfaba.piid,
        aamap.agency_id_for_toptier         awarding_agency_id,
        aamap.toptier_agency_name           awarding_agency_name,
        famap.agency_id_for_toptier         funding_agency_id,
        famap.toptier_agency_name           funding_agency_name,
        taa.agency_id,
        taa.main_account_code,
        fa.account_title,
        rpa.program_activity_code,
        rpa.program_activity_name,
        oc.object_class,
        oc.object_class_name,
        gfaba.transaction_obligated_amount
    from
        gather_financial_accounts_by_awards gfaba
        left outer join submission_attributes sa on sa.submission_id = gfaba.submission_id
        left outer join treasury_appropriation_account taa on
            taa.treasury_account_identifier = gfaba.treasury_account_id
        left outer join federal_account fa on fa.id = taa.federal_account_id
        left outer join ref_program_activity rpa on rpa.id = gfaba.program_activity_id
        left outer join object_class oc on oc.id = gfaba.object_class_id
        left outer join agency_id_to_agency_id_for_toptier_mapping aamap on aamap.agency_id = gfaba.awarding_agency_id
        left outer join agency_id_to_agency_id_for_toptier_mapping famap on famap.agency_id = gfaba.funding_agency_id
    {order_by}
    limit {limit} offset {offset}
""")


def _prepare_tiny_shield_models():
    models = customize_pagination_with_sort_columns(list(SORTABLE_COLUMNS.keys()), DEFAULT_SORT_COLUMN)
    models.extend([
        get_internal_or_generated_award_id_model(),
        {'key': 'piid', 'name': 'piid', 'optional': True, 'type': 'text', 'text_type': 'search'}
    ])
    return models


TINY_SHIELD_MODELS = _prepare_tiny_shield_models()


class IDVFundingViewSet(APIDocumentationView):
    """Returns File C funding records associated with an IDV."""

    @staticmethod
    def _parse_and_validate_request(request_data: dict) -> dict:
        return TinyShield(deepcopy(TINY_SHIELD_MODELS)).block(request_data)

    @staticmethod
    def _business_logic(request_data: dict) -> list:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data['award_id']
        award_id_column = 'award_id' if type(award_id) is int else 'generated_unique_award_id'

        sql = GET_FUNDING_SQL.format(
            award_id_column=Identifier(award_id_column),
            award_id=Literal(award_id),
            piid=Literal(request_data.get('piid')),
            order_by=build_composable_order_by(SORTABLE_COLUMNS[request_data['sort']], request_data['order']),
            limit=Literal(request_data['limit'] + 1),
            offset=Literal((request_data['page'] - 1) * request_data['limit']),
        )

        return execute_sql_to_ordered_dictionary(sql)

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
