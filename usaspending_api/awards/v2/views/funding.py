from collections import OrderedDict
from copy import deepcopy

from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_simple_pagination_metadata
from usaspending_api.common.helpers.sql_helpers import build_composable_order_by, execute_sql_to_ordered_dictionary
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.pagination import customize_pagination_with_sort_columns
from usaspending_api.common.validator.tinyshield import validate_post_request
from usaspending_api.references.constants import DOD_CGAC, DOD_SUBSUMED_CGAC


SORTABLE_COLUMNS = {
    "federal_account": "federal_account",
    "transaction_obligated_amount": "transaction_obligated_amount",
    "agency": "funding_agency_name",
    "account_title": "fa.account_title",
    "object_class": ["oc.object_class", "oc.object_class_name"],
    "program_activity": ["pa.program_activity_code", "pa.program_activity_name"],
    "reporting_fiscal_date": ["sa.reporting_fiscal_year", "sa.reporting_fiscal_quarter"],
    "funding_agency_name": "funding_agency_name",
    "awarding_agency_name": "funding_agency_name",
}


DEFAULT_SORT_COLUMN = "federal_account"


FUNDING_SQL = SQL(
    """
    with
    get_dod as (
        select  a.id
        from    agency as a
                inner join toptier_agency as ta on ta.toptier_agency_id = a.toptier_agency_id
        where   toptier_code = {dod_cgac}
        order
        by      a.toptier_flag desc, a.id asc
        limit 1
    ), get_subsumed_agency_ids as (
        select  a.id
        from    agency as a
                inner join toptier_agency as ta on ta.toptier_agency_id = a.toptier_agency_id
        where   toptier_code in {subsumed_cgacs}
    ), gather_financial_accounts_by_awards as (
        select  case
                    when a.awarding_agency_id in (select id from get_subsumed_agency_ids) then (select id from get_dod)
                    else a.awarding_agency_id
                end awarding_agency_id,
                case
                    when a.funding_agency_id in (select id from get_subsumed_agency_ids) then (select id from get_dod)
                    else a.funding_agency_id
                end funding_agency_id,
                faba.submission_id,
                nullif(faba.transaction_obligated_amount, 'NaN') transaction_obligated_amount,
                faba.treasury_account_id, faba.object_class_id, faba.program_activity_id
        from    awards as a
                inner join financial_accounts_by_awards as faba on faba.award_id = a.id
        where   a.{award_id_column} = {award_id}
    )
    select
        coalesce(gfaba.transaction_obligated_amount, 0.0)               transaction_obligated_amount,
        taa.agency_id || '-' || taa.main_account_code                   federal_account,
        fa.account_title,
        fta.name                                                        funding_agency_name,
        faa.id                                                          funding_agency_id,
        ata.name                                                        awarding_agency_name,
        aa.id                                                           awarding_agency_id,
        oc.object_class                                                 object_class,
        oc.object_class_name                                            object_class_name,
        pa.program_activity_code                                        program_activity_code,
        pa.program_activity_name                                        program_activity_name,
        sa.reporting_fiscal_year,
        sa.reporting_fiscal_quarter
    from
        gather_financial_accounts_by_awards gfaba
        left outer join treasury_appropriation_account taa on
            taa.treasury_account_identifier = gfaba.treasury_account_id
        left outer join federal_account fa on
            fa.agency_identifier = taa.agency_id and
            fa.main_account_code = taa.main_account_code
        left outer join agency aa on
            aa.id = gfaba.awarding_agency_id
        left outer join toptier_agency ata on
            ata.toptier_agency_id = aa.toptier_agency_id
        left outer join agency faa on faa.id =
            gfaba.funding_agency_id
        left outer join toptier_agency fta on
            fta.toptier_agency_id = faa.toptier_agency_id
        left outer join object_class oc on
            gfaba.object_class_id = oc.id
        left outer join ref_program_activity pa on
            gfaba.program_activity_id = pa.id
        left outer join submission_attributes sa on
            gfaba.submission_id = sa.submission_id
    {order_by}
    limit {limit} offset {offset};
"""
)


TINYSHIELD_MODELS = customize_pagination_with_sort_columns(list(SORTABLE_COLUMNS.keys()), DEFAULT_SORT_COLUMN)
TINYSHIELD_MODELS.append(get_internal_or_generated_award_id_model())


@validate_post_request(deepcopy(TINYSHIELD_MODELS))
class AwardFundingViewSet(APIView):
    """
    These endpoints are used to power USAspending.gov's Award V2 pages federal account funding table
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/awards/funding.md"

    @staticmethod
    def _business_logic(request_data: dict) -> list:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data["award_id"]
        award_id_column = "id" if type(award_id) is int else "generated_unique_award_id"

        sql = FUNDING_SQL.format(
            dod_cgac=Literal(DOD_CGAC),
            subsumed_cgacs=Literal(tuple(DOD_SUBSUMED_CGAC)),
            award_id_column=Identifier(award_id_column),
            award_id=Literal(award_id),
            order_by=build_composable_order_by(SORTABLE_COLUMNS[request_data["sort"]], request_data["order"]),
            limit=Literal(request_data["limit"] + 1),
            offset=Literal((request_data["page"] - 1) * request_data["limit"]),
        )
        return execute_sql_to_ordered_dictionary(sql)

    @cache_response()
    def post(self, request: Request) -> Response:
        results = self._business_logic(request.data)
        page_metadata = get_simple_pagination_metadata(len(results), request.data["limit"], request.data["page"])
        response = OrderedDict((("results", results[: request.data["limit"]]), ("page_metadata", page_metadata)))

        return Response(response)
