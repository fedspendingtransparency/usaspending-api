from collections import OrderedDict

from psycopg2.sql import Identifier, Literal, SQL
from rest_framework.request import Request
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.common.helpers.generic_helper import get_pagination
from usaspending_api.common.helpers.sql_helpers import execute_sql_to_ordered_dictionary
from usaspending_api.common.validator.award import get_internal_or_generated_award_id_model
from usaspending_api.common.validator.pagination import customize_pagination_with_sort_columns
from usaspending_api.common.validator.tinyshield import validate_post_request


SORTABLE_COLUMNS = {
    "federal_account": "federal_account",
    "transaction_obligated_amount": "transaction_obligated_amount",
    "agency": "funding_agency_name",
    "account_title": "fa.account_title",
    "object_class": ["oc.object_class_name", "oc.object_class"],
    "program_activity": ["rpa.program_activity_code", "rpa.program_activity_name"],
    "reporting_fiscal_date": ["sa.reporting_fiscal_year", "sa.reporting_fiscal_quarter"],
    "transaction_obligated_amount": ["gfaba.transaction_obligated_amount"],
}


DEFAULT_SORT_COLUMN = "federal_account"


FUNDING_SQL = SQL(
    """
    with gather_financial_accounts_by_awards as (
        select  a.funding_agency_id, faba.submission_id,
                nullif(faba.transaction_obligated_amount, 'NaN') transaction_obligated_amount,
                faba.treasury_account_id, faba.object_class_id, faba.program_activity_id
        from    awards a
                inner join financial_accounts_by_awards faba on faba.award_id = a.id
        where   {award_id_column} = {award_id}
    ),
    agency_id_to_agency_id_for_toptier_mapping as (
    select
        t.agency_id,
        t.toptier_agency_id,
        t.toptier_agency_name
        from (
            select
                case when sa.subtier_agency_id is null then 1173 else a.id end                   agency_id,
                ta.toptier_agency_id,
                ta.name                 toptier_agency_name,
                row_number() over (
                    partition by ta.toptier_agency_id
                    order by sa.name is not distinct from ta.name desc, a.update_date asc, a.id desc
                ) as per_toptier_row_number
            from
                agency a
                inner join toptier_agency ta on ta.toptier_agency_id = a.toptier_agency_id
                left outer join subtier_agency sa on sa.subtier_agency_id = a.subtier_agency_id
        ) t
        where t.per_toptier_row_number = 1
    )
    select
        coalesce(gfaba.transaction_obligated_amount, 0.0)              total_transaction_obligated_amount,
        taa.agency_id || '-' || taa.main_account_code                  federal_account,
        fa.account_title,
        afmap.toptier_agency_name                                      funding_agency_name,
        afmap.agency_id                                                funding_agency_id,
        aamap.toptier_agency_name                                      awarding_agency_name,
        aamap.agency_id                                                awarding_agency_id,
        oc.object_class                                                object_class,
        oc.object_class_name                                           object_class_name,
        pa.program_activity_code                                       program_activity_code,
        pa.program_activity_name                                       program_activity_name,
        sa.reporting_fiscal_year,
        sa.reporting_fiscal_quarter
    from
        gather_financial_accounts_by_awards gfaba
        left outer join treasury_appropriation_account taa on
            taa.treasury_account_identifier = gfaba.treasury_account_id
        left outer join federal_account fa on
            fa.agency_identifier = taa.agency_id and
            fa.main_account_code = taa.main_account_code
        left outer join agency_id_to_agency_id_for_toptier_mapping afmap on
            afmap.toptier_agency_id = taa.funding_toptier_agency_id
        left outer join agency_id_to_agency_id_for_toptier_mapping aamap on
            aamap.toptier_agency_id = taa.awarding_toptier_agency_id
        left outer join object_class oc on
            gfaba.object_class_id = oc.id
        left outer join ref_program_activity pa on
            gfaba.program_activity_id = pa.id
        left outer join submission_attributes sa on
            gfaba.submission_id = sa.submission_id
    group by
         federal_account, fa.account_title, funding_agency_name, awarding_agency_name, afmap.agency_id, aamap.agency_id,
         oc.object_class_name, pa.program_activity_name, oc.object_class, pa.program_activity_code,
         sa.reporting_fiscal_year, sa.reporting_fiscal_quarter, gfaba.transaction_obligated_amount
    order by
        {order_by} {order_direction};
"""
)


TINYSHIELD_MODELS = customize_pagination_with_sort_columns(list(SORTABLE_COLUMNS.keys()), DEFAULT_SORT_COLUMN)
TINYSHIELD_MODELS.append(get_internal_or_generated_award_id_model())


@validate_post_request(TINYSHIELD_MODELS)
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
        award_id_column = "award_id" if type(award_id) is int else "generated_unique_award_id"

        sql = FUNDING_SQL.format(
            award_id_column=Identifier(award_id_column),
            award_id=Literal(award_id),
            order_by=SQL(SORTABLE_COLUMNS[request_data["sort"]]),
            order_direction=SQL(request_data["order"]),
        )

        return execute_sql_to_ordered_dictionary(sql)

    @cache_response()
    def post(self, request: Request) -> Response:
        results = self._business_logic(request.data)
        paginated_results, page_metadata = get_pagination(results, request.data["limit"], request.data["page"])
        response = OrderedDict((("results", paginated_results), ("page_metadata", page_metadata)))

        return Response(response)
