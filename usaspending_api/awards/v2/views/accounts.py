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
from usaspending_api.references.helpers import generate_agency_slugs_for_agency_list

SORTABLE_COLUMNS = {
    "federal_account": "federal_account",
    "total_transaction_obligated_amount": "total_transaction_obligated_amount",
    "agency": "funding_agency_name",
    "account_title": "account_title",
}


DEFAULT_SORT_COLUMN = "federal_account"


ACCOUNTS_SQL = SQL(
    """
    with gather_financial_accounts_by_awards as (
        select  a.funding_agency_id,
                nullif(faba.transaction_obligated_amount, 'NaN') transaction_obligated_amount,
                faba.treasury_account_id
        from    vw_awards a
                inner join financial_accounts_by_awards faba on faba.award_id = a.id
        where   {award_id_column} = {award_id}
    )
    select
        coalesce(sum(gfaba.transaction_obligated_amount), 0.0)         total_transaction_obligated_amount,
        taa.agency_id || '-' || taa.main_account_code                  federal_account,
        fa.account_title                                               account_title,
        ta.abbreviation                                                funding_agency_abbreviation,
        ta.name                                                        funding_agency_name,
        a.id                                                           funding_agency_id,
        a.toptier_agency_id                                            funding_toptier_agency_id
    from
        gather_financial_accounts_by_awards gfaba
        left outer join treasury_appropriation_account taa on
            taa.treasury_account_identifier = gfaba.treasury_account_id
        left outer join federal_account fa on
            fa.id = taa.federal_account_id
        left outer join agency a on a.id = gfaba.funding_agency_id
        left outer join toptier_agency ta on ta.toptier_agency_id = a.toptier_agency_id
    group by
        federal_account, fa.account_title, funding_agency_abbreviation, funding_agency_name,
        a.id
    order by
        {order_by} {order_direction};
"""
)


TINYSHIELD_MODELS = customize_pagination_with_sort_columns(list(SORTABLE_COLUMNS.keys()), DEFAULT_SORT_COLUMN)
TINYSHIELD_MODELS.append(get_internal_or_generated_award_id_model())


@validate_post_request(TINYSHIELD_MODELS)
class AwardAccountsViewSet(APIView):
    """
    These endpoints are used to power USAspending.gov's Award Summary Funding Accounts component.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/awards/accounts.md"

    @staticmethod
    def _business_logic(request_data: dict) -> list:
        # By this point, our award_id has been validated and cleaned up by
        # TinyShield.  We will either have an internal award id that is an
        # integer or a generated award id that is a string.
        award_id = request_data["award_id"]
        award_id_column = "award_id" if type(award_id) is int else "generated_unique_award_id"

        sql = ACCOUNTS_SQL.format(
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

        agency_slugs = generate_agency_slugs_for_agency_list(
            [res["funding_toptier_agency_id"] for res in paginated_results]
        )

        for res in paginated_results:
            res["funding_agency_slug"] = agency_slugs.get(res.get("funding_toptier_agency_id"))

        response = OrderedDict((("results", paginated_results), ("page_metadata", page_metadata)))

        return Response(response)
