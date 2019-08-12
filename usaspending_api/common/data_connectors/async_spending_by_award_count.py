import asyncio
import asyncpg

from usaspending_api.awards.models_matviews import (
    ReportingAwardContractsView,
    ReportingAwardDirectPaymentsView,
    ReportingAwardGrantsView,
    ReportingAwardIdvsView,
    ReportingAwardLoansView,
    ReportingAwardOtherView,
)
from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string


QUERIES = {
    "contracts": ReportingAwardContractsView,
    "direct_payments": ReportingAwardDirectPaymentsView,
    "grants": ReportingAwardGrantsView,
    "idvs": ReportingAwardIdvsView,
    "loans": ReportingAwardLoansView,
    "other": ReportingAwardOtherView,
}


async def run(filters):
    conn = await asyncpg.connect(dsn=get_database_dsn_string())
    results = {}
    for k, v in QUERIES.items():
        sql = generate_raw_quoted_query(matview_search_filter(filters, v))
        # print("{}: {}".format(k, sql))
        results[k] = await conn.fetch("SELECT COUNT(*) as count FROM ({}) as temp".format(sql))
    await conn.close()
    return results


def get_values(filters):
    loop = asyncio.new_event_loop()
    results = loop.run_until_complete(run(filters))

    return {k: v[0]["count"] for k, v in results.items()}
