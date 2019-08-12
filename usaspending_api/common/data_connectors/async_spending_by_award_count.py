import asyncio
import asyncpg

from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string


async def run(sql):
    conn = await asyncpg.connect(dsn=get_database_dsn_string())
    sql_result = await conn.fetch("SELECT COUNT(*) as count FROM ({}) as temp".format(sql))
    await conn.close()
    return sql_result[0]["count"]


def async_fetch_category_counts(filters, category_to_model_dict):
    loop = asyncio.new_event_loop()
    results = {}
    for k, v in category_to_model_dict.items():
        sql = generate_raw_quoted_query(matview_search_filter(filters, v))
        results[k] = asyncio.ensure_future(run(sql), loop=loop)

    all_statements = asyncio.gather(*[value for value in results.values()])
    loop.run_until_complete(all_statements)
    loop.close()

    return {k: v.result() for k, v in results.items()}
