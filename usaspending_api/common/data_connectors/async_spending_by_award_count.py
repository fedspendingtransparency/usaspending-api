import asyncio
import asyncpg

from django.db.models import Count

from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string


async def run(sql):
    conn = await asyncpg.connect(dsn=get_database_dsn_string())
    removeal_index = sql.find("GROUP BY")  # Django refuses to provide a viable option to exclude "GROUP BY ..."
    sql_result = await conn.fetch(sql[:removeal_index])
    await conn.close()
    print(sql_result)
    return sql_result[0]["count"]


def async_fetch_category_counts(filters, category_to_model_dict):
    loop = asyncio.new_event_loop()
    results = {}
    for k, v in category_to_model_dict.items():
        queryset = matview_search_filter(filters, v).annotate(count=Count("*")).values("count")
        sql = generate_raw_quoted_query(queryset)
        results[k] = asyncio.ensure_future(run(sql), loop=loop)

    all_statements = asyncio.gather(*[value for value in results.values()])
    loop.run_until_complete(all_statements)
    loop.close()

    return {k: v.result() for k, v in results.items()}
