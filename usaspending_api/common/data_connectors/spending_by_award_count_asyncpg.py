import asyncio

from django.db.models import Count

from usaspending_api.awards.v2.filters.matview_filters import matview_search_filter
from usaspending_api.common.helpers.orm_helpers import generate_raw_quoted_query
from usaspending_api.common.data_connectors.async_sql_query import async_run_select


def fetch_all_category_counts(filters, category_to_model_dict):
    loop = asyncio.new_event_loop()
    results = {}
    for k, v in category_to_model_dict.items():
        queryset = matview_search_filter(filters, v).annotate(count=Count("*")).values("count")
        sql = generate_raw_quoted_query(queryset)

        # Django refuses to provide a viable option to exclude "GROUP BY ..." so it is stripped before running the SQL
        remove_groupby_string_index = sql.find("GROUP BY")
        results[k] = asyncio.ensure_future(async_run_select(sql[:remove_groupby_string_index]), loop=loop)

    all_statements = asyncio.gather(*[value for value in results.values()])
    loop.run_until_complete(all_statements)
    loop.close()

    return {k: v.result()[0]["count"] for k, v in results.items()}
