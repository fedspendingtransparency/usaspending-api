import asyncpg

from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string


async def async_run_select(sql):
    conn = await asyncpg.connect(dsn=get_database_dsn_string())
    sql_result = await conn.fetch(sql)
    await conn.close()
    return sql_result
