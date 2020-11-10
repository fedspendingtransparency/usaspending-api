import asyncpg
import sqlparse

from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string


async def async_run_select(sql):
    conn = await asyncpg.connect(dsn=get_database_dsn_string())
    sql_result = await conn.fetch(sql)
    await conn.close()
    return sql_result


async def async_run_create(sql, verify_text=None):
    conn = await asyncpg.connect(dsn=get_database_dsn_string())
    stmt = await conn.prepare(sql)
    await stmt.fetch()
    response_msg = stmt.get_statusmsg()
    await conn.close()

    if verify_text:
        if response_msg != verify_text:
            raise RuntimeError("SQL did not return the correct response")


async def async_run_creates(sql_statements, wrapper):
    with wrapper:
        conn = await asyncpg.connect(dsn=get_database_dsn_string())
        for sql in sqlparse.split(sql_statements):
            stmt = await conn.prepare(sql)
            await stmt.fetch()
        await conn.close()
