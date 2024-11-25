from usaspending_api.common.helpers.spark_helpers import clean_postgres_sql_for_spark_sql


def test_clean_postgres():
    view_sql = "json_agg(DISTINCT(jsonb_build_object()))"
    identifier_replacements = {
        "json_agg": "collect_list",
        "jsonb_build_object": "named_struct",
    }
    result = clean_postgres_sql_for_spark_sql(
        postgres_sql_str=view_sql,
        global_temp_view_proxies=None,
        identifier_replacements=identifier_replacements,
    )
    expected = "collect_list(DISTINCT(named_struct()))"
    assert result == expected
