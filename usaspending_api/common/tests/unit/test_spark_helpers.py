from django.conf import settings
from usaspending_api.common.helpers.spark_helpers import clean_postgres_sql_for_spark_sql


def test_clean_postgres():
    view_sql_file = f"{settings.ES_LOCATIONS_ETL_VIEW_NAME}.sql"
    view_sql = open(str(settings.APP_DIR / "database_scripts" / "etl" / view_sql_file), "r").read()
    identifier_replacements = {
        "json_agg": "collect_list",
        "jsonb_build_object": "named_struct",
    }
    print(view_sql)
    assert all([key in view_sql for key in identifier_replacements.keys()])
    result = clean_postgres_sql_for_spark_sql(
        postgres_sql_str=view_sql,
        global_temp_view_proxies=None,
        identifier_replacements=identifier_replacements,
    )
    assert not any([key in result for key in identifier_replacements.keys()])
