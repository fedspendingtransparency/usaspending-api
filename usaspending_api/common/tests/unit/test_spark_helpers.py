from usaspending_api.common.helpers.spark_helpers import clean_postgres_sql_for_spark_sql


def test_clean_postgres():
    view_sql = """
    SELECT
        ROW_NUMBER() OVER (ORDER BY country_name, state_name) AS id,
        country_name,
        state_name,
        array_agg(DISTINCT(city_name)) FILTER (WHERE city_name IS NOT NULL) AS cities,
        json_agg(DISTINCT(jsonb_build_object(
            'name', county_name,
            'fips', CONCAT(state_fips, county_fips)
        )))
        FILTER (
            WHERE (
                county_name IS NOT NULL
                AND
                state_fips IS NOT NULL
                AND
                county_fips IS NOT NULL
            )
        )
        AS counties,
        array_agg(DISTINCT(zip_code)) FILTER (WHERE zip_code IS NOT NULL) AS zip_codes,
        array_agg(DISTINCT(current_congressional_district)) FILTER (WHERE current_congressional_district IS NOT NULL) AS current_congressional_districts,
        array_agg(DISTINCT(original_congressional_district)) FILTER (WHERE original_congressional_district IS NOT NULL) AS original_congressional_districts
    FROM
        locations_cte
    """
    identifier_replacements = {
        "json_agg": "collect_list",
        "array_agg": "collect_list",
        "jsonb_build_object": "named_struct",
    }
    result = clean_postgres_sql_for_spark_sql(
        postgres_sql_str=view_sql,
        global_temp_view_proxies=None,
        identifier_replacements=identifier_replacements,
    )
    expected = """
    SELECT
        ROW_NUMBER() OVER (ORDER BY country_name, state_name) AS id,
        country_name,
        state_name,
        collect_list(DISTINCT(city_name)) FILTER (WHERE city_name IS NOT NULL) AS cities,
        collect_list(DISTINCT(named_struct(
            'name', county_name,
            'fips', CONCAT(state_fips, county_fips)
        )))
        FILTER (
            WHERE (
                county_name IS NOT NULL
                AND
                state_fips IS NOT NULL
                AND
                county_fips IS NOT NULL
            )
        )
        AS counties,
        collect_list(DISTINCT(zip_code)) FILTER (WHERE zip_code IS NOT NULL) AS zip_codes,
        collect_list(DISTINCT(current_congressional_district)) FILTER (WHERE current_congressional_district IS NOT NULL) AS current_congressional_districts,
        collect_list(DISTINCT(original_congressional_district)) FILTER (WHERE original_congressional_district IS NOT NULL) AS original_congressional_districts
    FROM
        locations_cte
    """
    assert result == expected
