import pytest

# Imports from your apps
from usaspending_api.download.filestreaming.download_generation import (
    apply_annotations_to_sql,
    _top_level_split,
    _select_columns,
)


def test_top_level_split_nesting_logic():
    unnested_string = "sdfoing SPLT noivijf"
    assert _top_level_split(unnested_string, "SPLT") == ["sdfoing ", " noivijf"]
    nested_string_1 = "() blah SP pojf"
    assert _top_level_split(nested_string_1, "SP") == ["() blah ", " pojf"]
    nested_string_2 = "(SP) blah SP pojf"
    assert _top_level_split(nested_string_2, "SP") == ["(SP) blah ", " pojf"]
    nested_string_3 = "(SP) blah SP (pojf SP) SP"
    assert _top_level_split(nested_string_3, "SP") == ["(SP) blah ", " (pojf SP) SP"]
    nested_string_4 = "((SP)) blah ()SP (pojf SP) SP"
    assert _top_level_split(nested_string_4, "SP") == ["((SP)) blah ()", " (pojf SP) SP"]


def test_top_level_split_quote_logic():
    unquoted_string = "oinodsg *** vsdoij"
    assert _top_level_split(unquoted_string, "***") == ["oinodsg ", " vsdoij"]
    quoted_string_1 = 'oinodsg "***"*** vsdoij'
    assert _top_level_split(quoted_string_1, "***") == ['oinodsg "***"', " vsdoij"]


def test_top_level_split_unsplittable():
    unsplittable = "sdoiaghioweg oijfewiwje efoiwnovisper"
    with pytest.raises(Exception):
        _top_level_split(unsplittable, "not in this")


def test_select_columns_nesting_logic():
    basic_sql = "SELECT steve,jimmy,bob FROM the bench"
    assert _select_columns(basic_sql) == (None, ["steve", "jimmy", "bob"])
    nested_sql = (
        "SELECT (SELECT things FROM a place), stuff , (things FROM stuff) FROM otherstuff SELECT FROM FROM SELECT"
    )
    assert _select_columns(nested_sql) == (None, ["(SELECT things FROM a place)", "stuff", "(things FROM stuff)"])
    cte_sql = (
        "WITH cte AS (SELECT inner_things FROM a inner_place) SELECT things FROM a_place "
        "INNER JOIN cte ON cte.inner_things = a_place.things"
    )
    assert _select_columns(cte_sql) == ("WITH cte AS (SELECT inner_things FROM a inner_place)", ["things"])


def test_apply_annotations_to_sql():
    sql_string = str(
        'SELECT "table"."col1", "table"."col2", (SELECT table2."three" FROM table_two table2 WHERE '
        'table2."code" = table."othercode") AS "alias_one" FROM table WHERE six = \'something\''
    )
    aliases = ["alias_one", "col1", "col2"]

    annotated_sql = apply_annotations_to_sql(sql_string, aliases)

    annotated_string = 'SELECT (SELECT table2."three" FROM table_two table2 WHERE table2."code" = table."othercode") AS "alias_one", "table"."col1" AS "col1", "table"."col2" AS "col2" FROM table WHERE six = \'something\''

    assert annotated_sql == annotated_string


def test_group_by_annotations():
    sql = (
        "SELECT"
        ' "some_table"."col1" AS "new_column_name",'
        ' CASE WHEN "some_table"."col2" = 1 THEN 1 ELSE 0 END AS "annotated_test1",'
        ' sum("some_table"."col3") AS "aggregate_col",'
        ' CASE WHEN "some_table"."col2" = 1 THEN 2 ELSE 3 END AS "annotated_test2"'
        " FROM some_table"
        ' GROUP BY "some_table"."col1", 2, 4'
    )
    aliases = ["new_column_name", "annotated_test1", "aggregate_col", "annotated_test2"]
    annotated_group_by_columns = ["annotated_test1", "annotated_test2"]

    expected_string = (
        "SELECT"
        ' "some_table"."col1" AS "new_column_name",'
        ' CASE WHEN "some_table"."col2" = 1 THEN 1 ELSE 0 END AS "annotated_test1",'
        ' sum("some_table"."col3") AS "aggregate_col",'
        ' CASE WHEN "some_table"."col2" = 1 THEN 2 ELSE 3 END AS "annotated_test2"'
        " FROM some_table"
        ' GROUP BY "some_table"."col1", CASE WHEN "some_table"."col2" = 1 THEN 1 ELSE 0 END,'
        ' CASE WHEN "some_table"."col2" = 1 THEN 2 ELSE 3 END'
    )

    assert apply_annotations_to_sql(sql, aliases, annotated_group_by_columns) == expected_string
