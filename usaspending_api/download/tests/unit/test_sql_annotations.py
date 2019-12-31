import pytest

# Imports from your apps
from usaspending_api.download.filestreaming.download_generation import _top_level_split, _select_columns


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
    assert _select_columns(basic_sql) == [" steve", "jimmy", "bob "]
    nested_sql = (
        "SELECT (SELECT things FROM a place), stuff , (things FROM stuff) FROM otherstuff SELECT FROM FROM SELECT"
    )
    assert _select_columns(nested_sql) == [" (SELECT things FROM a place)", " stuff ", " (things FROM stuff) "]
