from usaspending_api.data_load.data_load_helpers import capitalize_if_string, false_if_null, format_value_for_sql


def test_capitalize_if_string():
    assert capitalize_if_string("bob4") == "BOB4"
    assert capitalize_if_string(7) == 7
    assert capitalize_if_string(None) is None


def test_false_if_null():
    assert false_if_null("bob") == "bob"
    assert false_if_null(True)
    assert not false_if_null(False)
    assert not false_if_null(None)


def test_sql_formatter():
    assert format_value_for_sql(None) == "null"
    assert format_value_for_sql("null") == "'null'"
    assert format_value_for_sql(599) == "599"
    assert format_value_for_sql("599") == "'599'"
    assert format_value_for_sql([1, 2, "steve"]) == "ARRAY[1,2,'steve']"
