from usaspending_api.common.helpers.sql_helpers import split_sql_statements


def test_single_statement_no_semicolon():
    sql = "SELECT * FROM users"
    result = split_sql_statements(sql)
    assert len(result) == 1
    assert result[0] == "SELECT * FROM users"


def test_single_statement_with_semicolon():
    sql = "SELECT * FROM users;"
    result = split_sql_statements(sql)
    assert len(result) == 1
    assert result[0] == "SELECT * FROM users"


def test_multiple_statements():
    sql = "SELECT * FROM users; SELECT * FROM orders"
    result = split_sql_statements(sql)
    assert len(result) == 2
    assert result[0] == "SELECT * FROM users"
    assert result[1] == "SELECT * FROM orders"


def test_empty_string():
    sql = ""
    result = split_sql_statements(sql)
    assert len(result) == 0


def test_whitespace_only():
    sql = "  \n\t   "
    result = split_sql_statements(sql)
    assert len(result) == 0


# Quote Handling


def test_semicolon_in_single_quotes():
    sql = "SELECT * FROM users WHERE name = 'weird;name';"
    result = split_sql_statements(sql)
    assert len(result) == 1
    assert result[0] == "SELECT * FROM users WHERE name = 'weird;name'"
