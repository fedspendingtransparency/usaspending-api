from usaspending_api.common.helpers.sql_helpers import split_sql_statements


# Basic Functionality


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
    sql = "SELECT * FROM users WHERE name = 'weird;name;';"
    result = split_sql_statements(sql)
    assert len(result) == 1
    assert result[0] == "SELECT * FROM users WHERE name = 'weird;name;'"


def test_semicolon_in_double_quotes():
    sql = 'SELECT * FROM users WHERE name = "weird;name;";'
    result = split_sql_statements(sql)
    assert len(result) == 1
    assert result[0] == 'SELECT * FROM users WHERE name = "weird;name;"'


def test_mixed_quotes():
    sql = """UPDATE users SET data='{"key": "value; test"}';"""
    result = split_sql_statements(sql)
    assert len(result) == 1
    assert result[0] == """UPDATE users SET data='{"key": "value; test"}'"""


def test_escaped_double_quote():
    sql = 'SELECT * FROM users WHERE name = "Test\\"Quote'
    result = split_sql_statements(sql)
    assert len(result) == 1
    assert result[0] == 'SELECT * FROM users WHERE name = "Test\\"Quote'


def test_escaped_single_quote():
    sql = "SELECT * FROM users WHERE name = 'O\\'Brien;;'"
    result = split_sql_statements(sql)
    assert len(result) == 1
    assert result[0] == "SELECT * FROM users WHERE name = 'O\\'Brien;;'"


# Multi-line statements


def test_multiple_multiline_statements():
    sql = """
SELECT *
FROM users
WHERE active = true;
SELECT *
FROM orders WHERE
status = 'pending;';  SELECT * FROM groups WHERE admin = false;
"""
    result = split_sql_statements(sql)
    assert len(result) == 3
    assert "users" in result[0]
    assert "orders" in result[1]
    assert "groups" in result[2]


# Edge Cases


def test_only_semicolins():
    sql = ";;;"
    result = split_sql_statements(sql)
    assert len(result) == 0
