from usaspending_api.data_load.fpds_loader import setup_load_lists, fetch_broker_objects
import pytest

from unittest.mock import MagicMock


def mock_cursor(monkeypatch, result_value):
    """
    A mock connection/cursor that ignores calls to cursor.execute and returns from fetchall
    whatever you tell it based on mock_cursor.expected_value.  This is a bare minimum
    implementation.  If you use any other connection or cursor features you may need to tweak
    this accordingly (or you may not... MagicMock is, um, "magic").
    """
    class Result:
        pass

    result = Result()
    result.expected_value = result_value

    def mock_cursor():
        mock_context = MagicMock()
        mock_context.execute.return_value = None
        mock_context.fetchall.return_value = result.expected_value
        return mock_context

    def mock_connection(cursor_factory=None):
        mock_context = MagicMock()
        mock_context.__enter__.return_value = mock_cursor()
        mock_context.__exit__.return_value = False
        return mock_context

    def mock_connect(dsn):
        mock_context = MagicMock()
        mock_context.__enter__.return_value.cursor = mock_connection
        mock_context.__exit__.return_value = False
        return mock_context

    monkeypatch.setattr("psycopg2.connect", mock_connect)

    return result


def test_setup_load_lists():
    test_object = {"table": {"val1": 4, "string_val": "bob"}, "wrong_table": {"val": "wrong"}}

    columns, values, pairs = setup_load_lists(test_object, "table")
    # code is non-deterministic in order to be performance
    assert columns == '("val1","string_val")' or columns == '("string_val","val1")'
    assert values == "(4,'bob')" or values == "('bob',4)"
    assert pairs == " val1=4, string_val='bob'" or pairs == " string_val='bob', val1=4"


def test_load_from_broker(monkeypatch):
    # There isn't much to test here since this is just a basic DB query
    mock_cursor(monkeypatch, ["mock thing 1", "mock thing 2"])
    assert fetch_broker_objects([17459650, 678]) == ["mock thing 1", "mock thing 2"]
