from usaspending_api.common.threaded_data_loader import cleanse_values


def test_cleanse_values():
    """Test that sloppy values in CSV are cleaned before use"""

    row = {"a": "  15", "b": "abcde", "c": "null", "d": "Null", "e": "   ", "f": " abc def "}
    result = cleanse_values(row)
    expected = {"a": "15", "b": "abcde", "c": None, "d": None, "e": "", "f": "abc def"}
    assert result == expected
