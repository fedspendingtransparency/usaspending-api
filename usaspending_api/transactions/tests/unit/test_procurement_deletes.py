from unittest.mock import patch

import pytest

from usaspending_api.transactions.management.commands.delete_procurement_records import Command

LOGGER_PATH = "usaspending_api.transactions.management.commands.delete_procurement_records.logger"

DATE_KEY = "2024-01-01"


def validate(ids, date_key=DATE_KEY):
    return Command.validate_transaction_ids({date_key: ids})


@pytest.mark.parametrize(
    "raw_id, expected",
    [
        ("123.0", 123),  # float as a string
        (123.0, 123),  # float
        ("123", 123),  # int as a string
        (123, 123),  # int
    ],
)
def test_valid_ids_are_converted_to_int(raw_id, expected):
    result = validate([raw_id])

    assert result == {DATE_KEY: [expected]}
    assert all(type(v) is int for v in result[DATE_KEY])


@pytest.mark.parametrize("raw_id", ["abc", ""])
def test_invalid_ids_are_excluded(raw_id):
    with patch(LOGGER_PATH) as mock_logger:
        result = validate([raw_id])

    assert result == {}
    mock_logger.warning.assert_called_once_with(f"No valid IDs for {DATE_KEY}!")
    assert "Unexpected 1 non-numeric IDs" in mock_logger.info.call_args[0][0]


@pytest.mark.parametrize(
    "raw_id",
    [
        "nan",
        "NaN",
        float("nan"),
        "inf",
        "-Infinity",
        float("inf"),
        None,
        "-5",
        -5,
        "-5.0",
    ],
)
def test_unconvertible_or_negative_ids_are_excluded_without_raising(raw_id):
    with patch(LOGGER_PATH) as mock_logger:
        result = validate([raw_id])

    assert result == {}
    mock_logger.warning.assert_called_once_with(f"No valid IDs for {DATE_KEY}!")


def test_unconvertible_and_negative_ids_do_not_affect_valid_ids():
    result = validate(["1", "nan", None, "inf", "-5", 2])

    assert result == {DATE_KEY: [1, 2]}


def test_empty_id_list_is_excluded():
    with patch(LOGGER_PATH) as mock_logger:
        result = validate([])

    assert result == {}
    mock_logger.warning.assert_called_once_with(f"No IDs for {DATE_KEY}!")


def test_mixed_ids_keep_only_numeric_values():
    result = validate(["123.0", 456.0, "789", 1011, "abc", ""])

    assert result == {DATE_KEY: [123, 456, 789, 1011]}


def test_order_is_preserved():
    assert validate(["3", 1, "2.0"]) == {DATE_KEY: [3, 1, 2]}


def test_multiple_dates_are_handled_independently():
    result = Command.validate_transaction_ids(
        {
            "2024-01-01": ["1", "bad"],
            "2024-01-02": ["bad"],
            "2024-01-03": [2.0, "3.0"],
        }
    )

    assert result == {"2024-01-01": [1], "2024-01-03": [2, 3]}


def test_empty_input():
    assert Command.validate_transaction_ids({}) == {}


def test_non_numeric_ids_are_logged():
    with patch(LOGGER_PATH) as mock_logger:
        validate(["1", "abc", ""])

    messages = [call[0][0] for call in mock_logger.info.call_args_list]
    assert any("Unexpected 2 non-numeric IDs" in m for m in messages)
    assert any("Obtained 1 IDs" in m for m in messages)


@pytest.mark.parametrize(
    "raw_id, expected",
    [
        (" 42 ", 42),  # float() tolerates surrounding whitespace
        ("0", 0),
        ("1e3", 1000),  # scientific notation
        ("123.9", 123),  # decimals are truncated, not rounded
    ],
)
def test_numeric_edge_cases(raw_id, expected):
    assert validate([raw_id]) == {DATE_KEY: [expected]}


def test_large_id_keeps_precision_for_numeric_string():
    # Validating use of Decimal instead of float in the implementation to handle precision loss
    # due to float conversion
    assert validate(["9007199254740993"]) == {DATE_KEY: [9007199254740993]}
