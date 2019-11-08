import pytest
import pytz

from argparse import ArgumentTypeError
from datetime import datetime, timezone
from usaspending_api.common.helpers.date_helper import (
    cast_datetime_to_naive,
    cast_datetime_to_utc,
    datetime_command_line_argument_type,
    get_date_from_datetime,
)


def test_cast_datetime_to_naive():
    expected = datetime(2000, 1, 2, 3, 4, 5)

    assert cast_datetime_to_naive(datetime(2000, 1, 2, 3, 4, 5)) == expected
    assert cast_datetime_to_naive(datetime(2000, 1, 2, 3, 4, 5, tzinfo=timezone.utc)) == expected
    assert cast_datetime_to_naive(pytz.timezone("US/Eastern").localize(datetime(2000, 1, 1, 22, 4, 5))) == expected


def test_cast_datetime_to_utc():
    expected = datetime(2000, 1, 2, 3, 4, 5, tzinfo=timezone.utc)

    assert cast_datetime_to_utc(datetime(2000, 1, 2, 3, 4, 5)) == expected
    assert cast_datetime_to_utc(datetime(2000, 1, 2, 3, 4, 5, tzinfo=timezone.utc)) == expected
    assert cast_datetime_to_utc(pytz.timezone("US/Eastern").localize(datetime(2000, 1, 1, 22, 4, 5))) == expected


def test_datetime_command_line_argument_type():
    assert datetime_command_line_argument_type(True)("2000-01-02") == datetime(2000, 1, 2, tzinfo=None)
    assert datetime_command_line_argument_type(False)("2000-01-02") == datetime(2000, 1, 2, tzinfo=timezone.utc)

    assert datetime_command_line_argument_type(True)("2 jan 2000") == datetime(2000, 1, 2, tzinfo=None)
    assert datetime_command_line_argument_type(False)("2 jan 2000") == datetime(2000, 1, 2, tzinfo=timezone.utc)

    assert datetime_command_line_argument_type(True)("jan 2 2000") == datetime(2000, 1, 2, tzinfo=None)
    assert datetime_command_line_argument_type(False)("jan 2 2000") == datetime(2000, 1, 2, tzinfo=timezone.utc)

    assert datetime_command_line_argument_type(True)("1/2/2000") == datetime(2000, 1, 2, tzinfo=None)
    assert datetime_command_line_argument_type(False)("1/2/2000") == datetime(2000, 1, 2, tzinfo=timezone.utc)

    assert datetime_command_line_argument_type(True)("2000-01-02 3:04:05.06") == datetime(
        2000, 1, 2, 3, 4, 5, 60000, tzinfo=None
    )
    assert datetime_command_line_argument_type(False)("2000-01-02 3:04:05.06") == datetime(
        2000, 1, 2, 3, 4, 5, 60000, tzinfo=timezone.utc
    )

    assert datetime_command_line_argument_type(True)("2 jan 2000 3:04:05.06") == datetime(
        2000, 1, 2, 3, 4, 5, 60000, tzinfo=None
    )
    assert datetime_command_line_argument_type(False)("2 jan 2000 3:04:05.06") == datetime(
        2000, 1, 2, 3, 4, 5, 60000, tzinfo=timezone.utc
    )

    assert datetime_command_line_argument_type(True)("1/2/2000 3:04:05.06") == datetime(
        2000, 1, 2, 3, 4, 5, 60000, tzinfo=None
    )
    assert datetime_command_line_argument_type(False)("1/2/2000 3:04:05.06") == datetime(
        2000, 1, 2, 3, 4, 5, 60000, tzinfo=timezone.utc
    )

    assert datetime_command_line_argument_type(True)("2000-01-02 7:04:05.06+0400") == datetime(
        2000, 1, 2, 3, 4, 5, 60000, tzinfo=None
    )
    assert datetime_command_line_argument_type(False)("2000-01-02 7:04:05.06+0400") == datetime(
        2000, 1, 2, 3, 4, 5, 60000, tzinfo=timezone.utc
    )

    assert datetime_command_line_argument_type(True)("2000-01-02 3:04:05Z") == datetime(
        2000, 1, 2, 3, 4, 5, tzinfo=None
    )
    assert datetime_command_line_argument_type(False)("2000-01-02 3:04:05Z") == datetime(
        2000, 1, 2, 3, 4, 5, tzinfo=timezone.utc
    )

    assert datetime_command_line_argument_type(True)("2000-01-02T3:04:05Z") == datetime(
        2000, 1, 2, 3, 4, 5, tzinfo=None
    )
    assert datetime_command_line_argument_type(False)("2000-01-02T3:04:05Z") == datetime(
        2000, 1, 2, 3, 4, 5, tzinfo=timezone.utc
    )

    assert datetime_command_line_argument_type(True)("2000-1-2 3:4:5") == datetime(2000, 1, 2, 3, 4, 5, tzinfo=None)
    assert datetime_command_line_argument_type(False)("2000-1-2 3:4:5") == datetime(
        2000, 1, 2, 3, 4, 5, tzinfo=timezone.utc
    )

    with pytest.raises(ArgumentTypeError):
        assert datetime_command_line_argument_type(True)(None)
    with pytest.raises(ArgumentTypeError):
        assert datetime_command_line_argument_type(False)(None)

    with pytest.raises(ArgumentTypeError):
        assert datetime_command_line_argument_type(True)("a")
    with pytest.raises(ArgumentTypeError):
        assert datetime_command_line_argument_type(False)("a")

    with pytest.raises(ArgumentTypeError):
        assert datetime_command_line_argument_type(True)("#")
    with pytest.raises(ArgumentTypeError):
        assert datetime_command_line_argument_type(False)("#")

    with pytest.raises(ArgumentTypeError):
        assert datetime_command_line_argument_type(True)("2000-01-35")
    with pytest.raises(ArgumentTypeError):
        assert datetime_command_line_argument_type(False)("2000-01-35")


def test_get_date_from_datetime():
    from datetime import date, datetime

    assert get_date_from_datetime(1) == 1
    assert get_date_from_datetime(1, default=2) == 2
    assert get_date_from_datetime(1, default=None) is None

    assert get_date_from_datetime("a") == "a"
    assert get_date_from_datetime("a", default="b") == "b"

    assert get_date_from_datetime(datetime(2000, 1, 2)) == date(2000, 1, 2)
    assert get_date_from_datetime(datetime(2000, 1, 2), default="no") == date(2000, 1, 2)

    assert get_date_from_datetime(datetime(2000, 1, 2, 3, 4, 5)) == date(2000, 1, 2)
    assert get_date_from_datetime(datetime(2000, 1, 2, 3, 4, 5), default="maybe") == date(2000, 1, 2)
