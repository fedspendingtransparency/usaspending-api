import pytest

from collections import OrderedDict, namedtuple

from django.contrib.auth.models import User  # A table that should always exist.  Doesn't really matter what it is.
from django.test import TestCase

from model_bakery import baker
from psycopg2.sql import SQL

from usaspending_api.common.helpers.sql_helpers import (
    build_composable_order_by,
    execute_sql_to_ordered_dictionary,
    ordered_dictionary_fetcher,
    get_connection,
)


AWARD_COUNT = 5

RAW_SQL = """
    select
        a.id               award_id,
        tn.id              transaction_normalized_id,
        tf.transaction_id  transaction_fpds_id
    from
        vw_awards a
        inner join vw_transaction_normalized tn on tn.award_id = a.id
        inner join vw_transaction_fpds tf on tf.transaction_id = tn.id
    order by
        a.id
"""

EXPECTED_RESPONSE_ORDERED_DICTIONARY = [
    OrderedDict((("award_id", 1), ("transaction_normalized_id", 1), ("transaction_fpds_id", 1))),
    OrderedDict((("award_id", 2), ("transaction_normalized_id", 2), ("transaction_fpds_id", 2))),
    OrderedDict((("award_id", 3), ("transaction_normalized_id", 3), ("transaction_fpds_id", 3))),
    OrderedDict((("award_id", 4), ("transaction_normalized_id", 4), ("transaction_fpds_id", 4))),
    OrderedDict((("award_id", 5), ("transaction_normalized_id", 5), ("transaction_fpds_id", 5))),
]

Result = namedtuple("Result", ["award_id", "transaction_normalized_id", "transaction_fpds_id"])
EXPECTED_RESPONSE_NAMED_TUPLE = [
    Result(award_id=1, transaction_normalized_id=1, transaction_fpds_id=1),
    Result(award_id=2, transaction_normalized_id=2, transaction_fpds_id=2),
    Result(award_id=3, transaction_normalized_id=3, transaction_fpds_id=3),
    Result(award_id=4, transaction_normalized_id=4, transaction_fpds_id=4),
    Result(award_id=5, transaction_normalized_id=5, transaction_fpds_id=5),
]


class CursorExecuteTestCase(TestCase):
    @classmethod
    def setUpTestData(cls):
        """
        Set up some awards and transactions that we can query.
        """
        for _id in range(1, AWARD_COUNT + 1):
            baker.make("search.TransactionSearch", is_fpds=True, transaction_id=_id, award_id=_id)
            baker.make("search.AwardSearch", award_id=_id, latest_transaction_id=_id)

    @staticmethod
    def test_build_composable_order_by():
        connection = get_connection()
        with connection.cursor() as cursor:

            def _build_composable_order_by(*args, **kwargs):
                result = build_composable_order_by(*args, **kwargs)
                return result.as_string(cursor.connection)

            assert _build_composable_order_by("column") == 'order by "column"'
            assert _build_composable_order_by("this.column") == 'order by "this"."column"'
            assert _build_composable_order_by("column", "asc") == 'order by "column" asc'
            assert _build_composable_order_by("column", sort_nulls="first") == 'order by "column" nulls first'
            assert _build_composable_order_by("column", "asc", "first") == 'order by "column" asc nulls first'

            assert _build_composable_order_by(["column1", "column2"]) == 'order by "column1", "column2"'
            assert (
                _build_composable_order_by(["column1", "column2"], "desc") == 'order by "column1" desc, "column2" desc'
            )
            assert (
                _build_composable_order_by(["column1", "column2"], sort_nulls="last")
                == 'order by "column1" nulls last, "column2" nulls last'
            )
            assert (
                _build_composable_order_by(["column1", "column2"], "desc", "last")
                == 'order by "column1" desc nulls last, "column2" desc nulls last'
            )
            assert (
                _build_composable_order_by(["column1", "column2"], ["asc", "desc"])
                == 'order by "column1" asc, "column2" desc'
            )
            assert (
                _build_composable_order_by(["column1", "column2"], sort_nulls=["first", "last"])
                == 'order by "column1" nulls first, "column2" nulls last'
            )
            assert (
                _build_composable_order_by(["column1", "column2"], ["asc", "desc"], ["first", "last"])
                == 'order by "column1" asc nulls first, "column2" desc nulls last'
            )

            assert _build_composable_order_by(None) == ""
            assert _build_composable_order_by("") == ""
            assert _build_composable_order_by([]) == ""

            with pytest.raises(ValueError):
                _build_composable_order_by([1, 2, 3])

            with pytest.raises(ValueError):
                _build_composable_order_by(["column1", "column2"], "NOPE")

            with pytest.raises(ValueError):
                _build_composable_order_by(["column1", "column2"], sort_nulls="NOPE")

            with pytest.raises(ValueError):
                _build_composable_order_by(["column1", "column2"], ["asc", "NOPE"])

            with pytest.raises(ValueError):
                _build_composable_order_by(["column1", "column2"], sort_nulls=["first", "NOPE"])

            with pytest.raises(ValueError):
                _build_composable_order_by(["column1", "column2"], ["asc", "asc", "asc"])

            with pytest.raises(ValueError):
                _build_composable_order_by(["column1", "column2"], sort_nulls=["first", "first", "first"])

        _sql = SQL("select id, latest_transaction_id from vw_awards a ") + SQL(
            _build_composable_order_by(["a.id", "a.latest_transaction_id"], ["desc", "asc"], ["first", "last"])
        )
        assert execute_sql_to_ordered_dictionary(_sql) == [
            OrderedDict((("id", 5), ("latest_transaction_id", 5))),
            OrderedDict((("id", 4), ("latest_transaction_id", 4))),
            OrderedDict((("id", 3), ("latest_transaction_id", 3))),
            OrderedDict((("id", 2), ("latest_transaction_id", 2))),
            OrderedDict((("id", 1), ("latest_transaction_id", 1))),
        ]

    @staticmethod
    def test_execute_sql_to_ordered_dictionary():
        assert execute_sql_to_ordered_dictionary(RAW_SQL) == EXPECTED_RESPONSE_ORDERED_DICTIONARY
        assert execute_sql_to_ordered_dictionary(SQL(RAW_SQL)) == EXPECTED_RESPONSE_ORDERED_DICTIONARY
        assert execute_sql_to_ordered_dictionary(RAW_SQL, User) == EXPECTED_RESPONSE_ORDERED_DICTIONARY

    @staticmethod
    def test_fetchall_to_ordered_dictionary():
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(RAW_SQL)
            result = ordered_dictionary_fetcher(cursor)
        assert result == EXPECTED_RESPONSE_ORDERED_DICTIONARY

    @staticmethod
    def test_get_connection():
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(RAW_SQL)
            result = ordered_dictionary_fetcher(cursor)
        assert result == EXPECTED_RESPONSE_ORDERED_DICTIONARY

        connection = get_connection(User, True)
        with connection.cursor() as cursor:
            cursor.execute(RAW_SQL)
            result = ordered_dictionary_fetcher(cursor)
        assert result == EXPECTED_RESPONSE_ORDERED_DICTIONARY

        connection = get_connection(read_only=False)
        with connection.cursor() as cursor:
            cursor.execute(RAW_SQL)
            result = ordered_dictionary_fetcher(cursor)
        assert result == EXPECTED_RESPONSE_ORDERED_DICTIONARY
