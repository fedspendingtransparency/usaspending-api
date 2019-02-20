import pytest

from collections import OrderedDict, namedtuple

from django.contrib.auth.models import User  # A table that should always exist.  Doesn't really matter what it is.
from django.db.utils import InterfaceError
from django.test import TestCase

from model_mommy import mommy
from psycopg2.sql import SQL

from usaspending_api.common.helpers.sql_helpers import build_order_by, convert_composable_to_string, \
    fetchall_to_ordered_dictionary, fetchall_to_named_tuple, get_connection, open_cursor, execute_sql, \
    fetch_all_as_ordered_dictionary, fetch_all_as_named_tuple


AWARD_COUNT = 5

RAW_SQL = """
    select
        a.id               award_id,
        tn.id              transaction_normalized_id,
        tf.transaction_id  transaction_fpds_id
    from
        awards a
        inner join transaction_normalized tn on tn.award_id = a.id
        inner join transaction_fpds tf on tf.transaction_id = tn.id
    order by
        a.id
"""

EXPECTED_RESPONSE_ORDERED_DICTIONARY = [
    OrderedDict((('award_id', 1), ('transaction_normalized_id', 1), ('transaction_fpds_id', 1))),
    OrderedDict((('award_id', 2), ('transaction_normalized_id', 2), ('transaction_fpds_id', 2))),
    OrderedDict((('award_id', 3), ('transaction_normalized_id', 3), ('transaction_fpds_id', 3))),
    OrderedDict((('award_id', 4), ('transaction_normalized_id', 4), ('transaction_fpds_id', 4))),
    OrderedDict((('award_id', 5), ('transaction_normalized_id', 5), ('transaction_fpds_id', 5))),
]

Result = namedtuple('Result', ['award_id', 'transaction_normalized_id', 'transaction_fpds_id'])
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
            mommy.make(
                'awards.TransactionNormalized',
                id=_id,
                award_id=_id
            )
            mommy.make(
                'awards.TransactionFPDS',
                transaction_id=_id
            )
            mommy.make(
                'awards.Award',
                id=_id,
                latest_transaction_id=_id
            )

    @staticmethod
    def test_build_order_by():
        assert build_order_by('column') == 'order by "column"'
        assert build_order_by('this.column') == 'order by "this"."column"'
        assert build_order_by('column', 'asc') == 'order by "column" asc'
        assert build_order_by('column', nulls='first') == 'order by "column" nulls first'
        assert build_order_by('column', 'asc', 'first') == 'order by "column" asc nulls first'

        assert (
            build_order_by(['column1', 'column2']) ==
            'order by "column1", "column2"'
        )
        assert (
            build_order_by(['column1', 'column2'], 'desc') ==
            'order by "column1" desc, "column2" desc'
        )
        assert (
            build_order_by(['column1', 'column2'], nulls='last') ==
            'order by "column1" nulls last, "column2" nulls last'
        )
        assert (
            build_order_by(['column1', 'column2'], 'desc', 'last') ==
            'order by "column1" desc nulls last, "column2" desc nulls last'
        )
        assert (
            build_order_by(['column1', 'column2'], ['asc', 'desc']) ==
            'order by "column1" asc, "column2" desc'
        )
        assert (
            build_order_by(['column1', 'column2'], nulls=['first', 'last']) ==
            'order by "column1" nulls first, "column2" nulls last'
        )
        assert (
            build_order_by(['column1', 'column2'], ['asc', 'desc'], ['first', 'last']) ==
            'order by "column1" asc nulls first, "column2" desc nulls last'
        )

        assert build_order_by('column', model=User, read_only=False) == 'order by "column"'

        with pytest.raises(ValueError):
            build_order_by([1, 2, 3])

        with pytest.raises(ValueError):
            build_order_by(['column1', 'column2'], 'NOPE')

        with pytest.raises(ValueError):
            build_order_by(['column1', 'column2'], nulls='NOPE')

        with pytest.raises(ValueError):
            build_order_by(['column1', 'column2'], ['asc', 'NOPE'])

        with pytest.raises(ValueError):
            build_order_by(['column1', 'column2'], nulls=['first', 'NOPE'])

        with pytest.raises(ValueError):
            build_order_by(['column1', 'column2'], ['asc', 'asc', 'asc'])

        with pytest.raises(ValueError):
            build_order_by(['column1', 'column2'], nulls=['first', 'first', 'first'])

        _sql = (
            SQL('select id, latest_transaction_id from awards a ') +
            SQL(build_order_by(['a.id', 'a.latest_transaction_id'], ['desc', 'asc'], ['first', 'last']))
        )
        assert fetch_all_as_ordered_dictionary(_sql) == [
            OrderedDict((('id', 5), ('latest_transaction_id', 5))),
            OrderedDict((('id', 4), ('latest_transaction_id', 4))),
            OrderedDict((('id', 3), ('latest_transaction_id', 3))),
            OrderedDict((('id', 2), ('latest_transaction_id', 2))),
            OrderedDict((('id', 1), ('latest_transaction_id', 1))),
        ]

    @staticmethod
    def test_convert_composable_to_string():
        assert convert_composable_to_string(RAW_SQL) == RAW_SQL
        assert convert_composable_to_string(SQL('select ') + SQL('something')) == 'select something'
        assert convert_composable_to_string(SQL(RAW_SQL)) == RAW_SQL
        connection = get_connection()
        assert convert_composable_to_string(SQL(RAW_SQL), connection) == RAW_SQL
        with open_cursor() as cursor:
            assert convert_composable_to_string(SQL(RAW_SQL), cursor=cursor) == RAW_SQL
        assert convert_composable_to_string(SQL(RAW_SQL), model=User, read_only=True) == RAW_SQL
        assert convert_composable_to_string(SQL(RAW_SQL), read_only=False) == RAW_SQL

    @staticmethod
    def test_fetchall_to_ordered_dictionary():
        with execute_sql(RAW_SQL, close_cursor=False) as cursor:
            result = fetchall_to_ordered_dictionary(cursor)
        assert result == EXPECTED_RESPONSE_ORDERED_DICTIONARY

    @staticmethod
    def test_fetchall_to_named_tuple():
        with execute_sql(RAW_SQL, close_cursor=False) as cursor:
            result = fetchall_to_named_tuple(cursor)
        assert result == EXPECTED_RESPONSE_NAMED_TUPLE

    @staticmethod
    def test_get_connection():
        connection = get_connection()
        with connection.cursor() as cursor:
            cursor.execute(RAW_SQL)
            result = fetchall_to_ordered_dictionary(cursor)
        assert result == EXPECTED_RESPONSE_ORDERED_DICTIONARY

        connection = get_connection(User, True)
        with connection.cursor() as cursor:
            cursor.execute(RAW_SQL)
            result = fetchall_to_ordered_dictionary(cursor)
        assert result == EXPECTED_RESPONSE_ORDERED_DICTIONARY

        connection = get_connection(read_only=False)
        with connection.cursor() as cursor:
            cursor.execute(RAW_SQL)
            result = fetchall_to_ordered_dictionary(cursor)
        assert result == EXPECTED_RESPONSE_ORDERED_DICTIONARY

    @staticmethod
    def test_open_cursor():
        with open_cursor() as cursor:
            cursor.execute(RAW_SQL)
            result = fetchall_to_ordered_dictionary(cursor)
        assert result == EXPECTED_RESPONSE_ORDERED_DICTIONARY

        with open_cursor(User, True) as cursor:
            cursor.execute(RAW_SQL)
            result = fetchall_to_ordered_dictionary(cursor)
        assert result == EXPECTED_RESPONSE_ORDERED_DICTIONARY

        with open_cursor(read_only=False) as cursor:
            cursor.execute(RAW_SQL)
            result = fetchall_to_ordered_dictionary(cursor)
        assert result == EXPECTED_RESPONSE_ORDERED_DICTIONARY

    @staticmethod
    def test_execute_sql():
        cursor = execute_sql(RAW_SQL)
        assert cursor.closed is True

        cursor = execute_sql(RAW_SQL, User, True, True)
        assert cursor.closed is True

        with pytest.raises(InterfaceError):
            with execute_sql(RAW_SQL) as cursor:
                fetchall_to_ordered_dictionary(cursor)

        with execute_sql(RAW_SQL, User, False, False) as cursor:
            assert cursor.closed is False
            result = fetchall_to_ordered_dictionary(cursor)
            assert result == EXPECTED_RESPONSE_ORDERED_DICTIONARY

        with execute_sql(SQL(RAW_SQL), User, False, False) as cursor:
            assert cursor.closed is False
            result = fetchall_to_ordered_dictionary(cursor)
            assert result == EXPECTED_RESPONSE_ORDERED_DICTIONARY

        with execute_sql('select count(*) from awards', User, False, False) as cursor:
            assert cursor.fetchall()[0][0] == AWARD_COUNT

        mommy.make(
            'awards.Award',
            id=AWARD_COUNT + 1
        )

        with execute_sql('select count(*) from awards', User, False, False) as cursor:
            assert cursor.fetchall()[0][0] == AWARD_COUNT + 1

        execute_sql('delete from awards where id = %s' % str(AWARD_COUNT + 1), read_only=False)

        with execute_sql('select count(*) from awards', User, False, False) as cursor:
            assert cursor.fetchall()[0][0] == AWARD_COUNT

    @staticmethod
    def test_fetch_all_as_ordered_dictionary():
        assert fetch_all_as_ordered_dictionary(RAW_SQL) == EXPECTED_RESPONSE_ORDERED_DICTIONARY
        assert fetch_all_as_ordered_dictionary(SQL(RAW_SQL)) == EXPECTED_RESPONSE_ORDERED_DICTIONARY
        assert fetch_all_as_ordered_dictionary(RAW_SQL, User) == EXPECTED_RESPONSE_ORDERED_DICTIONARY

    @staticmethod
    def test_fetch_all_as_named_tuple():
        assert fetch_all_as_named_tuple(RAW_SQL) == EXPECTED_RESPONSE_NAMED_TUPLE
        assert fetch_all_as_named_tuple(SQL(RAW_SQL)) == EXPECTED_RESPONSE_NAMED_TUPLE
        assert fetch_all_as_named_tuple(RAW_SQL, User) == EXPECTED_RESPONSE_NAMED_TUPLE
