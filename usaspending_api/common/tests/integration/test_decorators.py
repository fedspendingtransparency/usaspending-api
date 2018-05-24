# Stdlib imports
import timeit

# Core Django imports
from django.db import connection
from django.db.utils import OperationalError

# Third-party app imports
import pytest

# Imports from your apps
from usaspending_api.common.helpers.decorators import set_db_timeout


@pytest.mark.django_db
def test_statement_timeout_successfully_times_out():
    """
    Test the django statement timeout setting
    """

    test_timeout_in_ms = 1000
    pg_sleep_in_s = 10

    @set_db_timeout(test_timeout_in_ms)
    def test_timeout_success():
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_sleep(%d)" % pg_sleep_in_s)  # pg_sleep takes in a parameter corresponding to seconds

    start = timeit.default_timer()
    try:
        test_timeout_success()
    except OperationalError:
        assert (timeit.default_timer() - start) < pg_sleep_in_s
    else:
        assert False


@pytest.mark.django_db
def test_statement_timeout_successfully_runs_within_timeout():
    """
    Test the django statement timeout setting
    """

    test_timeout_in_ms = 2000
    pg_sleep_in_s = 1

    @set_db_timeout(test_timeout_in_ms)
    def test_timeout_success():
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_sleep(%d)" % pg_sleep_in_s)  # pg_sleep takes in a parameter corresponding to seconds

    # noinspection PyBroadException
    try:
        start = timeit.default_timer()
        test_timeout_success()
    except:
        assert False
    else:
        assert (timeit.default_timer() - start) >= pg_sleep_in_s
