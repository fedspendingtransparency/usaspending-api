import pytest

from django.db import connection
from time import perf_counter

from usaspending_api.common.helpers.decorators import set_db_timeout


@pytest.mark.django_db
def test_statement_timeout_successfully_times_out():
    """
    Test the django statement timeout setting
    """

    test_timeout_in_seconds = 0.5
    pg_sleep_in_seconds = 10

    @set_db_timeout(test_timeout_in_seconds)
    def test_timeout_success():
        with connection.cursor() as cursor:
            # pg_sleep takes in a parameter corresponding to seconds
            cursor.execute(f"SELECT pg_sleep({pg_sleep_in_seconds:.2f})")

    start = perf_counter()
    try:
        test_timeout_success()
    except Exception:
        # Can't test for the endpoint timeout error type here since the cursor.execute raises an internal error when it
        # times out
        assert (perf_counter() - start) < pg_sleep_in_seconds
    else:
        assert False


@pytest.mark.django_db
def test_statement_timeout_successfully_runs_within_timeout():
    """
    Test the django statement timeout setting
    """

    test_timeout_in_seconds = 1
    pg_sleep_in_seconds = 0.5

    @set_db_timeout(test_timeout_in_seconds)
    def test_timeout_success():
        with connection.cursor() as cursor:
            # pg_sleep takes in a parameter corresponding to seconds
            cursor.execute(f"SELECT pg_sleep({pg_sleep_in_seconds:.2f})")

    try:
        start = perf_counter()
        test_timeout_success()
    except Exception:
        assert False
    else:
        assert (perf_counter() - start) >= pg_sleep_in_seconds


@pytest.mark.django_db
def test_statement_timeout_no_decorator():
    """Test the django statement timeout setting"""

    start = perf_counter()
    pg_sleep_in_seconds = 2
    tiny_offset = 0.001  # allows for slight rounding or timing differences between tools

    def test_timeout_success():
        with connection.cursor() as cursor:
            # pg_sleep takes in a parameter corresponding to seconds
            cursor.execute(f"SELECT pg_sleep({pg_sleep_in_seconds:.2f})")

    try:
        test_timeout_success()
    except Exception:
        assert False
    else:
        assert (perf_counter() - start) >= (pg_sleep_in_seconds - tiny_offset)
