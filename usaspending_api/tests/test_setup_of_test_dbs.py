"""
This module should contain only the one test.
It is to be called by name as the singular test in a pytest session, and used to trigger the  setup of MULTIPLE test
databases for the NEXT pytest pytest-xdist session with parallel test workers.
That next session can use the --reuse-db option along with -n=auto or --numprocesses=auto to utilize the prepared
test databases.
"""
from pytest import skip, fixture

TEST_DB_SETUP_TEST_NAME = "test_trigger_test_db_setup"  # MUST match the name of the 1 test in this module


@fixture(scope="function")
def _skip_if_xdist(request):
    # If this is running in a session with multiple xdist test workers, skip it
    # It's meant to be run solo simply to trigger setup of multiple test DBs
    if request.config.option.numprocesses is not None:
        skip("Test not intended to be run in an pytest-xdist session (i.e. with -n or --numprocesses")


def test_trigger_test_db_setup(request, _skip_if_xdist, django_db_setup):
    """See module docstring above.

    Run this test alone, like:

        pytest --reuse-db --no-cov --disable-warnings -rP -vv 'usaspending_api/tests/test_setup_of_test_dbs.py::test_trigger_test_db_setup'

    If running in a Continuous Integration pipeline (or if you want to nuke and pave your own local test DBs,
    add --create-db to ensure that the test DBs are (re)created from scratch
    """
    assert request.node.originalname == TEST_DB_SETUP_TEST_NAME


