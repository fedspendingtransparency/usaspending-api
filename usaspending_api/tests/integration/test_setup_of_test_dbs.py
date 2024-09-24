"""
This module should contain only the one test.
It is to be called by name as the singular test in a pytest session, and used to trigger the setup of the test
database(s) for the NEXT run of pytest. Both invocations should use the --reuse-db option to leave behind and pick
back up the test datbase(s).
It can conveniently and efficiently pre-establish MULTIPLE test databases using pytest-xdist for parallel test sessions
run by multiple workers. For this case, both invocations of pytest should use the same pytest-xdist value for the
number of workers (the value of -n or --numprocesses)
"""

from pytest import mark

TEST_DB_SETUP_TEST_NAME = "test_trigger_test_db_setup"  # MUST match the name of the 1 test in this module


@mark.database
def test_trigger_test_db_setup(request, django_db_setup):
    """See module docstring above.

    Run this test alone, like:

        pytest --reuse-db --no-cov --disable-warnings -rP -vv 'usaspending_api/tests/integration/test_setup_of_test_dbs.py::test_trigger_test_db_setup'

    If running in a Continuous Integration pipeline (or if you want to nuke and pave your own local test DBs,
    add --create-db to ensure that the test DBs are (re)created from scratch
    """
    assert request.node.originalname == TEST_DB_SETUP_TEST_NAME
