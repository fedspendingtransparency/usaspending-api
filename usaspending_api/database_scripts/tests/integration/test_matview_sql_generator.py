import pytest

from django.db import connection
from usaspending_api.common.helpers.generic_helper import generate_matviews
from usaspending_api.common.matview_manager import MATERIALIZED_VIEWS, CHUNKED_MATERIALIZED_VIEWS

ALL_MATVIEWS = {**MATERIALIZED_VIEWS, **CHUNKED_MATERIALIZED_VIEWS}


@pytest.fixture
def convert_traditional_views_to_materialized_views(transactional_db):
    """
    In conftest.py, we convert materialized views to traditional views for testing performance reasons.
    For the following test to work, we will need to undo all the traditional view stuff and generate
    actual materialized views.  Once done, we want to convert everything back to traditional views.
    """

    # Get rid of the traditional views that replace our materialized views for tests.
    with connection.cursor() as cursor:
        cursor.execute("; ".join(f"drop view if exists {v} cascade" for v in ALL_MATVIEWS))

    # Create materialized views.
    generate_matviews(materialized_views_as_traditional_views=False)

    yield

    # Great.  Test is over.  Drop all of our materialized views.
    with connection.cursor() as cursor:
        cursor.execute("; ".join(f"drop materialized view if exists {v} cascade" for v in ALL_MATVIEWS))

    # Recreate our traditional views.
    generate_matviews(materialized_views_as_traditional_views=True)


@pytest.mark.django_db
def test_matview_sql_generator(convert_traditional_views_to_materialized_views):
    """
    The goal of this test is to ensure that we can successfully create materialized views using our homegrown
    materialized view generator.  We will not test the validity of the data contained therein, just that we
    can create the materialized views without issue.
    """

    # Run through all of the materialized views and perform a simple count query.
    for matview in MATERIALIZED_VIEWS.values():
        # This will fail if the materialized view doesn't exist for whatever reason which is what we want.
        matview["model"].objects.count()
