import pytest
from usaspending_api.common.helpers.unit_test_helper import mappings_test


@pytest.mark.django_db
def test_award_mappings(refresh_matviews):
    """ Ensure the awards column-level mappings retrieve data from valid DB columns. """
    mappings_test('awards', 'd1')
    mappings_test('awards', 'd2')


@pytest.mark.django_db
def test_transactions_mappings(refresh_matviews):
    """ Ensure the transaction column-level mappings retrieve data from valid DB columns. """
    mappings_test('transactions', 'd1')
    mappings_test('transactions', 'd2')


@pytest.mark.django_db
def test_subawards_mappings(refresh_matviews):
    """ Ensure the subaward column-level mappings retrieve data from valid DB columns. """
    mappings_test('sub_awards', 'd1')
    mappings_test('sub_awards', 'd2')
