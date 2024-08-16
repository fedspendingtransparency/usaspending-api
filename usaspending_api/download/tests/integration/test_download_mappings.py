import pytest
from usaspending_api.common.helpers.unit_test_helper import mappings_test


@pytest.mark.django_db
def elasticsearch_transactionsaward_mappings():
    """Ensure the elasticsearch awards column-level mappings retrieve data from valid DB columns."""
    assert mappings_test("elasticsearch_awards", "d1")
    assert mappings_test("elasticsearch_awards", "d2")


@pytest.mark.django_db
def test_transactions_mappings():
    """Ensure the transaction column-level mappings retrieve data from valid DB columns."""
    assert mappings_test("transactions", "d1")
    assert mappings_test("transactions", "d2")


@pytest.mark.django_db
def test_elasticsearch_transactions_mappings():
    """Ensure the elasticsearch transaction column-level mappings retrieve data from valid DB columns."""
    assert mappings_test("elasticsearch_transactions", "d1")
    assert mappings_test("elasticsearch_transactions", "d2")


@pytest.mark.django_db
def test_subawards_mappings():
    """Ensure the subaward column-level mappings retrieve data from valid DB columns."""
    assert mappings_test("elasticsearch_sub_awards", "d1")
    assert mappings_test("elasticsearch_sub_awards", "d2")
