# Stdlib imports

# Core Django imports
from django.conf import settings

# Third-party app imports
import pytest
from django_mock_queries.query import MockSet

# Imports from your apps


@pytest.fixture()
def mock_matviews_qs(monkeypatch):
    """Mocks all matvies to a single mock queryset"""
    mock_qs = MockSet()  # mock queryset
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryPscCodesView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryNaicsCodesView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryCfdaNumbersView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryTransactionMonthView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryTransactionView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.UniversalTransactionView.objects', mock_qs)

    yield mock_qs

    mock_qs.delete()


def pytest_configure():
    # To make sure the test setup process doesn't try
    # to set up another test db, remove everything but the default
    # DATABASE_URL from the list of databases in django settings
    test_db = settings.DATABASES.pop('default', None)
    settings.DATABASES.clear()
    settings.DATABASES['default'] = test_db
    # Also remove any database routers
    settings.DATABASE_ROUTERS.clear()
