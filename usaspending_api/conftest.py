# Stdlib imports
import logging

# Core Django imports
from django.conf import settings

# Third-party app imports
import pytest
from django_mock_queries.query import MockSet

# Imports from your apps
from usaspending_api.etl.broker_etl_helpers import PhonyCursor


logger = logging.getLogger('console')
VALID_DB_CURSORS = ['default', 'data_broker']


@pytest.fixture()
def mock_db_cursor(monkeypatch, request):
    db_cursor_dict = request.param

    for cursor in VALID_DB_CURSORS:
        if cursor in db_cursor_dict:
            data_file_key = cursor + '_data_file'

            if data_file_key not in db_cursor_dict:
                # Missing data file for the mocked cursor. Skipping PhonyCursor setup.
                continue

            db_cursor_dict[cursor].cursor.return_value = PhonyCursor(db_cursor_dict[data_file_key])
            del db_cursor_dict[data_file_key]

    monkeypatch.setattr('django.db.connections', db_cursor_dict)


@pytest.fixture()
def mock_agencies(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_agency_qs = MockSet()
    mock_toptier_agency_qs = MockSet()
    mock_subtier_agency_qs = MockSet()

    monkeypatch.setattr('usaspending_api.references.models.Agency.objects', mock_agency_qs)
    monkeypatch.setattr('usaspending_api.references.models.ToptierAgency.objects', mock_toptier_agency_qs)
    monkeypatch.setattr('usaspending_api.references.models.SubtierAgency.objects', mock_subtier_agency_qs)

    mocked_agencies = {
        'agency': mock_agency_qs,
        'toptier_agency': mock_toptier_agency_qs,
        'subtier_agency': mock_subtier_agency_qs
    }

    yield mocked_agencies

    mock_agency_qs.delete()
    mock_toptier_agency_qs.delete()
    mock_subtier_agency_qs.delete()


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
