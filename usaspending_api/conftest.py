# Stdlib imports
import logging


# Core Django imports
from django.conf import settings

# Third-party app imports
import pytest
from django_mock_queries.query import MockSet

# Imports from your apps
from usaspending_api.common.helpers.generic_helper import generate_matviews
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
def mock_psc(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_psc_qs = MockSet()

    monkeypatch.setattr('usaspending_api.references.models.PSC.objects', mock_psc_qs)

    yield mock_psc_qs

    mock_psc_qs.delete()


@pytest.fixture()
def mock_cfda(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_cfda_qs = MockSet()

    monkeypatch.setattr('usaspending_api.references.models.Cfda.objects', mock_cfda_qs)

    yield mock_cfda_qs

    mock_cfda_qs.delete()


@pytest.fixture()
def mock_recipients(monkeypatch):
    """Mocks all agency querysets into a single mock"""
    mock_recipient_qs = MockSet()

    monkeypatch.setattr('usaspending_api.references.models.LegalEntity.objects', mock_recipient_qs)

    yield mock_recipient_qs

    mock_recipient_qs.delete()


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
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SubawardView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryAwardView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryCfdaNumbersView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryNaicsCodesView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryPscCodesView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryStateView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryTransactionGeoView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryTransactionMonthView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryTransactionRecipientView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryTransactionView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.SummaryView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.UniversalAwardView.objects', mock_qs)
    monkeypatch.setattr('usaspending_api.awards.models_matviews.UniversalTransactionView.objects', mock_qs)

    yield mock_qs

    mock_qs.delete()


@pytest.fixture()
def mock_reference_matviews(monkeypatch):
    mock_qs = MockSet()
    monkeypatch.setattr('usaspending_api.references.models.RecipientLookup.objects', mock_qs)

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


def pytest_addoption(parser):
    parser.addoption("--local", action="store", default="true")


@pytest.fixture(scope="session")
def local(request):
    return request.config.getoption("--local")


@pytest.fixture(scope='session')
def django_db_setup(django_db_blocker,
                    django_db_keepdb,
                    local,
                    request):

    from pytest_django.compat import setup_databases, teardown_databases

    setup_databases_args = {}

    with django_db_blocker.unblock():
        db_cfg = setup_databases(
            verbosity=pytest.config.option.verbose,
            interactive=False,
            **setup_databases_args
        )

    def teardown_database():
        with django_db_blocker.unblock():
            teardown_databases(
                db_cfg,
                verbosity=pytest.config.option.verbose,
            )

    if not django_db_keepdb:
        request.addfinalizer(teardown_database)


@pytest.fixture()
def refresh_matviews():
    generate_matviews()
