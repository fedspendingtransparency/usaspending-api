import pytest
from django.conf import settings

from usaspending_api.download.v2.list_database_downloads import ListDatabaseDownloadsViewSet


@pytest.fixture
def mock_settings(monkeypatch):
    # Mock a single property in the settings module
    monkeypatch.setattr("django.conf.settings.FILES_SERVER_BASE_URL", "http://files.usaspending.gov")


def test_structure_file_response_with_file_name(mock_settings):
    file_name = "test_file.zip"

    view = ListDatabaseDownloadsViewSet()
    response = view._structure_file_response(file_name)

    assert response == {
        "file_name": file_name,
        "url": f"{settings.FILES_SERVER_BASE_URL}/{view.redirect_dir}/{file_name}",
    }


def test_structure_file_response_no_file_name(mock_settings):
    file_name = None

    view = ListDatabaseDownloadsViewSet()
    response = view._structure_file_response(file_name)

    assert response == {
        "file_name": None,
        "url": None,
    }
