from pathlib import Path

import pytest
import requests

from usaspending_api.llm.evals.exceptions import DatasetError
from usaspending_api.llm.evals.ingestion.providers import (
    HttpFileProvider,
    LocalFileProvider,
)


def test_local_file_provider_reads_workbook(tmp_path: Path):
    workbook_path = tmp_path / "ground_truth.xlsx"
    workbook_path.write_bytes(b"workbook")

    result = LocalFileProvider().fetch(str(workbook_path))

    assert result.content == b"workbook"
    assert result.source_name == str(workbook_path)
    assert result.source_reference == str(workbook_path)


def test_local_file_provider_rejects_missing_file(tmp_path: Path):
    with pytest.raises(DatasetError, match="does not exist"):
        LocalFileProvider().fetch(str(tmp_path / "missing.xlsx"))


def test_http_provider_requires_https():
    with pytest.raises(DatasetError, match="must use HTTPS"):
        HttpFileProvider().fetch("http://example.test/ground_truth.xlsx")


def test_http_provider_returns_response_content(monkeypatch):
    response = requests.Response()
    response.status_code = 200
    response._content = b"workbook"
    response.headers["ETag"] = "version-1"

    monkeypatch.setattr(requests, "get", lambda *args, **kwargs: response)

    result = HttpFileProvider().fetch("https://example.test/ground_truth.xlsx")

    assert result.content == b"workbook"
    assert result.source_version == "version-1"


def test_http_provider_wraps_download_errors(monkeypatch):
    def fail(*args, **kwargs):
        raise requests.RequestException("network failure")

    monkeypatch.setattr(requests, "get", fail)

    with pytest.raises(DatasetError, match="Unable to download"):
        HttpFileProvider().fetch("https://example.test/ground_truth.xlsx")
