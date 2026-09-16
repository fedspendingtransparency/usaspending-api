from pathlib import Path

import requests

from usaspending_api.llm.evals.exceptions import DatasetError
from usaspending_api.llm.evals.ingestion.models import GroundTruthSource


class LocalFileProvider:
    """Reads an Excel workbook from a local path for development and CI."""

    def fetch(self, reference: str) -> GroundTruthSource:
        path = Path(reference)

        if not path.is_file():
            raise DatasetError(f"Ground-truth workbook does not exist: {path}")

        try:
            content = path.read_bytes()
        except OSError as error:
            raise DatasetError(f"Unable to read ground-truth workbook '{path}': {error}") from error

        return GroundTruthSource(
            content=content,
            source_name=str(path),
            source_reference=str(path),
        )


class HttpFileProvider:
    """Downloads an Excel workbook from a configured HTTPS endpoint."""

    def __init__(self, timeout: float = 30.0) -> None:
        self.timeout = timeout

    def fetch(self, reference: str) -> GroundTruthSource:
        if not reference.lower().startswith("https://"):
            raise DatasetError("HTTP ground-truth references must use HTTPS.")

        try:
            response = requests.get(reference, timeout=self.timeout)
            response.raise_for_status()
        except requests.RequestException as exc:
            raise DatasetError(f"Unable to download ground-truth workbook: {exc}") from exc

        return GroundTruthSource(
            content=response.content,
            source_name=reference,
            source_reference=reference,
            source_version=response.headers.get("ETag"),
        )
