from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True)
class GroundTruthSource:
    """A retrieved source workbook and its metadata."""

    content: bytes
    source_name: str
    source_reference: str
    source_version: str | None = None


@dataclass(frozen=True)
class FilterMapping:
    """One mapping from workbook naming to backend naming."""

    mapping_type: str
    filter_name: str
    subfilter: str
    source_name: str
    target_name: str


@dataclass(frozen=True)
class WorkbookData:
    """Validated rows and mappings loaded from the workbook."""

    ground_truth_rows: tuple[dict[str, Any], ...]
    filter_mappings: tuple[FilterMapping, ...]
    source: GroundTruthSource


@dataclass(frozen=True)
class IngestionResult:
    """Summary of one successful workbook ingestion."""

    output_path: Path
    case_count: int
    mapping_count: int
    source_name: str
    source_version: str | None = None


class GroundTruthProvider(Protocol):
    """Provider contract for local files, HTTP, S3, SharePoint, or GitLab."""

    def fetch(self, reference: str) -> GroundTruthSource:
        """Retrieve one workbook without transforming it."""
