from pathlib import Path

from usaspending_api.llm.evals.ingestion.excel import read_workbook
from usaspending_api.llm.evals.ingestion.models import GroundTruthProvider, IngestionResult
from usaspending_api.llm.evals.ingestion.transform import transform_workbook
from usaspending_api.llm.evals.ingestion.writer import write_ground_truth_json


def ingest_ground_truth(provider: GroundTruthProvider, source_reference: str, output_path: Path) -> IngestionResult:
    """Retrieve, validate, transform, and write one workbook to JSON."""
    source = provider.fetch(source_reference)
    workbook = read_workbook(source)
    cases = transform_workbook(workbook)
    write_ground_truth_json(cases, output_path)

    return IngestionResult(
        output_path=output_path,
        case_count=len(cases),
        mapping_count=len(workbook.filter_mappings),
        source_name=source.source_name,
        source_version=source.source_version,
    )
