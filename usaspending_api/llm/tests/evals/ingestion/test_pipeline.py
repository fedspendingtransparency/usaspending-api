from pathlib import Path

from usaspending_api.llm.evals.ingestion.pipeline import ingest_ground_truth
from usaspending_api.llm.evals.ingestion.providers import LocalFileProvider

from .test_excel import workbook_bytes


def test_ingest_ground_truth_writes_transformed_json(tmp_path: Path):
    source_path = tmp_path / "ground_truth.xlsx"
    output_path = tmp_path / "generated" / "ground_truth.json"
    source_path.write_bytes(workbook_bytes())

    result = ingest_ground_truth(
        provider=LocalFileProvider(),
        source_reference=str(source_path),
        output_path=output_path,
    )

    assert result.output_path == output_path
    assert result.case_count == 1
    assert result.mapping_count == 3
    assert output_path.is_file()
