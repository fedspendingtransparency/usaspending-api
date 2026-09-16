import json

from usaspending_api.llm.evals.ingestion.writer import write_ground_truth_json


def test_write_ground_truth_json_writes_cases_atomically(tmp_path):
    output_path = tmp_path / "data" / "ground_truth.json"
    cases = [
        {
            "id": 1,
            "query": "Example query",
            "expected_output": {"timePeriodType": "fy"},
            "expected_tools": ["execute_filter"],
            "tags": [],
            "notes": "",
            "approved": True,
            "sme_validation_notes": "",
        }
    ]

    write_ground_truth_json(cases, output_path)

    assert json.loads(output_path.read_text(encoding="utf-8")) == cases
    assert list(output_path.parent.glob("*.tmp")) == []
