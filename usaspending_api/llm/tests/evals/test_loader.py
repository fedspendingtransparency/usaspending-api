from pathlib import Path

import pytest

from usaspending_api.llm.evals.exceptions import DatasetError
from usaspending_api.llm.evals.loader import parse_csv_cases


SAMPLE_CASE = """
id,query,expected_output,expected_tools,tags,notes,approved,sme_validation_notes
1,"How much did Clark Construction receive in contracts for FY25?","timePeriodType = fy
timePeriodFy = [""2025""]
selectedRecipients = [""Clark Construction""]", "lookup_recipient
execute_filter","multi_filter
temporal","Approved recipient and fiscal-year case",yes,"SME approved"
"""


def write_dataset(tmp_path: Path, content: str) -> Path:
    """Write a temporary CSV dataset for one test."""
    dataset_path = tmp_path / "filter_search.csv"
    dataset_path.write_text(content, encoding="utf-8")
    return dataset_path


def test_parse_csv_cases_builds_expected_case(tmp_path: Path):
    """
    Verify that a valid CSV row becomes an EvalCase with:
        - query input
        - ordered expected tools
        - nested expected output
        - preserved metadata
    """
    dataset_path = write_dataset(tmp_path, SAMPLE_CASE)

    cases = parse_csv_cases(dataset_path)

    assert len(cases) == 1

    case = cases[0]

    assert case.name == "1"
    assert case.input == {
        "query": "How much did Clark Construction receive in contracts for FY25?",
    }
    assert [tool.name for tool in case.expected_tool_calls] == [
        "lookup_recipient",
        "execute_filter",
    ]
    assert case.expected_output == {
        "timePeriodType": "fy",
        "timePeriodFY": ["2025"],
        "selectedRecipients": ["CLARK CONSTRUCTION"],
    }
    assert case.metadata == {
        "approved": True,
        "tags": ["multi_filter", "temporal"],
        "notes": "Approved recipient and fiscal-year case",
        "sme_validation_notes": "SME approved",
    }


def test_parse_csv_cases_supports_multiple_dotted_keys(tmp_path: Path):
    """Multiple dotted keys sharing a parent should be combined rather than overwriting one another."""
    dataset_path = write_dataset(tmp_path, SAMPLE_CASE)

    cases = parse_csv_cases(dataset_path)

    assert cases[0].expected_output == {
        "time_period": {
            "start_date": "2024-10-01",
            "end_date": "2025-09-30",
        },
        "filters": {
            "award_type": "Contracts",
            "recipient": "Clark Construction",
        },
    }


def test_parse_csv_cases_rejects_duplicate_dotted_keys(tmp_path: Path):
    """
    Duplicate expected-output paths should fail instead of silently
    allowing a later CSV line to replace the first value.
    """
    dataset_path = write_dataset(
        tmp_path,
        """
        id,query,expected_output,expected_tools,tags,notes,approved,sme_validation_notes
        1,"Duplicate case","timePeriodType = fy
        timePeriodType = dr","execute_filter","","",yes,""
        """,
    )

    with pytest.raises(DatasetError, match="defines expected-output key 'timePeriodType' more than once"):
        parse_csv_cases(dataset_path)


def test_parse_csv_cases_rejects_conflicting_parent_path(tmp_path: Path):
    """A scalar parent cannot also be used as an object parent."""
    dataset_path = write_dataset(
        tmp_path,
        """
        id,query,expected_output,expected_tools,tags,notes,approved,sme_validation_notes
        1,"Conflicting case","time_period = fy
        time_period.fiscal_year = 2025","execute_filter","","",yes,""
        """
    )

    with pytest.raises(DatasetError, match="conflicting output path"):
        parse_csv_cases(dataset_path)


def test_parse_csv_cases_excludes_unapproved_cases_by_default(tmp_path: Path):
    """Draft cases stay in the source CSV but do not run."""
    dataset_path = write_dataset(
        tmp_path,
        """
        id,query,expected_output,expected_tools,tags,notes,approved,sme_validation_notes
        1,"Approved query","award_id = N0001917C0001","execute_filter","award_id","",yes,""
        2,"Draft query","award_id = N0001917C0002","execute_filter","award_id",":",no,""
        """,
    )

    cases = parse_csv_cases(dataset_path)

    assert [case.name for case in cases] == ["1"]


def test_parse_csv_cases_can_include_unapproved_cases(tmp_path: Path):
    """The --include-unapproved behavior is represented by passing include_unapproved=True to the loader."""
    dataset_path = write_dataset(
        tmp_path,
"""
        id,query,expected_output,expected_tools,tags,notes,approved,sme_validation_notes
        1,"Approved query","award_id = N0001917C0001","execute_filter","award_id","",yes,""
        2,"Draft query","award_id = N0001917C0002","execute_filter","award_id",":",no,""
        """,
    )

    cases = parse_csv_cases(dataset_path, include_unapproved=True)

    assert [case.name for case in cases] == ["1", "2"]


def test_parse_csv_cases_filters_by_tag(tmp_path: Path):
    """Only cases containing at least one selected tag are returned."""
    dataset_path = write_dataset(
        tmp_path,
        """
        id,query,expected_output,expected_tools,tags,notes,approved,sme_validation_notes
        1,"Recipient query","selectedRecipients = [""CLARK CONSTRUCTION""],"lookup_recipient
        execute_filter","recipient","",yes,""
        2,"Award query","selectedAwardIDs = {""N0001917C0001"": {}}","execute_filter","award_id","",yes,""
        """,
    )

    cases = parse_csv_cases(dataset_path, tags={"award_id"})

    assert [case.name for case in cases] == ["2"]


def test_parse_csv_cases_rejects_invalid_approved_value(tmp_path: Path):
    """Ambiguous approval values fail before any assistant is executed."""
    dataset_path = write_dataset(
        tmp_path,
        """
        id,query,expected_output,expected_tools,tags,notes,approved,sme_validation_notes
        1,"Invalid approval","award_id = N0001917C0001","execute_filter","","",maybe,""
        """,
    )

    with pytest.raises(DatasetError, match="invalid approved value"):
        parse_csv_cases(dataset_path)


def test_parse_csv_cases_rejects_missing_required_header(tmp_path: Path):
    """A malformed CSV should fail during loading rather than producing incomplete eval cases."""
    dataset_path = tmp_path / "filter_search.csv"
    dataset_path.write_text(
        "id,query,expected_output\n"
        '1,"Query","award_id = N0001917C0001"\n',
        encoding="utf-8",
    )

    with pytest.raises(DatasetError, match="missing required columns"):
        parse_csv_cases(dataset_path)


def test_parse_csv_cases_rejects_duplicate_case_ids(tmp_path: Path):
    """Case IDs are used by --case and must therefore be unique."""
    dataset_path = write_dataset(
        tmp_path,
        """
        id,query,expected_output,expected_tools,tags,notes,approved,sme_validation_notes
        1,"First query","award_id = A","execute_filter","","",yes,""
        1,"Second query","award_id = B","execute_filter","","",yes,""
        """,
    )

    with pytest.raises(DatasetError, match="duplicate case ID '1'"):
        parse_csv_cases(dataset_path)
