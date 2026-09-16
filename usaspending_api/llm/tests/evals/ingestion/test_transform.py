import pytest

from usaspending_api.llm.evals.exceptions import DatasetError
from usaspending_api.llm.evals.ingestion.models import (
    FilterMapping,
    GroundTruthSource,
    WorkbookData,
)
from usaspending_api.llm.evals.ingestion.transform import transform_workbook

SOURCE = GroundTruthSource(
    content=b"",
    source_name="fixture.xlsx",
    source_reference="fixture.xlsx",
)


def workbook_data(rows, mappings):
    return WorkbookData(
        ground_truth_rows=tuple(rows),
        filter_mappings=tuple(mappings),
        source=SOURCE,
    )


def mappings():
    return (
        FilterMapping("filter", "Filters", "Recipient", "recipient", "selectedRecipients"),
        FilterMapping("filter", "Filters", "Time Period", "time_period.fiscal_year", "timePeriodFY"),
        FilterMapping("filter", "Filters", "Award Type", "award_type", "awardType"),
        FilterMapping("tool", "Tools", "Recipient", "lookup recipient", "lookup_recipient"),
        FilterMapping("tool", "Tools", "Completion", "execute filter", "execute_filter"),
    )


def row():
    return {
        "id": 1,
        "query": "Find Clark Construction contracts in FY25",
        "expected_output": '{"recipient": "Clark Construction", "time_period": {"fiscal_year": 2025}, "award_type": "Contracts"}',
        "expected_tools": '["lookup recipient", "execute filter"]',
        "tags": '["recipient", "temporal"]',
        "notes": "",
        "approved": "yes",
        "sme_validation_notes": "",
    }


def test_transform_workbook_maps_filters_tools_and_value_shapes():
    cases = transform_workbook(workbook_data([row()], mappings()))

    assert cases == [
        {
            "id": 1,
            "query": "Find Clark Construction contracts in FY25",
            "expected_output": {
                "selectedRecipients": ["Clark Construction"],
                "timePeriodFY": ["2025"],
                "awardType": ["Contracts"],
                "timePeriodType": "fy",
            },
            "expected_tools": ["lookup_recipient", "execute_filter"],
            "tags": ["recipient", "temporal"],
            "notes": "",
            "approved": True,
            "sme_validation_notes": "",
        }
    ]


def test_transform_workbook_preserves_structured_tool_arguments():
    case = row()
    case["expected_tools"] = '[{"name": "lookup recipient", "arguments": {"query": "Clark Construction"}}, "execute filter"]'

    result = transform_workbook(workbook_data([case], mappings()))

    assert result[0]["expected_tools"] == [
        {
            "name": "lookup_recipient",
            "arguments": {"query": "Clark Construction"},
        },
        "execute_filter",
    ]


def test_transform_workbook_preserves_structured_mapped_filter():
    case = row()
    case["expected_output"] = '{"location": {"USA_TX": {"filter": {"state": "TX"}}}}'
    location_mapping = FilterMapping(
        "filter",
        "Filters",
        "Location",
        "location",
        "selectedLocations",
    )

    result = transform_workbook(
        workbook_data(
            [case],
            [location_mapping, mappings()[3], mappings()[4]],
        )
    )

    assert result[0]["expected_output"] == {
        "selectedLocations": {
            "USA_TX": {"filter": {"state": "TX"}},
        },
    }


def test_transform_workbook_rejects_unmapped_filter():
    case = row()
    case["expected_output"] = '{"unknown_filter": "value"}'

    with pytest.raises(DatasetError, match="unmapped filter field"):
        transform_workbook(workbook_data([case], mappings()))


def test_transform_workbook_rejects_unmapped_tool():
    case = row()
    case["expected_tools"] = '["unknown tool"]'

    with pytest.raises(DatasetError, match="unmapped tool"):
        transform_workbook(workbook_data([case], mappings()))
