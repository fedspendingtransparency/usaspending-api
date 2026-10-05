from decimal import Decimal

import pytest

from usaspending_api.search.v2.views.spending_by_subaward_grouped import (
    SpendingBySubawardGroupedVisualizationViewSet,
    _quantize_amount,
    _quantize_ratio,
)


@pytest.mark.parametrize(
    "value,expected",
    [
        (None, 0.0),
        (0, 0.0),
        ("0", 0.0),
        ("", 0),
        (100, Decimal("100.00")),
        ("100.5", Decimal("100.50")),
        (Decimal("1234.5678"), Decimal("1234.57")),
    ],
)
def test_quantize_amount(value, expected):
    assert _quantize_amount(value) == expected


@pytest.mark.parametrize(
    "subaward_amount,award_amount,expected",
    [
        (100, None, 0.0),
        (100, 0, 0.0),
        (None, 100, Decimal("0.00000000")),
        (50, 100, Decimal("0.50000000")),
        (Decimal("100.5"), Decimal("1000"), Decimal("0.10050000")),
    ],
)
def test_quantize_ratio(subaward_amount, award_amount, expected):
    assert _quantize_ratio(subaward_amount, award_amount) == expected


def test_build_result_includes_award_obligation_and_ratio():
    source = {
        "display_award_id": "ABC123",
        "subaward_count": 5,
        "total_subaward_amount": 500,
        "award_amount": 1000,
        "generated_unique_award_id": "CONT_AWD_ABC123",
    }

    result = SpendingBySubawardGroupedVisualizationViewSet._build_result(source)

    assert result == {
        "award_id": "ABC123",
        "subaward_count": 5,
        "subaward_obligation": Decimal("500.00"),
        "award_obligation": Decimal("1000.00"),
        "subaward_to_award_ratio": Decimal("0.50000000"),
        "award_generated_internal_id": "CONT_AWD_ABC123",
    }


def test_build_result_with_missing_amounts():
    source = {
        "display_award_id": "XYZ789",
        "subaward_count": 0,
        "total_subaward_amount": None,
        "award_amount": None,
        "generated_unique_award_id": "CONT_AWD_XYZ789",
    }

    result = SpendingBySubawardGroupedVisualizationViewSet._build_result(source)

    assert result == {
        "award_id": "XYZ789",
        "subaward_count": 0,
        "subaward_obligation": 0.0,
        "award_obligation": 0.0,
        "subaward_to_award_ratio": 0.0,
        "award_generated_internal_id": "CONT_AWD_XYZ789",
    }


def test_build_result_zero_award_amount_gives_zero_ratio():
    source = {
        "display_award_id": "ZERO-AWARD",
        "subaward_count": 2,
        "total_subaward_amount": 500,
        "award_amount": 0,
        "generated_unique_award_id": "CONT_AWD_ZERO",
    }

    result = SpendingBySubawardGroupedVisualizationViewSet._build_result(source)

    assert result["award_obligation"] == 0.0
    assert result["subaward_obligation"] == Decimal("500.00")
    assert result["subaward_to_award_ratio"] == 0.0
