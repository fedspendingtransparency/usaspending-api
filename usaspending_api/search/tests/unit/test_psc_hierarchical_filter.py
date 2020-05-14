import pytest

from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.search.filters.elasticsearch.filter import _QueryType
from usaspending_api.search.filters.elasticsearch.psc import PSCCodes


def test_generate_elasticsearch_query():
    # Just a simple happy path test to potentially detect breaking changes.
    assert PSCCodes.generate_elasticsearch_query(
        {"require": [["Product"], ["Service", "B", "B5"]], "exclude": [["Service", "B", "B5"], ["Product"]]},
        _QueryType.AWARDS,
    ).to_dict() == {
        "query_string": {
            "query": (
                "(((0*)) OR ((1*)) OR ((2*)) OR ((3*)) OR ((4*)) OR ((5*)) OR ((6*)) OR ((7*)) OR "
                "((8*)) OR ((9*)) OR ((B5*))) AND (((NOT B5*)) AND ((NOT 0*)) AND ((NOT 1*)) AND "
                "((NOT 2*)) AND ((NOT 3*)) AND ((NOT 4*)) AND ((NOT 5*)) AND ((NOT 6*)) AND "
                "((NOT 7*)) AND ((NOT 8*)) AND ((NOT 9*)))"
            ),
            "default_field": "product_or_service_code.keyword",
        }
    }


def test_validate_filter_values():
    # None of these should raise exceptions.
    PSCCodes.validate_filter_values([])
    PSCCodes.validate_filter_values(["A", "B"])
    PSCCodes.validate_filter_values({})
    PSCCodes.validate_filter_values({"require": [], "exclude": [], "whatever": []})
    PSCCodes.validate_filter_values(
        {"require": [["Product"], ["Service", "B", "B5"]], "exclude": [["Service", "B", "B5"], ["Product"]]}
    )

    # These should all raise exceptions.
    with pytest.raises(UnprocessableEntityException):
        PSCCodes.validate_filter_values("A")
    with pytest.raises(UnprocessableEntityException):
        PSCCodes.validate_filter_values(1)
    with pytest.raises(UnprocessableEntityException):
        PSCCodes.validate_filter_values([1])
    with pytest.raises(UnprocessableEntityException):
        PSCCodes.validate_filter_values([{"whenever": "wherever"}])
    with pytest.raises(UnprocessableEntityException):
        PSCCodes.validate_filter_values({"require": {"whenever": "wherever"}})
    with pytest.raises(UnprocessableEntityException):
        PSCCodes.validate_filter_values({"require": [{"whenever": "wherever"}]})
    with pytest.raises(UnprocessableEntityException):
        PSCCodes.validate_filter_values({"exclude": {"whenever": "wherever"}})
    with pytest.raises(UnprocessableEntityException):
        PSCCodes.validate_filter_values({"exclude": [{"whenever": "wherever"}]})
    with pytest.raises(UnprocessableEntityException):
        PSCCodes.validate_filter_values({"exclude": [[1]]})
    with pytest.raises(UnprocessableEntityException):
        PSCCodes.validate_filter_values({"require": [["This is not a valid group", "B", "B5"]]})
    with pytest.raises(UnprocessableEntityException):
        PSCCodes.validate_filter_values({"require": [["Service", "this is not a valid code", "B5"]]})


def test_split_filter_values():
    assert PSCCodes.split_filter_values([]) == ([], [])
    assert PSCCodes.split_filter_values(["A", "B"]) == ([["A"], ["B"]], [])
    assert PSCCodes.split_filter_values({}) == ([], [])
    assert PSCCodes.split_filter_values({"require": None, "exclude": None}) == ([], [])
    assert PSCCodes.split_filter_values({"require": [], "exclude": [], "whatever": []}) == ([], [])
    assert PSCCodes.split_filter_values(
        {"require": [["Product"], ["Service", "B", "B5"]], "exclude": [["Service", "B", "B5"], ["Product"]]}
    ) == ([["Product"], ["Service", "B", "B5"]], [["Service", "B", "B5"], ["Product"]])


def test_handle_tier1_names():
    assert PSCCodes.handle_tier1_names([]) == []
    assert PSCCodes.handle_tier1_names([["A"], ["B"]]) == [["A"], ["B"]]
    assert PSCCodes.handle_tier1_names([["Product"]]) == [
        ["0"],
        ["1"],
        ["2"],
        ["3"],
        ["4"],
        ["5"],
        ["6"],
        ["7"],
        ["8"],
        ["9"],
    ]
    assert PSCCodes.handle_tier1_names([["Service"]]) == [
        ["B"],
        ["C"],
        ["D"],
        ["E"],
        ["F"],
        ["G"],
        ["H"],
        ["I"],
        ["J"],
        ["K"],
        ["L"],
        ["M"],
        ["N"],
        ["O"],
        ["P"],
        ["Q"],
        ["R"],
        ["S"],
        ["T"],
        ["U"],
        ["V"],
        ["W"],
        ["X"],
        ["Y"],
        ["Z"],
    ]
    assert PSCCodes.handle_tier1_names([["Research and Development"]]) == [["A"]]
    assert PSCCodes.handle_tier1_names([["Product", "1", "11"]]) == [["1", "11"]]
    assert PSCCodes.handle_tier1_names([["Product"], ["Product", "1", "11"]]) == [
        ["0"],
        ["1"],
        ["2"],
        ["3"],
        ["4"],
        ["5"],
        ["6"],
        ["7"],
        ["8"],
        ["9"],
        ["1", "11"],
    ]


def test_code_is_parent_of():
    assert PSCCodes.code_is_parent_of([], []) is False
    assert PSCCodes.code_is_parent_of([1, 2], [1, 2, 3]) is True
    assert PSCCodes.code_is_parent_of([1, 2, 3], [1, 2]) is False
    assert PSCCodes.code_is_parent_of(["A"], ["A", "B"]) is True
