import pytest
from django.db.models import Q

from usaspending_api.common.exceptions import UnprocessableEntityException
from usaspending_api.search.filters.elasticsearch.filter import QueryType
from usaspending_api.search.filters.elasticsearch.psc import PSCCodes as ESPSCCodes
from usaspending_api.search.filters.mixins.psc import PSCCodesMixin
from usaspending_api.search.filters.postgres.psc import PSCCodes as PGPSCCodes


def test_generate_elasticsearch_query():
    # Just a simple happy path test to detect potentially breaking changes.
    assert ESPSCCodes.generate_elasticsearch_query(
        {
            "require": [["Product", "1", "1111"], ["Research and Development"]],
            "exclude": [["Product", "1"], ["Research and Development", "A", "A5"]],
        },
        QueryType.AWARDS,
    ).to_dict() == {
        "query_string": {
            "query": "((((A*)) AND (((NOT (A* AND A5*)))))) OR ((((1* AND 1111))))",
            "default_field": "product_or_service_code.keyword",
        }
    }


def test_generate_postgres_query():
    # Just a simple happy path test to detect potentially breaking changes.
    assert PGPSCCodes.build_tas_codes_filter(
        {
            "require": [["Product", "1", "1111"], ["Research and Development"]],
            "exclude": [["Product", "1"], ["Research and Development", "A", "A5"]],
        }
    ) == Q(Q(product_or_service_code__istartswith="A") & ~Q(product_or_service_code__istartswith="A5")) | Q(
        product_or_service_code="1111"
    )


def test_validate_filter_values():
    # None of these should raise exceptions.
    PSCCodesMixin.validate_filter_values([])
    PSCCodesMixin.validate_filter_values(["A", "B"])
    PSCCodesMixin.validate_filter_values({})
    PSCCodesMixin.validate_filter_values({"require": [], "exclude": [], "whatever": []})
    PSCCodesMixin.validate_filter_values(
        {"require": [["Product"], ["Service", "B", "B5"]], "exclude": [["Service", "B", "B5"], ["Product"]]}
    )

    # These should all raise exceptions.
    with pytest.raises(UnprocessableEntityException):
        PSCCodesMixin.validate_filter_values("A")
    with pytest.raises(UnprocessableEntityException):
        PSCCodesMixin.validate_filter_values(1)
    with pytest.raises(UnprocessableEntityException):
        PSCCodesMixin.validate_filter_values([1])
    with pytest.raises(UnprocessableEntityException):
        PSCCodesMixin.validate_filter_values([{"whenever": "wherever"}])
    with pytest.raises(UnprocessableEntityException):
        PSCCodesMixin.validate_filter_values({"require": {"whenever": "wherever"}})
    with pytest.raises(UnprocessableEntityException):
        PSCCodesMixin.validate_filter_values({"require": [{"whenever": "wherever"}]})
    with pytest.raises(UnprocessableEntityException):
        PSCCodesMixin.validate_filter_values({"exclude": {"whenever": "wherever"}})
    with pytest.raises(UnprocessableEntityException):
        PSCCodesMixin.validate_filter_values({"exclude": [{"whenever": "wherever"}]})
    with pytest.raises(UnprocessableEntityException):
        PSCCodesMixin.validate_filter_values({"exclude": [[1]]})
    with pytest.raises(UnprocessableEntityException):
        PSCCodesMixin.validate_filter_values({"require": [["This is not a valid group", "B", "B5"]]})
    with pytest.raises(UnprocessableEntityException):
        PSCCodesMixin.validate_filter_values({"require": [["Service", "this is not a valid code", "B5"]]})


def test_split_filter_values():
    assert PSCCodesMixin.split_filter_values([]) == ([], [])
    assert PSCCodesMixin.split_filter_values(["A", "B"]) == ([["A"], ["B"]], [])
    assert PSCCodesMixin.split_filter_values({}) == ([], [])
    assert PSCCodesMixin.split_filter_values({"require": None, "exclude": None}) == ([], [])
    assert PSCCodesMixin.split_filter_values({"require": [], "exclude": [], "whatever": []}) == ([], [])
    assert PSCCodesMixin.split_filter_values(
        {"require": [["Product"], ["Service", "B", "B5"]], "exclude": [["Service", "B", "B5"], ["Product"]]}
    ) == ([["Product"], ["Service", "B", "B5"]], [["Service", "B", "B5"], ["Product"]])


def test_handle_tier1_names():
    assert PSCCodesMixin.handle_tier1_names([]) == []
    assert PSCCodesMixin.handle_tier1_names([["A"], ["B"]]) == [["A"], ["B"]]
    assert PSCCodesMixin.handle_tier1_names([["Product"]]) == [
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
    assert PSCCodesMixin.handle_tier1_names([["Service"]]) == [
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
    assert PSCCodesMixin.handle_tier1_names([["Research and Development"]]) == [["A"]]
    assert PSCCodesMixin.handle_tier1_names([["Product", "1", "11"]]) == [["1", "11"]]
    assert PSCCodesMixin.handle_tier1_names([["Product"], ["Product", "1", "11"]]) == [
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
    assert ESPSCCodes.code_is_parent_of([], []) is False
    assert ESPSCCodes.code_is_parent_of([1, 2], [1, 2, 3]) is True
    assert ESPSCCodes.code_is_parent_of([1, 2, 3], [1, 2]) is False
    assert ESPSCCodes.code_is_parent_of(["A"], ["A", "B"]) is True
    assert PGPSCCodes.code_is_parent_of([], []) is False
    assert PGPSCCodes.code_is_parent_of([1, 2], [1, 2, 3]) is True
    assert PGPSCCodes.code_is_parent_of([1, 2, 3], [1, 2]) is False
    assert PGPSCCodes.code_is_parent_of(["A"], ["A", "B"]) is True
