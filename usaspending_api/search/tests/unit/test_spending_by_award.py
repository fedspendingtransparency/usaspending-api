import pytest

from usaspending_api.awards.v2.lookups.lookups import contract_subaward_mapping
from usaspending_api.common.helpers.api_helper import raise_if_award_types_not_valid_subset, raise_if_sort_key_not_valid
from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.v2.views.spending_by_award import SpendingByAwardVisualizationViewSet, GLOBAL_MAP
from usaspending_api.common.exceptions import UnprocessableEntityException, InvalidParameterException


def instantiate_view_for_tests():
    view = SpendingByAwardVisualizationViewSet()
    view.is_subaward = True
    view.constants = GLOBAL_MAP["subaward"]
    view.filters = {"award_type_codes": ["A"]}
    view.original_filters = {"award_type_codes": ["A"]}
    view.fields = ["Sub-Award Type", "Prime Award ID"]
    view.pagination = {
        "limit": 5,
        "lower_bound": 1,
        "page": 1,
        "sort_key": "Prime Award ID",
        "sort_order": "desc",
        "upper_bound": 6,
    }
    return view


def get_spending_by_award_url():
    return "/api/v2/search/spending_by_award/"


def test_no_intersection():
    view = SpendingByAwardVisualizationViewSet()
    view.filters = {}
    view.filters["award_type_codes"] = ["one", "two", "three"]
    assert not view.if_no_intersection(), "false positive"

    view.filters["award_type_codes"].append("no intersection")
    assert view.if_no_intersection(), "Faulty check method!!!"


def test_raise_if_award_types_not_valid_subset():
    valid_award_codes = ["09", "11"]
    valid_subaward_codes = ["IDV_C", "A", "IDV_B_A"]
    invalid_award_codes = ["A", "IDV_A"]
    invalid_subaward_codes = ["10", "A"]

    assert raise_if_award_types_not_valid_subset(valid_award_codes, is_subaward=False) is None
    assert raise_if_award_types_not_valid_subset(valid_subaward_codes, is_subaward=True) is None

    with pytest.raises(UnprocessableEntityException):
        raise_if_award_types_not_valid_subset(invalid_award_codes, is_subaward=False)

    with pytest.raises(UnprocessableEntityException):
        raise_if_award_types_not_valid_subset(invalid_subaward_codes, is_subaward=True)

    with pytest.raises(UnprocessableEntityException):
        raise_if_award_types_not_valid_subset(valid_subaward_codes, is_subaward=False)


def test_raise_if_sort_key_not_valid():
    example_award_fields = [
        "Award ID",
        "Recipient Name",
        "Start Date",
        "End Date",
        "Award Amount",
        "Awarding Agency",
        "Awarding Sub Agency",
        "Contract Award Type",
    ]

    example_subaward_fields = [
        "Sub-Award ID",
        "Prime Recipient Name",
        "Sub-Award Date",
        "Sub-Awardee Name",
        "Sub-Award Amount",
        "Awarding Agency",
        "Awarding Sub Agency",
        "Sub-Award Type",
    ]

    assert raise_if_sort_key_not_valid(sort_key="Award ID", field_list=example_award_fields, is_subaward=False) is None
    assert raise_if_sort_key_not_valid(sort_key="End Date", field_list=example_award_fields, is_subaward=False) is None
    assert (
        raise_if_sort_key_not_valid(sort_key="Awarding Agency", field_list=example_subaward_fields, is_subaward=True)
        is None
    )
    assert (
        raise_if_sort_key_not_valid(sort_key="Sub-Award ID", field_list=example_subaward_fields, is_subaward=True)
        is None
    )

    with pytest.raises(InvalidParameterException):
        # Valid mapping, but missing from the field list
        raise_if_sort_key_not_valid(sort_key="Description", field_list=example_award_fields, is_subaward=False)

    with pytest.raises(InvalidParameterException):
        raise_if_sort_key_not_valid(sort_key="Bad Key", field_list=example_award_fields, is_subaward=False)

    with pytest.raises(InvalidParameterException):
        raise_if_sort_key_not_valid(sort_key="", field_list=example_award_fields, is_subaward=False)

    with pytest.raises(InvalidParameterException):
        raise_if_sort_key_not_valid(sort_key=None, field_list=example_award_fields, is_subaward=False)

    with pytest.raises(InvalidParameterException):
        raise_if_sort_key_not_valid(sort_key="Bad Key", field_list=example_subaward_fields, is_subaward=True)

    with pytest.raises(InvalidParameterException):
        raise_if_sort_key_not_valid(sort_key="Description", field_list=example_subaward_fields, is_subaward=True)


def test_get_queryset():
    view = instantiate_view_for_tests()

    assert view.construct_queryset() is not None, "Failed to return a queryset object"

    db_fields_from_api = set([contract_subaward_mapping[field] for field in view.fields])
    db_fields_from_api |= set(GLOBAL_MAP["subaward"]["minimum_db_fields"])

    assert db_fields_from_api == set(view.get_database_fields())

    qs_fields = set(view.construct_queryset().__dict__["_fields"])

    assert qs_fields == db_fields_from_api


def test_populate_response():
    view = instantiate_view_for_tests()

    expected_dictionary = {
        "limit": 5,
        "results": ["item 1", "item 2"],
        "page_metadata": {"page": 1, "hasNext": True},
        "messages": [get_time_period_message()],
    }
    assert view.populate_response(results=["item 1", "item 2"], has_next=True) == expected_dictionary

    expected_dictionary["results"] = []
    assert view.populate_response(results=[], has_next=True) == expected_dictionary
