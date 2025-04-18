import pytest

from usaspending_api.awards.v2.lookups.lookups import subaward_mapping
from usaspending_api.common.helpers.api_helper import (
    raise_if_award_types_not_valid_subset,
    raise_if_sort_key_not_valid,
    get_award_type_and_mapping_values,
)
from usaspending_api.common.helpers.generic_helper import get_time_period_message
from usaspending_api.search.v2.views.enums import SpendingLevel
from usaspending_api.search.v2.views.spending_by_award import SpendingByAwardVisualizationViewSet, GLOBAL_MAP
from usaspending_api.common.exceptions import UnprocessableEntityException, InvalidParameterException
from usaspending_api.awards.v2.lookups.elasticsearch_lookups import (
    contracts_mapping,
    idv_mapping,
    loan_mapping,
    non_loan_assist_mapping,
)


def instantiate_view_for_tests():
    view = SpendingByAwardVisualizationViewSet()
    view.spending_level = SpendingLevel.SUBAWARD
    view.constants = GLOBAL_MAP["subawards"]
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

    assert raise_if_award_types_not_valid_subset(valid_award_codes, spending_level=SpendingLevel.AWARD) is None
    assert raise_if_award_types_not_valid_subset(valid_subaward_codes, spending_level=SpendingLevel.SUBAWARD) is None

    with pytest.raises(UnprocessableEntityException):
        raise_if_award_types_not_valid_subset(invalid_award_codes, spending_level=SpendingLevel.AWARD)

    with pytest.raises(UnprocessableEntityException):
        raise_if_award_types_not_valid_subset(invalid_subaward_codes, spending_level=SpendingLevel.SUBAWARD)

    with pytest.raises(UnprocessableEntityException):
        raise_if_award_types_not_valid_subset(valid_subaward_codes, spending_level=SpendingLevel.AWARD)


def test_raise_if_sort_key_not_valid():
    example_contract_award_codes = ["A", "B"]
    example_idv_award_codes = ["IDV_A", "IDV_B"]
    example_loan_award_codes = ["07", "08"]
    example_non_loan_assist_award_codes = ["09"]

    example_base_award_fields = [
        "Award ID",
        "Recipient Name",
        "Awarding Agency",
        "Awarding Sub Agency",
    ]

    example_contract_award_fields = [
        "Start Date",
        "End Date",
        "Award Amount",
        "Contract Award Type",
    ] + example_base_award_fields

    example_idv_award_fields = [
        "Start Date",
        "End Date",
        "Award Amount",
        "Last Date to Order",
    ] + example_base_award_fields

    example_loan_award_fields = ["Issued Date", "Loan Value"] + example_base_award_fields

    example_non_loan_assist_award_fields = [
        "Start Date",
        "End Date",
        "Award Amount",
        "Award Type",
    ] + example_base_award_fields

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

    assert (
        raise_if_sort_key_not_valid(
            sort_key="Award ID",
            field_list=example_contract_award_fields,
            award_type_codes=example_contract_award_codes,
            spending_level=SpendingLevel.AWARD,
        )
        is None
    )
    assert (
        raise_if_sort_key_not_valid(
            sort_key="End Date",
            field_list=example_contract_award_fields,
            award_type_codes=example_contract_award_codes,
            spending_level=SpendingLevel.AWARD,
        )
        is None
    )
    assert (
        raise_if_sort_key_not_valid(
            sort_key="Awarding Agency",
            field_list=example_subaward_fields,
            award_type_codes=example_contract_award_codes,
            spending_level=SpendingLevel.SUBAWARD,
        )
        is None
    )
    assert (
        raise_if_sort_key_not_valid(
            sort_key="Sub-Award ID",
            field_list=example_subaward_fields,
            award_type_codes=example_contract_award_codes,
            spending_level=SpendingLevel.SUBAWARD,
        )
        is None
    )
    assert (
        raise_if_sort_key_not_valid(
            sort_key="Last Date to Order",
            field_list=example_idv_award_fields,
            award_type_codes=example_idv_award_codes,
            spending_level=SpendingLevel.AWARD,
        )
        is None
    )
    assert (
        raise_if_sort_key_not_valid(
            sort_key="Issued Date",
            field_list=example_loan_award_fields,
            award_type_codes=example_loan_award_codes,
            spending_level=SpendingLevel.AWARD,
        )
        is None
    )
    assert (
        raise_if_sort_key_not_valid(
            sort_key="Award Type",
            field_list=example_non_loan_assist_award_fields,
            award_type_codes=example_non_loan_assist_award_codes,
            spending_level=SpendingLevel.AWARD,
        )
        is None
    )

    with pytest.raises(InvalidParameterException):
        # Valid mapping, but missing from the field list
        raise_if_sort_key_not_valid(
            sort_key="Description",
            field_list=example_contract_award_fields,
            award_type_codes=example_contract_award_codes,
            spending_level=SpendingLevel.AWARD,
        )

    with pytest.raises(InvalidParameterException):
        raise_if_sort_key_not_valid(
            sort_key="Bad Key",
            field_list=example_contract_award_fields,
            award_type_codes=example_contract_award_codes,
            spending_level=SpendingLevel.AWARD,
        )

    with pytest.raises(InvalidParameterException):
        raise_if_sort_key_not_valid(
            sort_key="",
            field_list=example_contract_award_fields,
            award_type_codes=example_contract_award_codes,
            spending_level=SpendingLevel.AWARD,
        )

    with pytest.raises(InvalidParameterException):
        raise_if_sort_key_not_valid(
            sort_key=None,
            field_list=example_contract_award_fields,
            award_type_codes=example_contract_award_codes,
            spending_level=SpendingLevel.AWARD,
        )

    with pytest.raises(InvalidParameterException):
        raise_if_sort_key_not_valid(
            sort_key="Bad Key",
            field_list=example_subaward_fields,
            award_type_codes=example_contract_award_codes,
            spending_level=SpendingLevel.SUBAWARD,
        )

    with pytest.raises(InvalidParameterException):
        raise_if_sort_key_not_valid(
            sort_key="Description",
            field_list=example_subaward_fields,
            award_type_codes=example_contract_award_codes,
            spending_level=SpendingLevel.SUBAWARD,
        )

    # Check that it won't allow a sort key that isn't in a specific award mapping
    with pytest.raises(InvalidParameterException):
        raise_if_sort_key_not_valid(
            sort_key="Issued Date",
            field_list=example_contract_award_fields,
            award_type_codes=example_contract_award_codes,
            spending_level=SpendingLevel.AWARD,
        )

    with pytest.raises(InvalidParameterException):
        raise_if_sort_key_not_valid(
            sort_key="Start Date",
            field_list=example_loan_award_fields,
            award_type_codes=example_loan_award_codes,
            spending_level=SpendingLevel.AWARD,
        )


def test_get_award_type_and_mapping_values():
    example_contract_award_codes = ["A", "B"]
    example_idv_award_codes = ["IDV_A", "IDV_B"]
    example_loan_award_codes = ["07", "08"]
    example_non_loan_assist_award_codes = ["09"]
    invalid_award_type_codes = ["07", "A"]

    subaward_results = get_award_type_and_mapping_values(
        award_type_codes=example_contract_award_codes, spending_level=SpendingLevel.SUBAWARD
    )
    assert subaward_results == ("Sub-Award", list(subaward_mapping.keys()))

    contract_results = get_award_type_and_mapping_values(
        award_type_codes=example_contract_award_codes, spending_level=SpendingLevel.AWARD
    )
    assert contract_results == ("Contract Award", list(contracts_mapping.keys()))

    idv_results = get_award_type_and_mapping_values(
        award_type_codes=example_idv_award_codes, spending_level=SpendingLevel.AWARD
    )
    assert idv_results == ("IDV Award", list(idv_mapping.keys()))

    loan_results = get_award_type_and_mapping_values(
        award_type_codes=example_loan_award_codes, spending_level=SpendingLevel.AWARD
    )
    assert loan_results == ("Loan Award", list(loan_mapping.keys()))

    non_loan_assist_results = get_award_type_and_mapping_values(
        award_type_codes=example_non_loan_assist_award_codes, spending_level=SpendingLevel.AWARD
    )
    assert non_loan_assist_results == ("Non-Loan Assistance Award", list(non_loan_assist_mapping.keys()))

    # Check that it will throw an exception if the award codes do not match any award type mappings
    with pytest.raises(InvalidParameterException):
        get_award_type_and_mapping_values(
            award_type_codes=invalid_award_type_codes,
            spending_level=SpendingLevel.AWARD,
        )


def test_populate_response():
    view = instantiate_view_for_tests()

    expected_dictionary = {
        "limit": 5,
        "results": ["item 1", "item 2"],
        "page_metadata": {"page": 1, "hasNext": True},
        "messages": [get_time_period_message()],
    }
    assert (
        view.populate_response(results=["item 1", "item 2"], has_next=True, models=[{"name": "award_type_codes"}])
        == expected_dictionary
    )

    expected_dictionary["results"] = []
    assert (
        view.populate_response(results=[], has_next=True, models=[{"name": "award_type_codes"}]) == expected_dictionary
    )
