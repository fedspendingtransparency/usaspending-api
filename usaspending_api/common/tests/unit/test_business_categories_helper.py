# Imports from your apps
from usaspending_api.common.helpers.business_categories_helper import (
    BUSINESS_CATEGORIES_LOOKUP_DICT,
    get_business_category_display_names,
)


def _get_all_business_category_field_names():
    return list(BUSINESS_CATEGORIES_LOOKUP_DICT.keys())


def _get_first_ten_business_category_field_names():
    return list(BUSINESS_CATEGORIES_LOOKUP_DICT.keys())[:10]


def _get_all_business_category_display_names():
    return list(BUSINESS_CATEGORIES_LOOKUP_DICT.values())


def _get_first_ten_business_category_display_names():
    return list(BUSINESS_CATEGORIES_LOOKUP_DICT.values())[:10]


def test_get_ten_valid_business_category_display_names():
    business_category_field_names_list = _get_first_ten_business_category_field_names()
    business_category_display_names_list = _get_first_ten_business_category_display_names()
    assert (
        get_business_category_display_names(business_category_field_names_list) == business_category_display_names_list
    )


def test_get_all_valid_business_category_display_names():
    business_category_field_names_list = _get_all_business_category_field_names()
    business_category_display_names_list = _get_all_business_category_display_names()
    assert (
        get_business_category_display_names(business_category_field_names_list) == business_category_display_names_list
    )


def test_get_invalid_business_category_display_names():
    business_category_field_names_list = _get_all_business_category_field_names()
    business_category_field_names_list.insert(0, "invalid_name_1")
    business_category_field_names_list.append("invalid_name_2")
    business_category_display_names_list = _get_all_business_category_display_names()
    assert (
        get_business_category_display_names(business_category_field_names_list) == business_category_display_names_list
    )


def test_get_empty_business_category_display_names():
    business_category_field_names_list = []
    assert get_business_category_display_names(business_category_field_names_list) == []
