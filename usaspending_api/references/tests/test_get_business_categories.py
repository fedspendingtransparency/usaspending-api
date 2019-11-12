from usaspending_api.broker.helpers.get_business_categories import get_business_categories


def test_update_business_type_categories():
    business_types = "P"
    business_categories = get_business_categories({"business_types": business_types}, "fabs")
    assert business_categories == ["individuals"]

    business_types = "L"
    business_categories = get_business_categories({"business_types": business_types}, "fabs")
    assert "authorities_and_commissions" in business_categories
    assert "government" in business_categories

    business_types = "M"
    business_categories = get_business_categories({"business_types": business_types}, "fabs")
    assert "nonprofit" in business_categories


def test_update_business_type_categories_faads_format():
    business_types = "01"  # B equivalent
    business_categories = get_business_categories({"business_types": business_types}, "fabs")
    assert "government" in business_categories
    assert "local_government" in business_categories

    business_types = "12"  # M equivalent
    business_categories = get_business_categories({"business_types": business_types}, "fabs")
    assert "nonprofit" in business_categories

    business_types = "21"  # P equivalent
    business_categories = get_business_categories({"business_types": business_types}, "fabs")
    assert "individuals" in business_categories

    business_types = "23"  # R equivalent
    business_categories = get_business_categories({"business_types": business_types}, "fabs")
    assert "small_business" in business_categories
    assert "category_business" in business_categories
