from usaspending_api.search.elasticsearch.filters.naics import NaicsCodes


def test_primative_naics_filter():
    assert NaicsCodes._query_string(["11"], []) == "(11*)"


def test_two_positive_sibling_naics():
    assert NaicsCodes._query_string(["11", "22"], []) == "(11*) OR (22*)"


def test_two_negative_sibling_naics():
    assert NaicsCodes._query_string([], ["11", "22"]) == "(NOT 11*) AND (NOT 22*)"
