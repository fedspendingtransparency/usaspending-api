from typing import List

from elasticsearch_dsl.response.hit import Hit

from usaspending_api.references.v2.views.location_autocomplete import LocationAutocompleteViewSet


def test_country_formatter():
    # Test formatter returns results in correct format
    mock_es_data: List[Hit] = [
        Hit(
            {
                "_source": {
                    "country_name": "UNITED STATES",
                    "state_name": "",
                    "cities": [],
                    "counties": [],
                    "zip_codes": [],
                    "current_congressional_districts": [],
                    "original_congressional_districts": [],
                },
                "highlight": {"country_name": "UNITED STATES"},
            }
        ),
        Hit(
            {
                "_source": {
                    "country_name": "CANADA",
                    "state_name": "",
                    "cities": [],
                    "counties": [],
                    "zip_codes": [],
                    "current_congressional_districts": [],
                    "original_congressional_districts": [],
                },
                "highlight": {"country_name": "CANADA"},
            }
        ),
    ]
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_country_results(es_results=mock_es_data) == [
        {"country_name": "UNITED STATES"},
        {"country_name": "CANADA"},
    ]

    # Test formatter returns None when hits are present
    mock_es_data_none: List = []
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_country_results(es_results=mock_es_data_none) is None


def test_state_formatter():
    # Test formatter returns results in correct format
    mock_es_data: List[Hit] = [
        Hit(
            {
                "_source": {
                    "country_name": "UNITED STATES",
                    "state_name": "CALIFORNIA",
                    "cities": [],
                    "counties": [],
                    "zip_codes": [],
                    "current_congressional_districts": [],
                    "original_congressional_districts": [],
                },
                "highlight": {"state_name": "CALIFORNIA"},
            }
        ),
        Hit(
            {
                "_source": {
                    "country_name": "UNITED STATES",
                    "state_name": "TEXAS",
                    "cities": [],
                    "counties": [],
                    "zip_codes": [],
                    "current_congressional_districts": [],
                    "original_congressional_districts": [],
                },
                "highlight": {"state_name": "TEXAS"},
            }
        ),
    ]
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_state_results(es_results=mock_es_data) == [
        {"state_name": "CALIFORNIA", "country_name": "UNITED STATES"},
        {"state_name": "TEXAS", "country_name": "UNITED STATES"},
    ]

    # Test formatter returns None when hits are present
    mock_es_data_none: List = []
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_state_results(es_results=mock_es_data_none) is None


def test_city_formatter():
    # Test formatter returns results in correct format
    mock_es_data: List[Hit] = [
        Hit(
            {
                "_source": {
                    "country_name": "UNITED STATES",
                    "state_name": "CALIFORNIA",
                    "cities": ["SAN DIEGO", "LOS ANGELES"],
                    "counties": [],
                    "zip_codes": [],
                    "current_congressional_districts": [],
                    "original_congressional_districts": [],
                },
                "highlight": {"cities": ["SAN DIEGO", "LOS ANGELES"]},
            }
        ),
        Hit(
            {
                "_source": {
                    "country_name": "UNITED STATES",
                    "state_name": "TEXAS",
                    "cities": ["DALLAS", "HOUSTON"],
                    "counties": [],
                    "zip_codes": [],
                    "current_congressional_districts": [],
                    "original_congressional_districts": [],
                },
                "highlight": {"cities": ["DALLAS", "HOUSTON"]},
            }
        ),
    ]
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_city_results(es_results=mock_es_data) == [
        {"city_name": "SAN DIEGO", "state_name": "CALIFORNIA", "country_name": "UNITED STATES"},
        {"city_name": "LOS ANGELES", "state_name": "CALIFORNIA", "country_name": "UNITED STATES"},
        {"city_name": "DALLAS", "state_name": "TEXAS", "country_name": "UNITED STATES"},
        {"city_name": "HOUSTON", "state_name": "TEXAS", "country_name": "UNITED STATES"},
    ]

    # Test formatter returns None when hits are present
    mock_es_data_none: List = []
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_city_results(es_results=mock_es_data_none) is None


def test_county_formatter():
    # Test formatter returns results in correct format
    mock_es_data: List[Hit] = [
        Hit(
            {
                "_source": {
                    "country_name": "UNITED STATES",
                    "state_name": "FLORIDA",
                    "cities": [],
                    "counties": ["GADSDEN"],
                    "zip_codes": [],
                    "current_congressional_districts": [],
                    "original_congressional_districts": [],
                },
                "highlight": {"counties": ["GADSDEN"]},
            }
        ),
        Hit(
            {
                "_source": {
                    "country_name": "UNITED STATES",
                    "state_name": "GEORGIA",
                    "cities": [],
                    "counties": ["CAMDEN"],
                    "zip_codes": [],
                    "current_congressional_districts": [],
                    "original_congressional_districts": [],
                },
                "highlight": {"counties": ["CAMDEN"]},
            }
        ),
    ]
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_county_results(es_results=mock_es_data) == [
        {"county_name": "GADSDEN", "state_name": "FLORIDA", "country_name": "UNITED STATES"},
        {"county_name": "CAMDEN", "state_name": "GEORGIA", "country_name": "UNITED STATES"},
    ]

    # Test formatter returns None when hits are present
    mock_es_data_none: List = []
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_county_results(es_results=mock_es_data_none) is None


def test_zip_code_formatter():
    # Test formatter returns results in correct format
    mock_es_data: List[Hit] = [
        Hit(
            {
                "_source": {
                    "country_name": "UNITED STATES",
                    "state_name": "CALIFORNIA",
                    "cities": [],
                    "counties": [],
                    "zip_codes": [12345, 12346],
                    "current_congressional_districts": [],
                    "original_congressional_districts": [],
                },
                "highlight": {"zip_codes": [12345, 12346]},
            }
        ),
        Hit(
            {
                "_source": {
                    "country_name": "UNITED STATES",
                    "state_name": "TEXAS",
                    "cities": [],
                    "counties": [],
                    "zip_codes": [23456, 23457],
                    "current_congressional_districts": [],
                    "original_congressional_districts": [],
                },
                "highlight": {"zip_codes": [23456, 23457]},
            }
        ),
    ]
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_zip_code_results(es_results=mock_es_data) == [
        {"zip_code": 12345, "state_name": "CALIFORNIA", "country_name": "UNITED STATES"},
        {"zip_code": 12346, "state_name": "CALIFORNIA", "country_name": "UNITED STATES"},
        {"zip_code": 23456, "state_name": "TEXAS", "country_name": "UNITED STATES"},
        {"zip_code": 23457, "state_name": "TEXAS", "country_name": "UNITED STATES"},
    ]

    # Test formatter returns None when hits are present
    mock_es_data_none: List = []
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_zip_code_results(es_results=mock_es_data_none) is None


def test_current_cd_formatter():
    # Test formatter returns results in correct format
    mock_es_data: List[Hit] = [
        Hit(
            {
                "_source": {
                    "country_name": "UNITED STATES",
                    "state_name": "CALIFORNIA",
                    "cities": [],
                    "counties": [],
                    "zip_codes": [],
                    "current_congressional_districts": ["CA01", "CA02"],
                    "original_congressional_districts": [],
                },
                "highlight": {"current_congressional_districts": ["CA01", "CA02"]},
            }
        ),
        Hit(
            {
                "_source": {
                    "country_name": "UNITED STATES",
                    "state_name": "TEXAS",
                    "cities": [],
                    "counties": [],
                    "zip_codes": [],
                    "current_congressional_districts": ["TX01", "TX02"],
                    "original_congressional_districts": [],
                },
                "highlight": {"current_congressional_districts": ["TX01", "TX02"]},
            }
        ),
    ]
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_current_cd_results(es_results=mock_es_data) == [
        {"current_cd": "CA-01", "state_name": "CALIFORNIA", "country_name": "UNITED STATES"},
        {"current_cd": "CA-02", "state_name": "CALIFORNIA", "country_name": "UNITED STATES"},
        {"current_cd": "TX-01", "state_name": "TEXAS", "country_name": "UNITED STATES"},
        {"current_cd": "TX-02", "state_name": "TEXAS", "country_name": "UNITED STATES"},
    ]

    # Test formatter returns None when hits are present
    mock_es_data_none: List = []
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_current_cd_results(es_results=mock_es_data_none) is None


def test_original_cd_formatter():
    # Test formatter returns results in correct format
    mock_es_data: List[Hit] = [
        Hit(
            {
                "_source": {
                    "country_name": "UNITED STATES",
                    "state_name": "CALIFORNIA",
                    "cities": [],
                    "counties": [],
                    "zip_codes": [],
                    "current_congressional_districts": [],
                    "original_congressional_districts": ["CA10", "CA11"],
                },
                "highlight": {"original_congressional_districts": ["CA10", "CA11"]},
            }
        ),
        Hit(
            {
                "_source": {
                    "country_name": "UNITED STATES",
                    "state_name": "TEXAS",
                    "cities": [],
                    "counties": [],
                    "zip_codes": [],
                    "current_congressional_districts": [],
                    "original_congressional_districts": ["TX20", "TX21"],
                },
                "highlight": {"original_congressional_districts": ["TX20", "TX21"]},
            }
        ),
    ]
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_original_cd_results(es_results=mock_es_data) == [
        {"original_cd": "CA-10", "state_name": "CALIFORNIA", "country_name": "UNITED STATES"},
        {"original_cd": "CA-11", "state_name": "CALIFORNIA", "country_name": "UNITED STATES"},
        {"original_cd": "TX-20", "state_name": "TEXAS", "country_name": "UNITED STATES"},
        {"original_cd": "TX-21", "state_name": "TEXAS", "country_name": "UNITED STATES"},
    ]

    # Test formatter returns None when hits are present
    mock_es_data_none: List = []
    location_viewset = LocationAutocompleteViewSet()

    assert location_viewset._format_original_cd_results(es_results=mock_es_data_none) is None
