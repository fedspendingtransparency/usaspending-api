# Stdlib imports

# Core Django imports

# Third-party app imports
from django_mock_queries.query import MockModel

# Imports from your apps
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.search.v2.views.spending_by_category import BusinessLogic


def test_category_awarding_agency_scope_agency(mock_matviews_qs):
    mock_model_1 = MockModel(awarding_toptier_agency_name='Department of Pizza',
                             awarding_toptier_agency_abbreviation='DOP', generated_pragmatic_obligation=5)
    mock_model_2 = MockModel(awarding_toptier_agency_name='Department of Pizza',
                             awarding_toptier_agency_abbreviation='DOP', generated_pragmatic_obligation=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'awarding_agency',
        'scope': 'agency',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload)

    expected_response = {
        'category': 'awarding_agency',
        'scope': 'agency',
        'limit': 50,
        'page_metadata': {
            'page': 1,
            'next': None,
            'previous': None,
            'hasNext': False,
            'hasPrevious': False
        },
        'results': [
            {
                'aggregated_amount': 15,
                'agency_name': 'Department of Pizza',
                'agency_abbreviation': 'DOP'
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_awarding_agency_scope_subagency(mock_matviews_qs):
    mock_model_1 = MockModel(awarding_subtier_agency_name='Department of Sub-pizza',
                             awarding_subtier_agency_abbreviation='DOSP', generated_pragmatic_obligation=10)
    mock_model_2 = MockModel(awarding_subtier_agency_name='Department of Sub-pizza',
                             awarding_subtier_agency_abbreviation='DOSP', generated_pragmatic_obligation=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'awarding_agency',
        'scope': 'subagency',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload)

    expected_response = {
        'category': 'awarding_agency',
        'scope': 'subagency',
        'limit': 50,
        'page_metadata': {
            'page': 1,
            'next': None,
            'previous': None,
            'hasNext': False,
            'hasPrevious': False
        },
        'results': [
            {
                'aggregated_amount': 20,
                'agency_name': 'Department of Sub-pizza',
                'agency_abbreviation': 'DOSP'
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_agency_scope_agency(mock_matviews_qs):
    mock_model_1 = MockModel(funding_toptier_agency_name='Department of Calzone',
                             funding_toptier_agency_abbreviation='DOC', generated_pragmatic_obligation=50)
    mock_model_2 = MockModel(funding_toptier_agency_name='Department of Calzone',
                             funding_toptier_agency_abbreviation='DOC', generated_pragmatic_obligation=50)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'funding_agency',
        'scope': 'agency',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload)

    expected_response = {
        'category': 'funding_agency',
        'scope': 'agency',
        'limit': 50,
        'page_metadata': {
            'page': 1,
            'next': None,
            'previous': None,
            'hasNext': False,
            'hasPrevious': False
        },
        'results': [
            {
                'aggregated_amount': 100,
                'agency_name': 'Department of Calzone',
                'agency_abbreviation': 'DOC'
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_agency_scope_subagency(mock_matviews_qs):
    mock_model_1 = MockModel(funding_subtier_agency_name='Department of Sub-calzone',
                             funding_subtier_agency_abbreviation='DOSC', generated_pragmatic_obligation=5)
    mock_model_2 = MockModel(funding_subtier_agency_name='Department of Sub-calzone',
                             funding_subtier_agency_abbreviation='DOSC', generated_pragmatic_obligation=-5)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'funding_agency',
        'scope': 'subagency',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload)

    expected_response = {
        'category': 'funding_agency',
        'scope': 'subagency',
        'limit': 50,
        'page_metadata': {
            'page': 1,
            'next': None,
            'previous': None,
            'hasNext': False,
            'hasPrevious': False
        },
        'results': [
            {
                'aggregated_amount': 0,
                'agency_name': 'Department of Sub-calzone',
                'agency_abbreviation': 'DOSC'
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_recipient_scope_duns(mock_matviews_qs):
    pass


def test_category_recipient_scope_parent_duns(mock_matviews_qs):
    pass


def test_category_cfda_programs_scope_none(mock_matviews_qs):
    pass


def test_category_industry_codes_scope_psc(mock_matviews_qs):
    pass


def test_category_industry_codes_scope_naics(mock_matviews_qs):
    pass
