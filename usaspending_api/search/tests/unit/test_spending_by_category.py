# Stdlib imports
import pytest
# Core Django imports

# Third-party app imports
from django_mock_queries.query import MockModel

# Imports from your apps
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.search.v2.views.spending_by_category import BusinessLogic


def test_category_awarding_agency_scope_agency_awards(mock_matviews_qs):
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

    spending_by_category_logic = BusinessLogic(test_payload).results()

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


def test_category_awarding_agency_scope_agency_subawards(mock_matviews_qs):
    """This is testing that results are empty until implemented """
    mock_model_1 = MockModel(awarding_toptier_agency_name='Department of Pizza',
                             awarding_toptier_agency_abbreviation='DOP', amount=5)
    mock_model_2 = MockModel(awarding_toptier_agency_name='Department of Pizza',
                             awarding_toptier_agency_abbreviation='DOP', amount=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'awarding_agency',
        'scope': 'agency',
        'subawards': True,
        'page': 1,
        'limit': 50,
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

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
        'results': [{'agency_abbreviation': 'DOP', 'agency_name': 'Department of Pizza', 'aggregated_amount': 15}]
    }

    assert expected_response == spending_by_category_logic


def test_category_awarding_agency_scope_subagency_awards(mock_matviews_qs):
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

    spending_by_category_logic = BusinessLogic(test_payload).results()

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


def test_category_awarding_agency_scope_subagency_subawards(mock_matviews_qs):
    """This is testing that results are empty until implemented """
    mock_model_1 = MockModel(awarding_subtier_agency_name='Department of Pizza',
                             awarding_subtier_agency_abbreviation='DOP', amount=10)
    mock_model_2 = MockModel(awarding_subtier_agency_name='Department of Pizza',
                             awarding_subtier_agency_abbreviation='DOP', amount=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'awarding_agency',
        'scope': 'subagency',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

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
        'results': [{'agency_abbreviation': 'DOP', 'agency_name': 'Department of Pizza', 'aggregated_amount': 20}]
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_agency_scope_agency_awards(mock_matviews_qs):
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

    spending_by_category_logic = BusinessLogic(test_payload).results()

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


def test_category_funding_agency_scope_agency_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(funding_toptier_agency_name='Department of Calzone',
                             funding_toptier_agency_abbreviation='DOC', amount=50)
    mock_model_2 = MockModel(funding_toptier_agency_name='Department of Calzone',
                             funding_toptier_agency_abbreviation='DOC', amount=50)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'funding_agency',
        'scope': 'agency',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

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
        'results': [{'agency_abbreviation': 'DOC', 'agency_name': 'Department of Calzone', 'aggregated_amount': 100}]
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_agency_scope_subagency_awards(mock_matviews_qs):
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

    spending_by_category_logic = BusinessLogic(test_payload).results()

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


def test_category_funding_agency_scope_subagency_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(funding_subtier_agency_name='Department of Sub-calzone',
                             funding_subtier_agency_abbreviation='DOSC', amount=5)
    mock_model_2 = MockModel(funding_subtier_agency_name='Department of Sub-calzone',
                             funding_subtier_agency_abbreviation='DOSC', amount=-5)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'funding_agency',
        'scope': 'subagency',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

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
        'results': [{'agency_abbreviation': 'DOSC', 'agency_name': 'Department of Sub-calzone', 'aggregated_amount': 0}]
    }

    assert expected_response == spending_by_category_logic


def test_category_recipient_scope_duns_awards(mock_matviews_qs):
    mock_model_1 = MockModel(recipient_name='University of Pawnee',
                             recipient_unique_id='00UOP00', generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(recipient_name='University of Pawnee',
                             recipient_unique_id='00UOP00', generated_pragmatic_obligation=1)
    mock_model_3 = MockModel(recipient_name='John Doe',
                             recipient_unique_id='1234JD4321', generated_pragmatic_obligation=1)
    mock_model_4 = MockModel(recipient_name='John Doe',
                             recipient_unique_id='1234JD4321', generated_pragmatic_obligation=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])

    test_payload = {
        'category': 'recipient',
        'scope': 'duns',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'recipient',
        'scope': 'duns',
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
                'aggregated_amount': 11,
                'recipient_name': 'John Doe',
                'legal_entity_id': '1234JD4321'
            },
            {
                'aggregated_amount': 2,
                'recipient_name': 'University of Pawnee',
                'legal_entity_id': '00UOP00'
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_recipient_scope_duns_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(recipient_name='University of Pawnee',
                             recipient_unique_id='00UOP00', amount=1)
    mock_model_2 = MockModel(recipient_name='University of Pawnee',
                             recipient_unique_id='00UOP00', amount=1)
    mock_model_3 = MockModel(recipient_name='John Doe',
                             recipient_unique_id='1234JD4321', amount=1)
    mock_model_4 = MockModel(recipient_name='John Doe',
                             recipient_unique_id='1234JD4321', amount=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])

    test_payload = {
        'category': 'recipient',
        'scope': 'duns',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'recipient',
        'scope': 'duns',
        'limit': 50,
        'page_metadata': {
            'page': 1,
            'next': None,
            'previous': None,
            'hasNext': False,
            'hasPrevious': False
        },
        'results': [
            {'aggregated_amount': 11, 'legal_entity_id': '1234JD4321', 'recipient_name': 'John Doe'},
            {'aggregated_amount': 2, 'legal_entity_id': '00UOP00', 'recipient_name': 'University of Pawnee'}
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_recipient_scope_parent_duns_awards(mock_matviews_qs):
    mock_model_1 = MockModel(recipient_name='University of Pawnee',
                             parent_recipient_unique_id='00UOP00', generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(recipient_name='University of Pawnee',
                             parent_recipient_unique_id='00UOP00', generated_pragmatic_obligation=1)
    mock_model_3 = MockModel(recipient_name='John Doe',
                             parent_recipient_unique_id='1234JD4321', generated_pragmatic_obligation=1)
    mock_model_4 = MockModel(recipient_name='John Doe',
                             parent_recipient_unique_id='1234JD4321', generated_pragmatic_obligation=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])

    test_payload = {
        'category': 'recipient',
        'scope': 'parent_duns',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'recipient',
        'scope': 'parent_duns',
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
                'aggregated_amount': 11,
                'recipient_name': 'John Doe',
                'parent_recipient_unique_id': '1234JD4321'
            },
            {
                'aggregated_amount': 2,
                'recipient_name': 'University of Pawnee',
                'parent_recipient_unique_id': '00UOP00'
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_recipient_scope_parent_duns_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(recipient_name='University of Pawnee',
                             parent_recipient_unique_id='00UOP00', amount=1)
    mock_model_2 = MockModel(recipient_name='University of Pawnee',
                             parent_recipient_unique_id='00UOP00', amount=1)
    mock_model_3 = MockModel(recipient_name='John Doe',
                             parent_recipient_unique_id='1234JD4321', amount=1)
    mock_model_4 = MockModel(recipient_name='John Doe',
                             parent_recipient_unique_id='1234JD4321', amount=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])

    test_payload = {
        'category': 'recipient',
        'scope': 'parent_duns',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'recipient',
        'scope': 'parent_duns',
        'limit': 50,
        'page_metadata': {
            'page': 1,
            'next': None,
            'previous': None,
            'hasNext': False,
            'hasPrevious': False
        },
        'results': [
            {'aggregated_amount': 11, 'parent_recipient_unique_id': '1234JD4321', 'recipient_name': 'John Doe'},
            {'aggregated_amount': 2, 'parent_recipient_unique_id': '00UOP00', 'recipient_name': 'University of Pawnee'}
        ]
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.skip
def test_category_cfda_programs_scope_none_awards(mock_matviews_qs):
    mock_model_1 = MockModel(cfda_title='CFDA TITLE 1234', cfda_number='CFDA1234',
                             cfda_popular_name='POPULAR CFDA TITLE 1234', federal_action_obligation=1,
                             generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(cfda_title='CFDA TITLE 1234', cfda_number='CFDA1234',
                             cfda_popular_name='POPULAR CFDA TITLE 1234', federal_action_obligation=1,
                             generated_pragmatic_obligation=1)

    # mock_model_cfda = MockModel(program_title='CFDA TITLE 1234', program_number='CFDA1234',
    #                             popular_name='Popular Title for 1234')

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'cfda_programs',
        'scope': 'cfda',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'cfda_programs',
        'scope': None,
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
                'aggregated_amount': 2,
                'cfda_program_number': 'CFDA1234',
                'popular_name': 'POPULAR CFDA TITLE 1234',
                'popular_title': 'CFDA TITLE 1234'
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_cfda_programs_scope_none_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(cfda_title='CFDA TITLE 1234', cfda_number='CFDA1234',
                             cfda_popular_name='POPULAR CFDA TITLE 1234', amount=1)
    mock_model_2 = MockModel(cfda_title='CFDA TITLE 1234', cfda_number='CFDA1234',
                             cfda_popular_name='POPULAR CFDA TITLE 1234', amount=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'cfda_programs',
        'scope': 'cfda',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'cfda_programs',
        'scope': None,
        'limit': 50,
        'page_metadata': {
            'page': 1,
            'next': None,
            'previous': None,
            'hasNext': False,
            'hasPrevious': False
        },
        'results': [{
            'aggregated_amount': 2, 'cfda_program_number': 'CFDA1234',
            'popular_name': 'POPULAR CFDA TITLE 1234', 'popular_title': 'CFDA TITLE 1234'}]
    }

    assert expected_response == spending_by_category_logic


def test_category_industry_codes_scope_psc_awards(mock_matviews_qs):
    mock_model_1 = MockModel(product_or_service_code='PSC 1234', generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(product_or_service_code='PSC 1234', generated_pragmatic_obligation=1)
    mock_model_3 = MockModel(product_or_service_code='PSC 9876', generated_pragmatic_obligation=2)
    mock_model_4 = MockModel(product_or_service_code='PSC 9876', generated_pragmatic_obligation=2)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])

    test_payload = {
        'category': 'industry_codes',
        'scope': 'psc',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'industry_codes',
        'scope': 'psc',
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
                'aggregated_amount': 4,
                'psc_code': 'PSC 9876'
            },
            {
                'aggregated_amount': 2,
                'psc_code': 'PSC 1234'
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_industry_codes_scope_naics_awards(mock_matviews_qs):
    mock_model_1 = MockModel(naics_code='NAICS 1234', naics_description='NAICS DESC 1234',
                             generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(naics_code='NAICS 1234', naics_description='NAICS DESC 1234',
                             generated_pragmatic_obligation=1)
    mock_model_3 = MockModel(naics_code='NAICS 9876', naics_description='NAICS DESC 9876',
                             generated_pragmatic_obligation=2)
    mock_model_4 = MockModel(naics_code='NAICS 9876', naics_description='NAICS DESC 9876',
                             generated_pragmatic_obligation=2)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])

    test_payload = {
        'category': 'industry_codes',
        'scope': 'naics',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'industry_codes',
        'scope': 'naics',
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
                'aggregated_amount': 4,
                'naics_code': 'NAICS 9876',
                'naics_description': 'NAICS DESC 9876'
            },
            {
                'aggregated_amount': 2,
                'naics_code': 'NAICS 1234',
                'naics_description': 'NAICS DESC 1234'
            }
        ]
    }

    assert expected_response == spending_by_category_logic
