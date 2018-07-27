# Stdlib imports
import pytest
# Core Django imports

# Third-party app imports
from django_mock_queries.query import MockModel
from model_mommy import mommy

# Imports from your apps
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects
from usaspending_api.search.v2.views.spending_by_category import BusinessLogic


def test_category_awarding_agency_awards(mock_matviews_qs, mock_agencies):
    mock_toptier = MockModel(toptier_agency_id=1, name='Department of Pizza', abbreviation='DOP')
    mock_agency = MockModel(id=2, toptier_agency=mock_toptier, toptier_flag=True)
    mock_agency_1 = MockModel(id=3, toptier_agency=mock_toptier, toptier_flag=False)
    mock_model_1 = MockModel(awarding_agency_id=2, awarding_toptier_agency_name='Department of Pizza',
                             awarding_toptier_agency_abbreviation='DOP', generated_pragmatic_obligation=5)
    mock_model_2 = MockModel(awarding_agency_id=3, awarding_toptier_agency_name='Department of Pizza',
                             awarding_toptier_agency_abbreviation='DOP', generated_pragmatic_obligation=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies['agency'], [mock_agency, mock_agency_1])
    add_to_mock_objects(mock_agencies['toptier_agency'], [mock_toptier])

    test_payload = {
        'category': 'awarding_agency',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'awarding_agency',
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
                'amount': 15,
                'name': 'Department of Pizza',
                'code': 'DOP',
                'id': 2
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_awarding_agency_subawards(mock_matviews_qs, mock_agencies):
    mock_toptier = MockModel(toptier_agency_id=1, name='Department of Pizza', abbreviation='DOP')
    mock_agency = MockModel(id=2, toptier_agency=mock_toptier, toptier_flag=True)
    mock_agency_1 = MockModel(id=3, toptier_agency=mock_toptier, toptier_flag=False)
    mock_model_1 = MockModel(awarding_agency_id=2, awarding_toptier_agency_name='Department of Pizza',
                             awarding_toptier_agency_abbreviation='DOP', amount=5)
    mock_model_2 = MockModel(awarding_agency_id=3, awarding_toptier_agency_name='Department of Pizza',
                             awarding_toptier_agency_abbreviation='DOP', amount=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies['agency'], [mock_agency, mock_agency_1])

    test_payload = {
        'category': 'awarding_agency',
        'subawards': True,
        'page': 1,
        'limit': 50,
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'awarding_agency',
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
                'amount': 15,
                'name': 'Department of Pizza',
                'code': 'DOP',
                'id': 2
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_awarding_subagency_awards(mock_matviews_qs, mock_agencies):
    mock_subtier = MockModel(subtier_agency_id=1, name='Department of Sub-pizza', abbreviation='DOSP')
    mock_agency = MockModel(id=2, subtier_agency=mock_subtier, toptier_flag=False)
    mock_agency_1 = MockModel(id=3, subtier_agency=mock_subtier, toptier_flag=True)
    mock_model_1 = MockModel(awarding_agency_id=2, awarding_subtier_agency_name='Department of Sub-pizza',
                             awarding_subtier_agency_abbreviation='DOSP', generated_pragmatic_obligation=10)
    mock_model_2 = MockModel(awarding_agency_id=3, awarding_subtier_agency_name='Department of Sub-pizza',
                             awarding_subtier_agency_abbreviation='DOSP', generated_pragmatic_obligation=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies['agency'], [mock_agency, mock_agency_1])

    test_payload = {
        'category': 'awarding_subagency',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'awarding_subagency',
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
                'amount': 20,
                'name': 'Department of Sub-pizza',
                'code': 'DOSP',
                'id': 2
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_awarding_subagency_subawards(mock_matviews_qs, mock_agencies):
    mock_subtier = MockModel(subtier_agency_id=1, name='Department of Sub-pizza', abbreviation='DOSP')
    mock_agency = MockModel(id=2, subtier_agency=mock_subtier, toptier_flag=False)
    mock_agency_1 = MockModel(id=3, subtier_agency=mock_subtier, toptier_flag=True)
    mock_model_1 = MockModel(awarding_agency_id=2, awarding_subtier_agency_name='Department of Sub-pizza',
                             awarding_subtier_agency_abbreviation='DOSP', amount=10)
    mock_model_2 = MockModel(awarding_agency_id=3, awarding_subtier_agency_name='Department of Sub-pizza',
                             awarding_subtier_agency_abbreviation='DOSP', amount=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies['agency'], [mock_agency, mock_agency_1])

    test_payload = {
        'category': 'awarding_subagency',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'awarding_subagency',
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
                'amount': 20,
                'name': 'Department of Sub-pizza',
                'code': 'DOSP',
                'id': 2
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_agency_awards(mock_matviews_qs, mock_agencies):
    mock_toptier = MockModel(toptier_agency_id=1, name='Department of Calzone', abbreviation='DOC')
    mock_agency = MockModel(id=2, toptier_agency=mock_toptier, toptier_flag=True)
    mock_agency_1 = MockModel(id=3, toptier_agency=mock_toptier, toptier_flag=False)
    mock_model_1 = MockModel(funding_agency_id=2, funding_toptier_agency_name='Department of Calzone',
                             funding_toptier_agency_abbreviation='DOC', generated_pragmatic_obligation=50)
    mock_model_2 = MockModel(funding_agency_id=3, funding_toptier_agency_name='Department of Calzone',
                             funding_toptier_agency_abbreviation='DOC', generated_pragmatic_obligation=50)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies['agency'], [mock_agency, mock_agency_1])

    test_payload = {
        'category': 'funding_agency',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'funding_agency',
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
                'amount': 100,
                'name': 'Department of Calzone',
                'code': 'DOC',
                'id': 2
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_agency_subawards(mock_matviews_qs, mock_agencies):
    mock_toptier = MockModel(toptier_agency_id=1, name='Department of Calzone', abbreviation='DOC')
    mock_agency = MockModel(id=2, toptier_agency=mock_toptier, toptier_flag=True)
    mock_agency_1 = MockModel(id=3, toptier_agency=mock_toptier, toptier_flag=False)
    mock_model_1 = MockModel(funding_agency_id=2, funding_toptier_agency_name='Department of Calzone',
                             funding_toptier_agency_abbreviation='DOC', amount=50)
    mock_model_2 = MockModel(funding_agency_id=3, funding_toptier_agency_name='Department of Calzone',
                             funding_toptier_agency_abbreviation='DOC', amount=50)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies['agency'], [mock_agency, mock_agency_1])

    test_payload = {
        'category': 'funding_agency',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'funding_agency',
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
                'amount': 100,
                'name': 'Department of Calzone',
                'code': 'DOC',
                'id': 2
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_subagency_awards(mock_matviews_qs, mock_agencies):
    mock_subtier = MockModel(subtier_agency_id=1, name='Department of Sub-calzone', abbreviation='DOSC')
    mock_agency = MockModel(id=2, subtier_agency=mock_subtier, toptier_flag=False)
    mock_agency_1 = MockModel(id=3, subtier_agency=mock_subtier, toptier_flag=True)
    mock_model_1 = MockModel(funding_agency_id=2, funding_subtier_agency_name='Department of Sub-calzone',
                             funding_subtier_agency_abbreviation='DOSC', generated_pragmatic_obligation=5)
    mock_model_2 = MockModel(funding_agency_id=3, funding_subtier_agency_name='Department of Sub-calzone',
                             funding_subtier_agency_abbreviation='DOSC', generated_pragmatic_obligation=-5)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies['agency'], [mock_agency, mock_agency_1])

    test_payload = {
        'category': 'funding_subagency',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'funding_subagency',
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
                'amount': 0,
                'name': 'Department of Sub-calzone',
                'code': 'DOSC',
                'id': 2
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_funding_subagency_subawards(mock_matviews_qs, mock_agencies):
    mock_subtier = MockModel(subtier_agency_id=1, name='Department of Sub-calzone', abbreviation='DOSC')
    mock_agency = MockModel(id=2, subtier_agency=mock_subtier, toptier_flag=False)
    mock_agency_1 = MockModel(id=3, subtier_agency=mock_subtier, toptier_flag=True)
    mock_model_1 = MockModel(funding_agency_id=2, funding_subtier_agency_name='Department of Sub-calzone',
                             funding_subtier_agency_abbreviation='DOSC', amount=5)
    mock_model_2 = MockModel(funding_agency_id=3, funding_subtier_agency_name='Department of Sub-calzone',
                             funding_subtier_agency_abbreviation='DOSC', amount=-5)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_agencies['agency'], [mock_agency, mock_agency_1])

    test_payload = {
        'category': 'funding_subagency',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'funding_subagency',
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
                'amount': 0,
                'name': 'Department of Sub-calzone',
                'code': 'DOSC',
                'id': 2
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_recipient_duns_awards(mock_matviews_qs, mock_reference_matviews):
    # recipient_hash = SELECT MD5(UPPER(CONCAT('<duns>','<recipient_name>')))::uuid;
    mock_model_1 = MockModel(recipient_hash='59f9a646-cd1c-cbdc-63dd-1020fac59336', generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(recipient_hash='59f9a646-cd1c-cbdc-63dd-1020fac59336', generated_pragmatic_obligation=1)
    mock_model_3 = MockModel(recipient_hash='3725ba78-a607-7ab4-1cf6-2a08207bac3c', generated_pragmatic_obligation=1)
    mock_model_4 = MockModel(recipient_hash='3725ba78-a607-7ab4-1cf6-2a08207bac3c', generated_pragmatic_obligation=10)
    mock_model_5 = MockModel(recipient_hash='18569a71-3b0a-1586-50a9-cbb8bb070136', generated_pragmatic_obligation=15)

    mock_recipients_1 = MockModel(recipient_hash='3725ba78-a607-7ab4-1cf6-2a08207bac3c',
                                  legal_business_name='John Doe', duns='1234JD4321')
    mock_recipients_2 = MockModel(recipient_hash='59f9a646-cd1c-cbdc-63dd-1020fac59336',
                                  legal_business_name='University of Pawnee', duns='00UOP00')
    mock_recipients_3 = MockModel(recipient_hash='18569a71-3b0a-1586-50a9-cbb8bb070136',
                                  legal_business_name='MULTIPLE RECIPIENTS', duns=None)
    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4, mock_model_5])
    add_to_mock_objects(mock_reference_matviews, [mock_recipients_1, mock_recipients_2, mock_recipients_3])

    test_payload = {
        'category': 'recipient_duns',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'recipient_duns',
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
                'amount': 15,
                'name': 'MULTIPLE RECIPIENTS',
                'code': None,
                'id': None
            },
            {
                'amount': 11,
                'name': 'John Doe',
                'code': '1234JD4321',
                'id': None
            },
            {
                'amount': 2,
                'name': 'University of Pawnee',
                'code': '00UOP00',
                'id': None
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_recipient_duns_subawards(mock_matviews_qs, mock_recipients):
    mock_recipient_1 = MockModel(recipient_unique_id='00UOP00', legal_entity_id=1)
    mock_recipient_2 = MockModel(recipient_unique_id='1234JD4321', legal_entity_id=2)
    mock_recipient_3 = MockModel(recipient_name='MULTIPLE RECIPIENTS', recipient_unique_id=None, legal_entity_id=3)
    mock_model_1 = MockModel(recipient_name='University of Pawnee',
                             recipient_unique_id='00UOP00', amount=1)
    mock_model_2 = MockModel(recipient_name='University of Pawnee',
                             recipient_unique_id='00UOP00', amount=1)
    mock_model_3 = MockModel(recipient_name='John Doe',
                             recipient_unique_id='1234JD4321', amount=1)
    mock_model_4 = MockModel(recipient_name='John Doe',
                             recipient_unique_id='1234JD4321', amount=10)
    mock_model_5 = MockModel(recipient_name='MULTIPLE RECIPIENTS',
                             recipient_unique_id=None, amount=15)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4, mock_model_5])
    add_to_mock_objects(mock_recipients, [mock_recipient_1, mock_recipient_2, mock_recipient_3])

    test_payload = {
        'category': 'recipient_duns',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'recipient_duns',
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
                'amount': 15,
                'name': 'MULTIPLE RECIPIENTS',
                'code': None,
                'id': None
            },
            {
                'amount': 11,
                'name': 'John Doe',
                'code': '1234JD4321',
                'id': None
            },
            {
                'amount': 2,
                'name': 'University of Pawnee',
                'code': '00UOP00',
                'id': None
            }
        ]
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.skip(reason="Currently not supporting recipient parent duns")
def test_category_recipient_parent_duns_awards(mock_matviews_qs, mock_recipients):
    mock_recipient_1 = MockModel(recipient_unique_id='00UOP00', legal_entity_id=1)
    mock_recipient_2 = MockModel(recipient_unique_id='1234JD4321', legal_entity_id=2)
    mock_model_1 = MockModel(recipient_name='University of Pawnee',
                             parent_recipient_unique_id='00UOP00', generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(recipient_name='University of Pawnee',
                             parent_recipient_unique_id='00UOP00', generated_pragmatic_obligation=1)
    mock_model_3 = MockModel(recipient_name='John Doe',
                             parent_recipient_unique_id='1234JD4321', generated_pragmatic_obligation=1)
    mock_model_4 = MockModel(recipient_name='John Doe',
                             parent_recipient_unique_id='1234JD4321', generated_pragmatic_obligation=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])
    add_to_mock_objects(mock_recipients, [mock_recipient_1, mock_recipient_2])

    test_payload = {
        'category': 'recipient_parent_duns',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'recipient_parent_duns',
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
                'amount': 11,
                'name': 'John Doe',
                'code': '1234JD4321',
                'id': 2,
            },
            {
                'amount': 2,
                'name': 'University of Pawnee',
                'code': '00UOP00',
                'id': 1,
            }
        ]
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.skip(reason="Currently not supporting recipient parent duns")
def test_category_recipient_parent_duns_subawards(mock_matviews_qs, mock_recipients):
    mock_recipient_1 = MockModel(recipient_unique_id='00UOP00', legal_entity_id=1)
    mock_recipient_2 = MockModel(recipient_unique_id='1234JD4321', legal_entity_id=2)
    mock_model_1 = MockModel(recipient_name='University of Pawnee',
                             parent_recipient_unique_id='00UOP00', amount=1)
    mock_model_2 = MockModel(recipient_name='University of Pawnee',
                             parent_recipient_unique_id='00UOP00', amount=1)
    mock_model_3 = MockModel(recipient_name='John Doe',
                             parent_recipient_unique_id='1234JD4321', amount=1)
    mock_model_4 = MockModel(recipient_name='John Doe',
                             parent_recipient_unique_id='1234JD4321', amount=10)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])
    add_to_mock_objects(mock_recipients, [mock_recipient_1, mock_recipient_2])

    test_payload = {
        'category': 'recipient_parent_duns',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'recipient_parent_duns',
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
                'amount': 11,
                'name': 'John Doe',
                'code': '1234JD4321',
                'id': 2,
            },
            {
                'amount': 2,
                'name': 'University of Pawnee',
                'code': '00UOP00',
                'id': 1,
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_cfda_awards(mock_matviews_qs, mock_cfda):
    mock_model_cfda = MockModel(program_title='CFDA TITLE 1234', program_number='CFDA1234', id=1)
    mock_model_1 = MockModel(cfda_number='CFDA1234', generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(cfda_number='CFDA1234', generated_pragmatic_obligation=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_cfda, [mock_model_cfda])

    test_payload = {
        'category': 'cfda',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'cfda',
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
                'amount': 2,
                'code': 'CFDA1234',
                'name': 'CFDA TITLE 1234',
                'id': 1
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_cfda_subawards(mock_matviews_qs, mock_cfda):
    mock_model_cfda = MockModel(program_title='CFDA TITLE 1234', program_number='CFDA1234', id=1)
    mock_model_1 = MockModel(cfda_number='CFDA1234', amount=1)
    mock_model_2 = MockModel(cfda_number='CFDA1234', amount=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])
    add_to_mock_objects(mock_cfda, [mock_model_cfda])

    test_payload = {
        'category': 'cfda',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'cfda',
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
                'amount': 2,
                'code': 'CFDA1234',
                'name': 'CFDA TITLE 1234',
                'id': 1
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_psc_awards(mock_matviews_qs, mock_psc):
    mock_psc_1 = MockModel(code='PSC 1234', description='PSC DESCRIPTION UP')
    mock_psc_2 = MockModel(code='PSC 9876', description='PSC DESCRIPTION DOWN')
    mock_model_1 = MockModel(product_or_service_code='PSC 1234', generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(product_or_service_code='PSC 1234', generated_pragmatic_obligation=1)
    mock_model_3 = MockModel(product_or_service_code='PSC 9876', generated_pragmatic_obligation=2)
    mock_model_4 = MockModel(product_or_service_code='PSC 9876', generated_pragmatic_obligation=2)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])
    add_to_mock_objects(mock_psc, [mock_psc_1, mock_psc_2])

    test_payload = {
        'category': 'psc',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'psc',
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
                'amount': 4,
                'code': 'PSC 9876',
                'id': None,
                'name': 'PSC DESCRIPTION DOWN',
            },
            {
                'amount': 2,
                'code': 'PSC 1234',
                'id': None,
                'name': 'PSC DESCRIPTION UP'

            }
        ]
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.skip(reason="Currently not supporting psc subawards")
def test_category_psc_subawards(mock_matviews_qs, mock_psc):
    mock_psc_1 = MockModel(code='PSC 1234', description='PSC DESCRIPTION UP')
    mock_psc_2 = MockModel(code='PSC 9876', description='PSC DESCRIPTION DOWN')
    mock_model_1 = MockModel(product_or_service_code='PSC 1234', amount=1)
    mock_model_2 = MockModel(product_or_service_code='PSC 1234', amount=1)
    mock_model_3 = MockModel(product_or_service_code='PSC 9876', amount=2)
    mock_model_4 = MockModel(product_or_service_code='PSC 9876', amount=2)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2, mock_model_3, mock_model_4])
    add_to_mock_objects(mock_psc, [mock_psc_1, mock_psc_2])

    test_payload = {
        'category': 'psc',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'psc',
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
                'amount': 4,
                'code': 'PSC 9876',
                'id': None,
                'name': 'PSC DESCRIPTION DOWN',
            },
            {
                'amount': 2,
                'code': 'PSC 1234',
                'id': None,
                'name': 'PSC DESCRIPTION UP'

            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_naics_awards(mock_matviews_qs):
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
        'category': 'naics',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'naics',
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
                'amount': 4,
                'code': 'NAICS 9876',
                'name': 'NAICS DESC 9876',
                'id': None
            },
            {
                'amount': 2,
                'code': 'NAICS 1234',
                'name': 'NAICS DESC 1234',
                'id': None
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_county_awards(mock_matviews_qs):
    mock_model_1 = MockModel(pop_county_code='04', pop_county_name='COUNTYSVILLE',
                             generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(pop_county_code='04', pop_county_name='COUNTYSVILLE',
                             generated_pragmatic_obligation=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'county',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'county',
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
                'amount': 2,
                'code': '04',
                'name': 'COUNTYSVILLE',
                'id': None
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_county_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(pop_county_code='04', pop_county_name='COUNTYSVILLE',
                             amount=1)
    mock_model_2 = MockModel(pop_county_code='04', pop_county_name='COUNTYSVILLE',
                             amount=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'county',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'county',
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
                'amount': 2,
                'code': '04',
                'name': 'COUNTYSVILLE',
                'id': None
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_district_awards(mock_matviews_qs):
    mock_model_1 = MockModel(pop_congressional_code='06', pop_state_code='XY',
                             generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(pop_congressional_code='06', pop_state_code='XY',
                             generated_pragmatic_obligation=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'district',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'district',
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
                'amount': 2,
                'code': '06',
                'name': 'XY-06',
                'id': None
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_district_awards_multiple_districts(mock_matviews_qs):
    mock_model_1 = MockModel(pop_congressional_code='90', pop_state_code='XY',
                             generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(pop_congressional_code='90', pop_state_code='XY',
                             generated_pragmatic_obligation=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'district',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'district',
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
                'amount': 2,
                'code': '90',
                'name': 'XY-MULTIPLE DISTRICTS',
                'id': None
            }
        ]
    }

    assert expected_response == spending_by_category_logic


def test_category_district_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(pop_congressional_code='06', pop_state_code='XY',
                             amount=1)
    mock_model_2 = MockModel(pop_congressional_code='06', pop_state_code='XY',
                             amount=1)

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'district',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'district',
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
                'amount': 2,
                'code': '06',
                'name': 'XY-06',
                'id': None
            }
        ]
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_state_territory(mock_matviews_qs):
    mock_model_1 = MockModel(pop_state_code='XY', generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(pop_state_code='XY', generated_pragmatic_obligation=1)
    mommy.make(
        'recipient.StateData',
        name='Test State',
        code='XY',
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'state/territory',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'state/territory',
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
                'amount': 2,
                'code': 'XY',
                'name': 'Test State',
                'id': None
            }
        ]
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_state_territory_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(pop_state_code='XY', amount=1)
    mock_model_2 = MockModel(pop_state_code='XY', amount=1)
    mommy.make(
        'recipient.StateData',
        name='Test State',
        code='XY',
    )

    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'state/territory',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'state/territory',
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
                'amount': 2,
                'code': 'XY',
                'name': 'Test State',
                'id': None
            }
        ]
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_country(mock_matviews_qs):
    mock_model_1 = MockModel(pop_country_code='US', generated_pragmatic_obligation=1)
    mock_model_2 = MockModel(pop_country_code='US', generated_pragmatic_obligation=1)
    mommy.make(
        'references.RefCountryCode',
        country_name='UNITED STATES',
        country_code='US',
    )
    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'country',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'country',
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
                'amount': 2,
                'code': 'US',
                'name': 'UNITED STATES',
                'id': None
            }
        ]
    }

    assert expected_response == spending_by_category_logic


@pytest.mark.django_db
def test_category_country_subawards(mock_matviews_qs):
    mock_model_1 = MockModel(pop_country_code='US', amount=1)
    mock_model_2 = MockModel(pop_country_code='US', amount=1)
    mommy.make(
        'references.RefCountryCode',
        country_name='UNITED STATES',
        country_code='US',
    )
    add_to_mock_objects(mock_matviews_qs, [mock_model_1, mock_model_2])

    test_payload = {
        'category': 'country',
        'subawards': True,
        'page': 1,
        'limit': 50
    }

    spending_by_category_logic = BusinessLogic(test_payload).results()

    expected_response = {
        'category': 'country',
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
                'amount': 2,
                'code': 'US',
                'name': 'UNITED STATES',
                'id': None
            }
        ]
    }

    assert expected_response == spending_by_category_logic
