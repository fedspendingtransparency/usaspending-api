# Stdlib imports
import json
import pytest
# Core Django imports

# Third-party app imports
from django_mock_queries.query import MockModel

# Imports from your apps
from usaspending_api.common.helpers.unit_test_helper import add_to_mock_objects


def get_new_awards_over_time_url():
    return '/api/v2/search/new_awards_over_time/'


@pytest.mark.skip
def test_new_awards_simple_case(mock_matviews_qs, client):
    mock_transaction_1 = MockModel(awarding_agency_id=2, awarding_toptier_agency_name='Department of Pizza',
                             awarding_toptier_agency_abbreviation='DOP', generated_pragmatic_obligation=5)
    mock_transaction_2 = MockModel(awarding_agency_id=3, awarding_toptier_agency_name='Department of Pizza',
                             awarding_toptier_agency_abbreviation='DOP', generated_pragmatic_obligation=10)

    add_to_mock_objects(mock_matviews_qs, [mock_transaction_1, mock_transaction_2])

    test_payload = {
        'category': 'awarding_agency',
        'subawards': False,
        'page': 1,
        'limit': 50
    }

    resp = client.post(
        get_new_awards_over_time_url(),
        content_type='application/json',
        data=json.dumps(test_payload)
    )
    assert resp.status_code == 200

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
    print(resp.__dict__)

    assert expected_response == resp.data


@pytest.mark.skip
def test_new_awards_more_data(mock_matviews_qs, client):
    assert True
