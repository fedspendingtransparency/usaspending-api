import json

import pytest
from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def financial_spending_data(db):

    # create Object classes
    mommy.make(
        'financial_activities.FinancialAccountsByProgramActivityObjectClass',
        program_activity_id=1,
        program_activity__id=1,
        program_activity__program_activity_code=100,
        program_activity__program_activity_name='cuttlefish training',
        object_class_id=1,
        object_class__id=1,
        object_class__object_class='001',
        object_class__object_class_name='Full-time permanent',
        obligations_incurred_by_program_object_class_cpe=1000000,
    )
    mommy.make(
        'financial_activities.FinancialAccountsByProgramActivityObjectClass',
        program_activity_id=1,
        program_activity__id=1,
        program_activity__program_activity_code=100,
        program_activity__program_activity_name='cuttlefish training',
        object_class_id=1,
        object_class__id=1,
        object_class__object_class='001',
        object_class__object_class_name='Full-time permanent',
        obligations_incurred_by_program_object_class_cpe=1000000,
    )
    mommy.make(
        'financial_activities.FinancialAccountsByProgramActivityObjectClass',
        program_activity_id=2,
        program_activity__id=2,
        program_activity__program_activity_code=200,
        program_activity__program_activity_name='metaphysical optimization',
        object_class_id=2,
        object_class__id=2,
        object_class__object_class='002',
        object_class__object_class_name='Other than full-time permanent',
        obligations_incurred_by_program_object_class_cpe=1000000,
    )
    mommy.make(
        'financial_activities.FinancialAccountsByProgramActivityObjectClass',
        program_activity_id=3,
        program_activity__id=3,
        program_activity__program_activity_code=300,
        program_activity__program_activity_name='phase shift',
        object_class_id=2,
        object_class__id=2,
        object_class__object_class='002',
        object_class__object_class_name='Other than full-time permanent',
        obligations_incurred_by_program_object_class_cpe=1000000,
    )


@pytest.mark.django_db
def test_federal_account_spending_by_category(client, financial_spending_data):
    """Test grouping over all available categories"""

    payload = {
        "category": "program_activity",
        "filters": {
            "object_classes": [{
                "major_object_class_name": "Personnel compensation and benefits",
                "object_class_names": [
                    "Full-time permanent",
                    "Other than full-time permanent",
                ]
            }, {
                "major_object_class_name": "Other"
            }],
            "program_activites": [1, 2, 3],
            "time_period": [{
                "start_date": "2001-01-01",
                "end_date": "2001-01-31"
            }]
        }
    }

    resp = client.post(
        '/api/v2/federal_accounts/1/spending_by_category', content_type='application/json', data=json.dumps(payload))
    assert resp.status_code == status.HTTP_200_OK

    # test response in correct form

    assert 'results' in resp.json()
    results = resp.json()['results']
    assert len(results)
    for (k, v) in results.items():
        assert isinstance(k, str)
        assert hasattr(v, '__pow__')  # is a number

    # test categorize by program_activity
    assert results['phase shift'] == 1000000
    assert results['cuttlefish training'] == 2000000
    assert results['metaphysical optimization'] == 1000000

    payload['category'] = 'object_class'
    resp = client.post(
        '/api/v2/federal_accounts/1/spending_by_category', content_type='application/json', data=json.dumps(payload))
    assert resp.status_code == status.HTTP_200_OK
    results = resp.json()['results']
    assert results['Full-time permanent'] == 2000000
    assert results['Other than full-time permanent'] == 2000000
