import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency, ToptierAgency, SubtierAgency


@pytest.fixture
def financial_spending_data(db):
    # federal Account
    federal_account_1 = mommy.make('accounts.FederalAccount', id=1)

    # create Object classes
    object_class_1 = mommy.make('references.ObjectClass', major_object_class="10",
                                major_object_class_name="mocName1", object_class="111", object_class_name="ocName1")
    object_class_2 = mommy.make('references.ObjectClass', major_object_class="20",
                                major_object_class_name="mocName2", object_class="222", object_class_name="ocName2")
    object_class_3 = mommy.make('references.ObjectClass', major_object_class="30",
                                major_object_class_name="mocName3", object_class="333", object_class_name="ocName3")
    object_class_4 = mommy.make('references.ObjectClass', major_object_class="20",
                                major_object_class_name="mocName2", object_class="444", object_class_name="ocName4")

    # create TAS
    tas = mommy.make('accounts.TreasuryAppropriationAccount', federal_account=federal_account_1)
    tas2 = mommy.make('accounts.TreasuryAppropriationAccount', federal_account=federal_account_1)

    # CREATE Financial account by program activity object class
    fabpaoc = mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', treasury_account=tas,
                         object_class=object_class_1)
    fabpaoc2 = mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', treasury_account=tas,
                          object_class=object_class_2)
    fabpaoc3 = mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', treasury_account=tas,
                          object_class=object_class_4)


@pytest.mark.django_db
def test_federal_account_object_class_endpoint(client, financial_spending_data):
    """Test the award_type endpoint."""

    resp = client.get('/api/v2/federal_accounts/object_class/1/')
    assert resp.status_code == status.HTTP_200_OK
    assert (resp.data == {'results': [{'major_object_class': 'mocName1', 'minor_object_class': ['ocName1']},
                                      {'major_object_class': 'mocName2', 'minor_object_class':
                                          ['ocName2', 'ocName4']}]}) or \
           (resp.data == {'results': [{'major_object_class': 'mocName2', 'minor_object_class': ['ocName2', 'ocName4']},
                                      {'major_object_class': 'mocName1', 'minor_object_class': ['ocName1']}]})

    # check for bad request due to missing params
    resp = client.get('/api/v2/federal_accounts/object_class/2/')
    assert resp.data == {'results': {}}
