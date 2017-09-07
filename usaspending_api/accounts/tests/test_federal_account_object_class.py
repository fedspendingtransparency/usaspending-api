import pytest

from usaspending_api.accounts.models import TreasuryAppropriationAccount, FederalAccount
from usaspending_api.financial_activities.models import FinancialAccountsByProgramActivityObjectClass
from usaspending_api.references.models import ObjectClass

from model_mommy import mommy
import json


@pytest.fixture
def federal_account_models():
    fed_1 = mommy.make(FederalAccount, id=1)
    tas_1 = mommy.make(TreasuryAppropriationAccount, federal_account = 1, treasury_account=1)
    tas_2 =  mommy.make(TreasuryAppropriationAccount, federal_account = 1, treasury_account=2)
    fabpaoc_1 = mommy.make(FinancialAccountsByProgramActivityObjectClass, treasury_account=1, object_class=1)
    fabpaoc_2 = mommy.make(FinancialAccountsByProgramActivityObjectClass, treasury_account=2, object_class=2)
    obj_class_1 = mommy.make(ObjectClass, major_object_class= '1', major_object_class_name='major', object_class='1', object_class_name='minor')
    obj_class_2 = mommy.make(ObjectClass, major_object_class= '2', major_object_class_name='major', object_class='2', object_class_name='minor')


@pytest.mark.django_db
def test_federal_account_list(federal_account_models, client):
    """
    Ensure the accounts endpoint lists the right number of entities
    """
    resp = client.get('/api/v2/federal_account/available_object_classes/1')
    assert resp.status_code == 200
    assert len(resp.data['results']) == 3
