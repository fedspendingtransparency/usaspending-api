from datetime import date
import json

from model_mommy import mommy
import pytest
from rest_framework import status

from usaspending_api.awards.models import Transaction, TransactionAssistance, TransactionContract

@pytest.fixture
def award_data(db):
    # Create Locations
    loc1 = mommy.make('references.Location')

    # Create LE
    le1 = mommy.make('references.LegalEntity')

    # Create Awarding Top Agency
    ata1 = mommy.make('references.ToptierAgency', name="tta_name", cgac_code='100', website='http://test.com',
                           mission='test', icon_filename='test')

    # Create Awarding sub
    asa1 = mommy.make('references.SubtierAgency', name="tta_name")

    # Create Awarding Agency
    aa1 = mommy.make('references.Agency', id=1, toptier_flag=False)

    # Create Funding Top Agency
    fta = mommy.make('references.ToptierAgency', name="tta_name", cgac_code='100', website='http://test.com',
                           mission='test', icon_filename='test')

    # Create Funding SUB
    fsa1 = mommy.make('references.SubtierAgency', name="tta_name")

    # Create Funding Agency
    fa1 = mommy.make('references.Agency', id=1, toptier_flag=False)

    # Create Award
    award = mommy.make('awards.Award', category='contracts', awarding_agency=aa1)

    # Create Transaction
    tran1 = mommy.make(Transaction)

    # Create TransactionContract
    tc1 = mommy.make(TransactionContract, transaction=tran1)

    # Create TransactionAssistance
    ta1 = mommy.make(TransactionAssistance, transaction=tran1)



@pytest.mark.django_db
def test_download_transactions_v2_endpoint(client, award_data):
    """Test the transaction endpoint."""

    assert client.post(
        '/api/v2/download/transactions',
        content_type='application/json',
        data=json.dumps({
            "filters": {},
            "columns": {}
        })).status_code == status.HTTP_200_OK


