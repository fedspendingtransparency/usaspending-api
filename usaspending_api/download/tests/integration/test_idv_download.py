import json
import pytest
import random

from django.conf import settings
from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import TransactionNormalized, TransactionFABS, TransactionFPDS
from usaspending_api.awards.v2.lookups.lookups import award_type_mapping
from usaspending_api.download.lookups import JOB_STATUS
from usaspending_api.etl.award_helpers import update_awards


@pytest.fixture
def award_data(db):
    # Populate job status lookup table
    for js in JOB_STATUS:
        mommy.make('download.JobStatus', job_status_id=js.id, name=js.name, description=js.desc)

    # Create Locations
    mommy.make('references.Location')

    # Create LE
    mommy.make('references.LegalEntity')

    # Create Awarding Top Agency
    ata1 = mommy.make(
        'references.ToptierAgency',
        name="Bureau of Things",
        cgac_code='100',
        website='http://test.com',
        mission='test',
        icon_filename='test')
    ata2 = mommy.make(
        'references.ToptierAgency',
        name="Bureau of Stuff",
        cgac_code='101',
        website='http://test.com',
        mission='test',
        icon_filename='test')

    # Create Awarding subs
    mommy.make('references.SubtierAgency', name="Bureau of Things")

    # Create Awarding Agencies
    aa1 = mommy.make('references.Agency', id=1, toptier_agency=ata1, toptier_flag=False)
    aa2 = mommy.make('references.Agency', id=2, toptier_agency=ata2, toptier_flag=False)

    # Create Funding Top Agency
    mommy.make(
        'references.ToptierAgency',
        name="Bureau of Money",
        cgac_code='102',
        website='http://test.com',
        mission='test',
        icon_filename='test')

    # Create Funding SUB
    mommy.make('references.SubtierAgency', name="Bureau of Things")

    # Create Funding Agency
    mommy.make('references.Agency', id=3, toptier_flag=False)

    # Create Awards
    award1 = mommy.make('awards.Award', id=1, category='contracts', type='IDV_A', piid='ABC', fpds_agency_id=123)
    award2 = mommy.make(
        'awards.Award', id=2, category='contracts', type='A', parent_award_piid='ABC', fpds_parent_agency_id=123)
    award3 = mommy.make('awards.Award', id=3, category='assistance')

    # Create Transactions
    trann1 = mommy.make(
        TransactionNormalized,
        award=award1,
        action_date='2018-01-01',
        type='IDV_A',
        modification_number=1,
        awarding_agency=aa1)
    trann2 = mommy.make(
        TransactionNormalized,
        award=award2,
        action_date='2018-01-01',
        type='A',
        modification_number=1,
        awarding_agency=aa2)
    trann3 = mommy.make(
        TransactionNormalized,
        award=award3,
        action_date='2018-01-01',
        type=random.choice(list(award_type_mapping)),
        modification_number=1,
        awarding_agency=aa2)

    # Create TransactionContract
    mommy.make(TransactionFPDS, transaction=trann1, piid='tc1piid')
    mommy.make(TransactionFPDS, transaction=trann2, piid='tc2piid')

    # Create TransactionAssistance
    mommy.make(TransactionFABS, transaction=trann3, fain='ta1fain')

    # Set latest_award for each award
    update_awards()


# TODO: FINISH THIS
@pytest.mark.django_db
@pytest.mark.skip
def test_download_idv_endpoint(client, award_data):
    resp = client.post(
        '/api/v2/download/idv',
        content_type='application/json',
        data=json.dumps({"award_id": 1}))

    print(resp.json())
    assert resp.status_code == status.HTTP_200_OK
    assert '.zip' in resp.json()['url']
