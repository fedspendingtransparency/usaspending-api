import json
import pytest

from model_mommy import mommy
from rest_framework import status

from usaspending_api.download.lookups import JOB_STATUS


@pytest.fixture
def award_data(db):
    for js in JOB_STATUS:
        mommy.make('download.JobStatus', job_status_id=js.id, name=js.name, description=js.desc)

    mommy.make(
        'awards.ParentAward',
        award_id=1
    )

    mommy.make(
        'awards.Award',
        id=1,
        category='contracts',
        piid='piid1',
        fpds_agency_id=123,
        type='IDV_A'
    )

    mommy.make(
        'awards.Award',
        id=2,
        category='contracts',
        piid='piid2',
        fpds_agency_id=123,
        fpds_parent_agency_id=123,
        parent_award_piid='piid1',
        type='A'
    )


@pytest.mark.django_db
def test_download_idv_endpoint(client, award_data):
    resp = client.post(
        '/api/v2/download/idv',
        content_type='application/json',
        data=json.dumps({"award_id": 1}))

    assert resp.status_code == status.HTTP_200_OK
    assert '.zip' in resp.json()['url']
