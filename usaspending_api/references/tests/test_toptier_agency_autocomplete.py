import json

import pytest
from model_mommy import mommy
from rest_framework import status

from usaspending_api.common.tests.autocomplete import check_autocomplete
from usaspending_api.references.models import Agency


@pytest.fixture
def agency_data(db):
    mommy.make(
        Agency,
        toptier_agency__name="Lunar Colonization Society",
        toptier_agency__cgac_code="LCS123",
        subtier_agency=None,
        toptier_flag=True,
        _fill_optional=True),
    mommy.make(
        Agency,
        toptier_agency__name="Cerean Mineral Extraction Corp.",
        toptier_agency__cgac_code="CMEC",
        subtier_agency=None,
        toptier_flag=True,
        _fill_optional=True),
    mommy.make(
        Agency,
        toptier_agency__name="Department of Transportation",
        subtier_agency__name="Department of Transportation",
        toptier_flag=True,
        _fill_optional=True)
    mommy.make(
        Agency,
        toptier_agency__name="Department of Defence",
        subtier_agency__name="Department of the Army",
        toptier_flag=False,
        _fill_optional=True)


@pytest.mark.django_db
def test_toptier_agency_autocomplete_success(client, agency_data):

    # test for exact match
    resp = client.post(
        '/api/v2/autocomplete/toptier_agency/',
        content_type='application/json',
        data=json.dumps({'search_text': 'Department of Transportation'}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 1
    assert resp.data['results'][0]['agency_name'] == 'Department of Transportation'

    # test for similar matches
    resp = client.post(
        '/api/v2/autocomplete/toptier_agency/',
        content_type='application/json',
        data=json.dumps({'search_text': 'department', 'limit': 3}))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 3

    # test closest match is at the top
    assert resp.data['results'][0]['agency_name'] == 'Department of Transportation'
    assert resp.data['results'][-1]['agency_name'] == 'Cerean Mineral Extraction Corp.'


@pytest.mark.django_db
def test_toptier_agency_autocomplete_failure(client):
    """Verify error on bad autocomplete request for awarding agency."""

    resp = client.post(
        '/api/v2/autocomplete/toptier_agency/',
        content_type='application/json',
        data=json.dumps({}))
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

