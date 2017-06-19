import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency, ToptierAgency, SubtierAgency


@pytest.fixture
def financial_spending_data(db):
    # Create agency - submission relationship
    # Create AGENCY AND TopTier AGENCY
    ttagency1 = mommy.make('references.ToptierAgency', name="tta_name", cgac_code='100')
    mommy.make('references.Agency', id=1, toptier_agency=ttagency1)

    # CREATE SUBMISSIONS
    submission_3 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2015, cgac_code='100')
    submission_1 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2017, cgac_code='100')
    submission_2 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2016, cgac_code='100')


@pytest.mark.django_db
def test_award_type_endpoint(client, financial_spending_data):
    """Test the award_type endpoint."""

    resp = client.get('/api/v2/references/agency/1/')
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data == {'results': {'agency_name': 'tta_name', 'active_fy': '2017'}}

    # check for bad request due to missing params
    resp = client.get('/api/v2/references/agency/4/')
    assert resp.data == {'results': {}}
