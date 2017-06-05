import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency, ToptierAgency, SubtierAgency


# @pytest.fixture
# def award_spending_data(db):
#     agency1 = mommy.make('references.Agency', id=111)
#     awd1 = mommy.make('awards.Award', category='grants', awarding_agency=agency1)
#     mommy.make(
#         'awards.Transaction',
#         award=awd1,
#         awarding_agency=agency1,
#         federal_action_obligation=10,
#         fiscal_year=2017
#     )

@pytest.fixture
def financial_spending_data(db):
    # agency w/ tt agency
    ttagency1 = mommy.make('references.ToptierAgency', cgac_code="cgac_code", name="tta_name")
    mommy.make('references.Agency', id=1, toptier_agency=ttagency1)

    # Financial Account with Object class and submission
    object_class_1 = mommy.make('references.ObjectClass',major_object_class="mocCode",
                                major_object_class_name="mocName", object_class="ocCode", object_class_name="ocName")
    submission_1 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2017, cgac_code="cgac_code")
    mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', object_class=object_class_1,
               obligations_incurred_by_program_object_class_cpe=1000, submission=submission_1)

    # TAS w/ same ttagency
    mommy.make('accounts.TreasuryAppropriationAccount', awarding_toptier_agency=ttagency1)


@pytest.mark.django_db
def test_award_type_endpoint(client, financial_spending_data):
    """Test the award_type endpoint."""

    resp = client.get('/api/v1/financial_spending/object_class/?fiscal_year=2017&agency_id=1')
    print(resp.data)
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) > 0

    resp = client.get('/api/v1/financial_spending/object_class/')
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
