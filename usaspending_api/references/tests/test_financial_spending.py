import pytest
import json
from datetime import date

from model_mommy import mommy
from rest_framework import status

from usaspending_api.awards.models import Award
from usaspending_api.references.models import Agency, ToptierAgency, SubtierAgency


@pytest.fixture
def financial_spending_data(db):
    # Create 2 objects that should be returned and one that should not.
    # Create AGENCY AND TopTier AGENCY For FinancialAccountsByProgramActivityObjectClass objects
    ttagency1 = mommy.make('references.ToptierAgency', name="tta_name")
    mommy.make('references.Agency', id=1, toptier_agency=ttagency1)

    # Object 1
    tas1 = mommy.make('accounts.TreasuryAppropriationAccount', funding_toptier_agency=ttagency1)
    # Financial Account with Object class and submission
    object_class_1 = mommy.make('references.ObjectClass', major_object_class="mocCode",
                                major_object_class_name="mocName", object_class="ocCode", object_class_name="ocName")
    submission_1 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2017)
    mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', object_class=object_class_1,
               obligations_incurred_by_program_object_class_cpe=1000, submission=submission_1, treasury_account=tas1,
               final_of_fy=True)

    # Object 2 (contains 2 fabpaoc s)
    object_class_2 = mommy.make('references.ObjectClass', major_object_class="mocCode2",
                                major_object_class_name="mocName2", object_class="ocCode2", object_class_name="ocName2")
    submission_2 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2017)
    mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', object_class=object_class_2,
               obligations_incurred_by_program_object_class_cpe=1000, submission=submission_2, treasury_account=tas1,
               final_of_fy=True)
    mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', object_class=object_class_2,
               obligations_incurred_by_program_object_class_cpe=2000, submission=submission_2, treasury_account=tas1,
               final_of_fy=True)

    # not reported by api Object
    tas3 = mommy.make('accounts.TreasuryAppropriationAccount', funding_toptier_agency=ttagency1)
    submission_3 = mommy.make('submissions.SubmissionAttributes', reporting_fiscal_year=2018)
    mommy.make('financial_activities.FinancialAccountsByProgramActivityObjectClass', object_class=object_class_1,
               obligations_incurred_by_program_object_class_cpe=1000, submission=submission_3, treasury_account=tas3,
               final_of_fy=True)


@pytest.mark.django_db
def test_award_type_endpoint(client, financial_spending_data):
    """Test the award_type endpoint."""

    resp = client.get('/api/v2/financial_spending/object_class/?fiscal_year=2017&funding_agency_id=1')
    print(resp.data)
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data['results']) == 2

    # make sure resp.data['results'] contains a 3000 value
    assert resp.data['results'][1]['obligated_amount'] == "3000.00"

    # check for bad request due to missing params
    resp = client.get('/api/v2/financial_spending/object_class/')
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
