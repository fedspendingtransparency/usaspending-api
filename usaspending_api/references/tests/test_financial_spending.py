import pytest

from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def financial_spending_data(db):
    # Create 2 objects that should be returned and one that should not.
    # Create AGENCY AND TopTier AGENCY For FinancialAccountsByProgramActivityObjectClass objects
    ttagency1 = mommy.make("references.ToptierAgency", name="tta_name")
    mommy.make("references.Agency", id=1, toptier_agency=ttagency1)

    # Object 1
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ttagency1)
    # Financial Account with Object class and submission
    object_class_1 = mommy.make(
        "references.ObjectClass",
        major_object_class="10",
        major_object_class_name="mocName",
        object_class="ocCode",
        object_class_name="ocName",
    )
    submission_1 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=2017)
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=object_class_1,
        obligations_incurred_by_program_object_class_cpe=1000,
        submission=submission_1,
        treasury_account=tas1,
        final_of_fy=True,
    )

    # Object 2 (contains 2 fabpaoc s)
    object_class_2 = mommy.make(
        "references.ObjectClass",
        major_object_class="10",
        major_object_class_name="mocName2",
        object_class="ocCode2",
        object_class_name="ocName2",
    )
    submission_2 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=2017)
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=object_class_2,
        obligations_incurred_by_program_object_class_cpe=1000,
        submission=submission_2,
        treasury_account=tas1,
        final_of_fy=True,
    )
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=object_class_2,
        obligations_incurred_by_program_object_class_cpe=2000,
        submission=submission_2,
        treasury_account=tas1,
        final_of_fy=True,
    )

    # 2018, not reported by 2017 api call
    object_class_0 = mommy.make(
        "references.ObjectClass",
        major_object_class="00",
        major_object_class_name="Zero object type, override me",
        object_class="ocCode2",
        object_class_name="ocName2",
    )
    tas3 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ttagency1)
    submission_3 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=2018)
    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        object_class=object_class_0,
        obligations_incurred_by_program_object_class_cpe=1000,
        submission=submission_3,
        treasury_account=tas3,
        final_of_fy=True,
    )


@pytest.mark.django_db
def test_award_type_endpoint(client, financial_spending_data):
    """Test the award_type endpoint."""

    resp = client.get("/api/v2/financial_spending/major_object_class/?fiscal_year=2017&funding_agency_id=1")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2

    # make sure resp.data['results'] contains a 3000 value
    assert (
        resp.data["results"][1]["obligated_amount"] == "3000.00"
        or resp.data["results"][0]["obligated_amount"] == "3000.00"
    )

    # check for bad request due to missing params
    resp = client.get("/api/v2/financial_spending/object_class/")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST

    resp = client.get(
        "/api/v2/financial_spending/object_class/?fiscal_year=2017&funding_agency_id=1&major_object_class_code=10"
    )
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 2

    # make sure resp.data['results'] contains a 3000 value
    assert (
        resp.data["results"][1]["obligated_amount"] == "3000.00"
        or resp.data["results"][0]["obligated_amount"] == "3000.00"
    )

    # check for bad request due to missing params
    resp = client.get("/api/v2/financial_spending/object_class/")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST


@pytest.mark.django_db
def test_award_type_endpoint_no_object_class(client, financial_spending_data):
    """Test the award_type endpoint in the major object class 00 special case.

    Object class 00 should be reported as 'Unknown Object Type' despite
    nme in database."""

    resp = client.get("/api/v2/financial_spending/major_object_class/?fiscal_year=2018&funding_agency_id=1")
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.data["results"]) == 1

    # verify that object class name has been overriden
    assert resp.data["results"][0]["major_object_class_name"] == "Unknown Object Type"
