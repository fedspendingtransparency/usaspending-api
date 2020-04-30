import pytest

from model_mommy import mommy
from rest_framework import status
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


url = "/api/v2/agency/{code}/program_activity/count/{filter}"


@pytest.fixture
def agency_data():
    ta1 = mommy.make("references.ToptierAgency", toptier_code="007")
    ta2 = mommy.make("references.ToptierAgency", toptier_code="008")
    ta3 = mommy.make("references.ToptierAgency", toptier_code="009")
    sub1 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=current_fiscal_year())
    sub2 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=2017)
    sub3 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=2018)
    sub4 = mommy.make("submissions.SubmissionAttributes", reporting_fiscal_year=2019)
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta1)
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta2)
    tas3 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta3)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas2)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas3)
    pa1 = mommy.make("references.RefProgramActivity", program_activity_code="000")
    pa2 = mommy.make("references.RefProgramActivity", program_activity_code="1000")
    pa3 = mommy.make("references.RefProgramActivity", program_activity_code="4567")
    pa4 = mommy.make("references.RefProgramActivity", program_activity_code="111")

    fabpaoc = "financial_activities.FinancialAccountsByProgramActivityObjectClass"
    mommy.make(fabpaoc, final_of_fy=True, treasury_account=tas1, submission=sub1, program_activity=pa1)
    mommy.make(fabpaoc, final_of_fy=True, treasury_account=tas1, submission=sub1, program_activity=pa2)
    mommy.make(fabpaoc, final_of_fy=True, treasury_account=tas1, submission=sub1, program_activity=pa3)
    mommy.make(fabpaoc, final_of_fy=True, treasury_account=tas2, submission=sub2, program_activity=pa4)
    mommy.make(fabpaoc, final_of_fy=True, treasury_account=tas2, submission=sub3, program_activity=pa4)
    mommy.make(fabpaoc, final_of_fy=True, treasury_account=tas3, submission=sub3, program_activity=pa4)
    mommy.make(fabpaoc, final_of_fy=True, treasury_account=tas3, submission=sub4, program_activity=pa4)
    mommy.make(fabpaoc, final_of_fy=True, treasury_account=tas3, submission=sub4, program_activity=pa4)


@pytest.mark.django_db
def test_program_activity_count_success(client, agency_data):
    resp = client.get(url.format(code="007", filter=""))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["program_activity_count"] == 3

    resp = client.get(url.format(code="007", filter="?fiscal_year=2017"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["program_activity_count"] == 0


@pytest.mark.django_db
def test_program_activity_count_too_early(client, agency_data):
    resp = client.get(url.format(code="007", filter="?fiscal_year=2007"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_program_activity_count_future(client, agency_data):
    resp = client.get(url.format(code="007", filter=f"?fiscal_year={current_fiscal_year() + 1}"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_program_activity_count_specific(client, agency_data):
    resp = client.get(url.format(code="008", filter="?fiscal_year=2017"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["program_activity_count"] == 1

    resp = client.get(url.format(code="008", filter="?fiscal_year=2018"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["program_activity_count"] == 1


@pytest.mark.django_db
def test_program_activity_count_ignore_duplicates(client, agency_data):
    resp = client.get(url.format(code="009", filter="?fiscal_year=2019"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["program_activity_count"] == 1
