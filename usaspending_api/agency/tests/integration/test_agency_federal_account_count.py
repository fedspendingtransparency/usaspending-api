import pytest

from model_mommy import mommy
from rest_framework import status
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year

url = "/api/v2/agency/{code}/federal_account/count/{filter}"


@pytest.fixture
def agency_data():
    mommy.make("references.ToptierAgency", toptier_agency_id=1, toptier_code="007")
    mommy.make("references.ToptierAgency", toptier_agency_id=2, toptier_code="008")
    mommy.make("references.ToptierAgency", toptier_agency_id=3, toptier_code="009")

    mommy.make("awards.FinancialAccountsByAwards", pk=1, treasury_account_id=1, reporting_period_start="2019-10-1")
    mommy.make("awards.FinancialAccountsByAwards", pk=2, treasury_account_id=2, reporting_period_start="2019-10-1")
    mommy.make("awards.FinancialAccountsByAwards", pk=3, treasury_account_id=3, reporting_period_start="2019-10-1")
    mommy.make("awards.FinancialAccountsByAwards", pk=4, treasury_account_id=4, reporting_period_start="2017-01-01")
    mommy.make("awards.FinancialAccountsByAwards", pk=5, treasury_account_id=4, reporting_period_start="2018-01-01")
    mommy.make("awards.FinancialAccountsByAwards", pk=6, treasury_account_id=5, reporting_period_start="2018-10-1")
    mommy.make("awards.FinancialAccountsByAwards", pk=7, treasury_account_id=5, reporting_period_start="2018-10-1")

    mommy.make("accounts.TreasuryAppropriationAccount", pk=1, agency_id="007", federal_account_id=1)
    mommy.make("accounts.TreasuryAppropriationAccount", pk=2, agency_id="007", federal_account_id=1)
    mommy.make("accounts.TreasuryAppropriationAccount", pk=3, agency_id="007", federal_account_id=2)
    mommy.make("accounts.TreasuryAppropriationAccount", pk=4, agency_id="008", federal_account_id=3)
    mommy.make("accounts.TreasuryAppropriationAccount", pk=5, agency_id="009", federal_account_id=4)
    mommy.make("accounts.FederalAccount", pk=1)
    mommy.make("accounts.FederalAccount", pk=2)
    mommy.make("accounts.FederalAccount", pk=3)
    mommy.make("accounts.FederalAccount", pk=4)


@pytest.mark.django_db
def test_federal_account_count_success(client, agency_data):
    resp = client.get(url.format(code="007", filter=""))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["federal_account_count"] == 2
    assert resp.data["treasury_account_count"] == 3

    resp = client.get(url.format(code="007", filter="?fiscal_year=2017"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["federal_account_count"] == 0
    assert resp.data["treasury_account_count"] == 0


@pytest.mark.django_db
def test_federal_account_count_too_early(client, agency_data):
    resp = client.get(url.format(code="007", filter="?fiscal_year=2007"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_program_activity_count_future(client, agency_data):
    resp = client.get(url.format(code="007", filter=f"?fiscal_year={current_fiscal_year() + 1}"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_federal_account_count_specific(client, agency_data):
    resp = client.get(url.format(code="008", filter="?fiscal_year=2017"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["federal_account_count"] == 1
    assert resp.data["treasury_account_count"] == 1

    resp = client.get(url.format(code="008", filter="?fiscal_year=2018"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["federal_account_count"] == 1
    assert resp.data["treasury_account_count"] == 1


@pytest.mark.django_db
def test_federal_account_ignore_duplicates(client, agency_data):
    resp = client.get(url.format(code="009", filter="?fiscal_year=2019"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["federal_account_count"] == 1
    assert resp.data["treasury_account_count"] == 1
