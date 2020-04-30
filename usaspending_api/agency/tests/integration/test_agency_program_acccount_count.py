import pytest

from model_mommy import mommy
from rest_framework import status
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year

url = "/api/v2/agency/{code}/program_activity/count/{filter}"


@pytest.fixture
def agency_data():
    mommy.make("references.ToptierAgency", toptier_agency_id=1, toptier_code="007")
    mommy.make("references.ToptierAgency", toptier_agency_id=2, toptier_code="008")
    mommy.make("references.ToptierAgency", toptier_agency_id=3, toptier_code="009")
    mommy.make(
        "references.RefProgramActivity",
        responsible_agency_id="007",
        program_activity_code="000",
        budget_year=str(current_fiscal_year()),
    )
    mommy.make(
        "references.RefProgramActivity",
        responsible_agency_id="007",
        program_activity_code="1000",
        budget_year=str(current_fiscal_year()),
    )
    mommy.make(
        "references.RefProgramActivity",
        responsible_agency_id="007",
        program_activity_code="4567",
        budget_year=str(current_fiscal_year()),
    )
    mommy.make(
        "references.RefProgramActivity", responsible_agency_id="008", program_activity_code="111", budget_year="2017",
    )
    mommy.make(
        "references.RefProgramActivity", responsible_agency_id="008", program_activity_code="111", budget_year="2018",
    )
    mommy.make(
        "references.RefProgramActivity", responsible_agency_id="009", program_activity_code="111", budget_year="2018",
    )
    mommy.make(
        "references.RefProgramActivity", responsible_agency_id="009", program_activity_code="111", budget_year="2019",
    )
    mommy.make(
        "references.RefProgramActivity", responsible_agency_id="009", program_activity_code="111", budget_year="2019",
    )


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
