import pytest

from django.conf import settings
from model_mommy import mommy
from rest_framework import status
from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


URL = "/api/v2/agency/{code}/{filter}"


@pytest.fixture
def agency_data():
    ta1 = mommy.make(
        "references.ToptierAgency",
        toptier_code="001",
        abbreviation="ABBR",
        name="NAME",
        toptier_agency_id=534,
        mission="TO BOLDLY GO",
        about_agency_data="ABOUT",
        website="HTTP",
        justification="BECAUSE",
        icon_filename="HAI.jpg",
    )
    ta2 = mommy.make("references.ToptierAgency", toptier_code="002")
    sa1 = mommy.make("references.SubtierAgency", subtier_code="ST1")
    sa2 = mommy.make("references.SubtierAgency", subtier_code="ST2")
    a1 = mommy.make("references.Agency", toptier_agency=ta1, subtier_agency=sa1)
    mommy.make("references.Agency", toptier_agency=ta2, subtier_agency=sa2)
    tas1 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta1)
    tas2 = mommy.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta2)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1)
    mommy.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas2)
    mommy.make("awards.TransactionNormalized", awarding_agency=a1, fiscal_year=current_fiscal_year())


@pytest.mark.django_db
def test_happy_path(client, agency_data):
    resp = client.get(URL.format(code="001", filter=""))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["fiscal_year"] == current_fiscal_year()
    assert resp.data["toptier_code"] == "001"
    assert resp.data["abbreviation"] == "ABBR"
    assert resp.data["name"] == "NAME"
    assert resp.data["agency_id"] == 534
    assert resp.data["mission"] == "TO BOLDLY GO"
    assert resp.data["about_agency_data"] == "ABOUT"
    assert resp.data["website"] == "HTTP"
    assert resp.data["congressional_justification_url"] == "BECAUSE"
    assert resp.data["icon_filename"] == "HAI.jpg"
    assert resp.data["subtier_agency_count"] == 1
    assert resp.data["messages"] == []

    resp = client.get(URL.format(code="001", filter=f"?fiscal_year={current_fiscal_year()}"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["fiscal_year"] == current_fiscal_year()
    assert resp.data["toptier_code"] == "001"
    assert resp.data["subtier_agency_count"] == 1

    resp = client.get(URL.format(code="001", filter=f"?fiscal_year={current_fiscal_year() - 1}"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["fiscal_year"] == current_fiscal_year() - 1
    assert resp.data["toptier_code"] == "001"
    assert resp.data["subtier_agency_count"] == 1

    resp = client.get(URL.format(code="002", filter=""))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["fiscal_year"] == current_fiscal_year()
    assert resp.data["toptier_code"] == "002"
    assert resp.data["subtier_agency_count"] == 0


@pytest.mark.django_db
def test_invalid_agency(client, agency_data):
    resp = client.get(URL.format(code="XXX", filter=""))
    assert resp.status_code == status.HTTP_404_NOT_FOUND

    resp = client.get(URL.format(code="999", filter=""))
    assert resp.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.django_db
def test_really_old_transaction(client, agency_data):
    """Subtier Agencies for really old transactions should not count."""
    TransactionNormalized.objects.update(fiscal_year=fy(settings.API_SEARCH_MIN_DATE) - 1)
    resp = client.get(URL.format(code="001", filter=""))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["toptier_code"] == "001"
    assert resp.data["subtier_agency_count"] == 0


@pytest.mark.django_db
def test_old_fiscal_year(client, agency_data):
    resp = client.get(URL.format(code="001", filter=f"?fiscal_year={fy(settings.API_SEARCH_MIN_DATE) - 1}"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_future_fiscal_year(client, agency_data):
    resp = client.get(URL.format(code="001", filter=f"?fiscal_year={current_fiscal_year() + 1}"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
