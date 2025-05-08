import pytest

from django.conf import settings
from model_bakery import baker
from rest_framework import status
from usaspending_api.awards.models import TransactionNormalized
from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year


URL = "/api/v2/agency/{code}/{filter}"


@pytest.fixture
def agency_data(helpers):
    ta1 = baker.make(
        "references.ToptierAgency",
        toptier_code="001",
        abbreviation="ABBR",
        name="NAME",
        mission="TO BOLDLY GO",
        about_agency_data="ABOUT",
        website="HTTP",
        justification="BECAUSE",
        icon_filename="HAI.jpg",
    )
    ta2 = baker.make("references.ToptierAgency", toptier_code="002", _fill_optional=True)
    sa1 = baker.make("references.SubtierAgency", subtier_code="ST1", _fill_optional=True)
    sa2 = baker.make("references.SubtierAgency", subtier_code="ST2", _fill_optional=True)
    a1 = baker.make(
        "references.Agency", id=1, toptier_flag=True, toptier_agency=ta1, subtier_agency=sa1, _fill_optional=True
    )
    baker.make(
        "references.Agency", id=2, toptier_flag=True, toptier_agency=ta2, subtier_agency=sa2, _fill_optional=True
    )
    dabs = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date="2020-10-09",
        submission_fiscal_year=2020,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=False,
        period_start_date="2020-09-01",
        period_end_date="2020-10-01",
    )
    sub1 = baker.make(
        "submissions.SubmissionAttributes",
        toptier_code=ta1.toptier_code,
        submission_window_id=dabs.id,
        reporting_fiscal_year=helpers.get_mocked_current_fiscal_year(),
    )
    baker.make("submissions.SubmissionAttributes", toptier_code=ta2.toptier_code, submission_window_id=dabs.id)
    tas1 = baker.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta1)
    tas2 = baker.make("accounts.TreasuryAppropriationAccount", funding_toptier_agency=ta2)
    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission=sub1)
    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas2, submission=sub1)
    baker.make(
        "search.TransactionSearch",
        transaction_id=1,
        awarding_agency_id=a1.id,
        fiscal_year=helpers.get_mocked_current_fiscal_year(),
    )

    defc = baker.make(
        "references.DisasterEmergencyFundCode", code="L", group_name="covid_19", public_law="LAW", title="title"
    )

    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        submission=sub1,
        treasury_account=tas1,
        disaster_emergency_fund=defc,
    )


@pytest.mark.django_db
def test_happy_path(client, monkeypatch, agency_data, helpers):
    helpers.mock_current_fiscal_year(monkeypatch)
    resp = client.get(URL.format(code="001", filter=""))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["fiscal_year"] == helpers.get_mocked_current_fiscal_year()
    assert resp.data["toptier_code"] == "001"
    assert resp.data["abbreviation"] == "ABBR"
    assert resp.data["name"] == "NAME"
    assert resp.data["agency_id"] == 1
    assert resp.data["mission"] == "TO BOLDLY GO"
    assert resp.data["about_agency_data"] == "ABOUT"
    assert resp.data["website"] == "HTTP"
    assert resp.data["congressional_justification_url"] == "BECAUSE"
    assert resp.data["icon_filename"] == "HAI.jpg"
    assert resp.data["subtier_agency_count"] == 1
    assert resp.data["def_codes"] == [
        {"code": "L", "public_law": "LAW", "title": "title", "urls": None, "disaster": "covid_19"}
    ]
    assert resp.data["messages"] == []

    resp = client.get(URL.format(code="001", filter=f"?fiscal_year={helpers.get_mocked_current_fiscal_year()}"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["fiscal_year"] == helpers.get_mocked_current_fiscal_year()
    assert resp.data["toptier_code"] == "001"
    assert resp.data["subtier_agency_count"] == 1

    resp = client.get(URL.format(code="001", filter=f"?fiscal_year={helpers.get_mocked_current_fiscal_year() - 1}"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["fiscal_year"] == helpers.get_mocked_current_fiscal_year() - 1
    assert resp.data["toptier_code"] == "001"
    assert resp.data["subtier_agency_count"] == 1

    resp = client.get(URL.format(code="002", filter=""))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["fiscal_year"] == helpers.get_mocked_current_fiscal_year()
    assert resp.data["toptier_code"] == "002"
    assert resp.data["agency_id"] == 2
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
