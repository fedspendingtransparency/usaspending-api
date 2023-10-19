import pytest
from decimal import Decimal

from model_bakery import baker
from rest_framework import status

from usaspending_api.common.helpers.fiscal_year_helpers import current_fiscal_year

URL = "/api/v2/reporting/agencies/{code}/differences/{filter}"


@pytest.fixture
def differences_data():
    dabs1 = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2020,
        submission_fiscal_month=6,
        submission_fiscal_quarter=2,
        is_quarter=False,
        submission_reveal_date="2020-04-01",
        period_start_date="2020-04-01",
    )
    baker.make(
        "submissions.SubmissionAttributes",
        toptier_code="001",
        quarter_format_flag=False,
        submission_window=dabs1,
    )
    ta1 = baker.make("references.ToptierAgency", toptier_code="001", _fill_optional=True)
    baker.make(
        "references.Agency", id=1, toptier_agency_id=ta1.toptier_agency_id, toptier_flag=True, _fill_optional=True
    )
    tas1 = baker.make("accounts.TreasuryAppropriationAccount")
    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1)
    baker.make(
        "reporting.ReportingAgencyTas",
        toptier_code=ta1.toptier_code,
        fiscal_year=2020,
        fiscal_period=3,
        tas_rendering_label="TAS-1",
        appropriation_obligated_amount=123.4,
        object_class_pa_obligated_amount=120,
        diff_approp_ocpa_obligated_amounts=3.4,
    )
    baker.make(
        "reporting.ReportingAgencyTas",
        toptier_code=ta1.toptier_code,
        fiscal_year=2020,
        fiscal_period=3,
        tas_rendering_label="TAS-2",
        appropriation_obligated_amount=500,
        object_class_pa_obligated_amount=1000,
        diff_approp_ocpa_obligated_amounts=-500,
    )
    baker.make(
        "reporting.ReportingAgencyTas",
        toptier_code=ta1.toptier_code,
        fiscal_year=2020,
        fiscal_period=3,
        tas_rendering_label="TAS-3",
        appropriation_obligated_amount=100,
        object_class_pa_obligated_amount=100,
        diff_approp_ocpa_obligated_amounts=0,
    )


@pytest.mark.django_db
def test_happy_path(client, differences_data):
    resp = client.get(URL.format(code="001", filter="?fiscal_year=2020&fiscal_period=3"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["page_metadata"] == {
        "page": 1,
        "next": None,
        "previous": None,
        "hasNext": False,
        "hasPrevious": False,
        "total": 2,
        "limit": 10,
    }
    assert resp.data["results"] == [
        {
            "tas": "TAS-2",
            "file_a_obligation": Decimal(500),
            "file_b_obligation": Decimal(1000),
            "difference": Decimal(-500),
        },
        {
            "tas": "TAS-1",
            "file_a_obligation": Decimal("123.4"),
            "file_b_obligation": Decimal(120),
            "difference": Decimal("3.4"),
        },
    ]


@pytest.mark.django_db
def test_sort_by_file_a_obligation_ascending(client, differences_data):
    resp = client.get(
        URL.format(code="001", filter="?fiscal_year=2020&fiscal_period=3&sort=file_a_obligation&order=asc")
    )
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["page_metadata"] == {
        "page": 1,
        "next": None,
        "previous": None,
        "hasNext": False,
        "hasPrevious": False,
        "total": 2,
        "limit": 10,
    }
    assert resp.data["results"] == [
        {
            "tas": "TAS-1",
            "file_a_obligation": Decimal("123.4"),
            "file_b_obligation": Decimal(120),
            "difference": Decimal("3.4"),
        },
        {
            "tas": "TAS-2",
            "file_a_obligation": Decimal(500),
            "file_b_obligation": Decimal(1000),
            "difference": Decimal(-500),
        },
    ]


@pytest.mark.django_db
def test_limit_one(client, differences_data):
    resp = client.get(URL.format(code="001", filter="?fiscal_year=2020&fiscal_period=3&limit=1"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["page_metadata"] == {
        "page": 1,
        "next": 2,
        "previous": None,
        "hasNext": True,
        "hasPrevious": False,
        "total": 2,
        "limit": 1,
    }
    assert resp.data["results"] == [
        {
            "tas": "TAS-2",
            "file_a_obligation": Decimal(500),
            "file_b_obligation": Decimal(1000),
            "difference": Decimal(-500),
        }
    ]


@pytest.mark.django_db
def test_limit_one_page_two(client, differences_data):
    resp = client.get(URL.format(code="001", filter="?fiscal_year=2020&fiscal_period=3&limit=1&page=2"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["page_metadata"] == {
        "page": 2,
        "next": None,
        "previous": 1,
        "hasNext": False,
        "hasPrevious": True,
        "total": 2,
        "limit": 1,
    }
    assert resp.data["results"] == [
        {
            "tas": "TAS-1",
            "file_a_obligation": Decimal("123.4"),
            "file_b_obligation": Decimal(120),
            "difference": Decimal("3.4"),
        },
    ]


@pytest.mark.django_db
def test_no_results(client, differences_data):
    resp = client.get(URL.format(code="001", filter="?fiscal_year=2020&fiscal_period=2"))
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["page_metadata"] == {
        "page": 1,
        "next": None,
        "previous": None,
        "hasNext": False,
        "hasPrevious": False,
        "total": 0,
        "limit": 10,
    }
    assert resp.data["results"] == []


@pytest.mark.django_db
def test_missing_fiscal_year(client, differences_data):
    resp = client.get(URL.format(code="001", filter="?fiscal_period=3"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_missing_fiscal_period(client, differences_data):
    resp = client.get(URL.format(code="001", filter="?fiscal_year=2020"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_invalid_code(client, differences_data):
    resp = client.get(URL.format(code="002", filter="?fiscal_year=2020&fiscal_period=3"))
    assert resp.status_code == status.HTTP_404_NOT_FOUND


@pytest.mark.django_db
def test_invalid_period(client, differences_data):
    resp = client.get(URL.format(code="001", filter="?fiscal_year=2020&fiscal_period=13"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY


@pytest.mark.django_db
def test_invalid_year(client, differences_data):
    resp = client.get(URL.format(code="001", filter=f"?fiscal_year={current_fiscal_year() + 1}&fiscal_period=3"))
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
