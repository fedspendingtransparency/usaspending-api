import pytest
from rest_framework import status
from model_bakery import baker

from .conftest import CURRENT_FISCAL_YEAR


@pytest.fixture
def tas_submissions_across_multiple_years():
    """Sets up test data such that for a specific treasury account there are multiple
    submission years
    """
    dabs = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date=f"{CURRENT_FISCAL_YEAR}-10-09",
        submission_fiscal_year=CURRENT_FISCAL_YEAR,
        submission_fiscal_month=12,
        submission_fiscal_quarter=4,
        is_quarter=False,
        period_start_date=f"{CURRENT_FISCAL_YEAR}-09-01",
        period_end_date=f"{CURRENT_FISCAL_YEAR}-10-01",
    )

    ta1 = baker.make("references.ToptierAgency", toptier_code="007", _fill_optional=True)

    baker.make("references.Agency", id=1, toptier_flag=True, toptier_agency=ta1, _fill_optional=True)

    sub1 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=12,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )
    sub2 = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=CURRENT_FISCAL_YEAR,
        reporting_fiscal_period=12,
        toptier_code=ta1.toptier_code,
        is_final_balances_for_fy=True,
        submission_window_id=dabs.id,
    )

    fa1 = baker.make("accounts.FederalAccount", federal_account_code="001-0000", account_title="FA 1")
    tas1 = baker.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=ta1,
        budget_function_code=100,
        budget_function_title="NAME 1",
        budget_subfunction_code=1100,
        budget_subfunction_title="NAME 1A",
        federal_account=fa1,
        account_title="TA 1",
        tas_rendering_label="001-X-0000-000",
    )

    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission=sub1)
    baker.make("accounts.AppropriationAccountBalances", treasury_account_identifier=tas1, submission=sub2)

    pa1 = baker.make("references.RefProgramActivity", program_activity_code="000", program_activity_name="NAME 1")
    pa2 = baker.make("references.RefProgramActivity", program_activity_code="1000", program_activity_name="NAME 2")
    pa3 = baker.make("references.RefProgramActivity", program_activity_code="4567", program_activity_name="NAME 3")

    oc = "references.ObjectClass"
    oc1 = baker.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=100, object_class_name="equipment"
    )
    oc2 = baker.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=110, object_class_name="hvac"
    )
    oc3 = baker.make(
        oc, major_object_class=10, major_object_class_name="Other", object_class=120, object_class_name="supplies"
    )

    fabpaoc = "financial_activities.FinancialAccountsByProgramActivityObjectClass"
    baker.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa1,
        object_class=oc1,
        obligations_incurred_by_program_object_class_cpe=1,
        gross_outlay_amount_by_program_object_class_cpe=10000000,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub1,
        program_activity=pa2,
        object_class=oc2,
        obligations_incurred_by_program_object_class_cpe=10,
        gross_outlay_amount_by_program_object_class_cpe=1000000,
    )
    baker.make(
        fabpaoc,
        treasury_account=tas1,
        submission=sub2,
        program_activity=pa3,
        object_class=oc3,
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=100000,
    )


url = "/api/v2/agency/treasury_account/{tas}/object_class/{query_params}"


@pytest.mark.django_db
def test_tas_object_class_success(client, monkeypatch, agency_account_data, helpers):
    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "001-X-0000-000"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
        },
        "results": [
            {
                "gross_outlay_amount": 11100000.0,
                "name": "Other",
                "obligated_amount": 111.0,
                "children": [
                    {"gross_outlay_amount": 100000.0, "name": "NAME 3", "obligated_amount": 100.0},
                    {"gross_outlay_amount": 1000000.0, "name": "NAME 2", "obligated_amount": 10.0},
                    {"gross_outlay_amount": 10000000.0, "name": "NAME 1", "obligated_amount": 1.0},
                ],
            }
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    # Tests non-existent tas
    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "001-X-0000-999"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 0,
        },
        "results": [],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result

    # Tests tas with slashes
    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "002-2008/2009-0000-000"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
        },
        "results": [
            {
                "gross_outlay_amount": 1000000.0,
                "name": "Other",
                "obligated_amount": 10.0,
                "children": [
                    {"gross_outlay_amount": 1000000.0, "name": "NAME 5", "obligated_amount": 10.0},
                ],
            }
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_tas_object_class_multiple_pa_per_oc(client, monkeypatch, tas_mulitple_pas_per_oc, helpers):
    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "001-X-0000-000"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
        },
        "results": [
            {
                "gross_outlay_amount": 11000000.0,
                "name": "Other",
                "obligated_amount": 11.0,
                "children": [
                    {"gross_outlay_amount": 1000000.0, "name": "NAME 2", "obligated_amount": 10.0},
                    {"gross_outlay_amount": 10000000.0, "name": "NAME 1", "obligated_amount": 1.0},
                ],
            },
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_tas_object_class_multiple_submission_years(client, agency_account_data):
    tas = "002-X-0000-000"
    submission_year = 2017
    query_params = f"?fiscal_year={submission_year}"
    resp = client.get(url.format(tas=tas, query_params=query_params))
    expected_result = {
        "fiscal_year": submission_year,
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 1,
        },
        "results": [
            {
                "gross_outlay_amount": 10000.0,
                "name": "Other",
                "obligated_amount": 1000.0,
                "children": [{"gross_outlay_amount": 10000.0, "name": "NAME 4", "obligated_amount": 1000.0}],
            }
        ],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result


@pytest.mark.django_db
def test_tas_with_no_object_class(client, monkeypatch, tas_with_no_object_class, helpers):
    helpers.mock_current_fiscal_year(monkeypatch)
    tas = "001-X-0000-000"
    resp = client.get(url.format(tas=tas, query_params=""))
    expected_result = {
        "fiscal_year": helpers.get_mocked_current_fiscal_year(),
        "treasury_account_symbol": tas,
        "messages": [],
        "page_metadata": {
            "hasNext": False,
            "hasPrevious": False,
            "limit": 10,
            "next": None,
            "page": 1,
            "previous": None,
            "total": 0,
        },
        "results": [],
    }

    assert resp.status_code == status.HTTP_200_OK
    assert resp.json() == expected_result
