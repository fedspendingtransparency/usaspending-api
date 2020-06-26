import pytest
from model_mommy import mommy
from rest_framework import status

url = "/api/v2/disaster/federal_account/loans/"


@pytest.fixture
def account_data():
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=7,
        submission_reveal_date="2022-6-15",
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        is_quarter=True,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=7,
        submission_reveal_date="2022-6-15",
    )
    mommy.make("references.DisasterEmergencyFundCode", code="A")
    award1 = mommy.make("awards.Award", id=111, total_loan_value=1111)
    award2 = mommy.make("awards.Award", id=222, total_loan_value=2222)
    award3 = mommy.make("awards.Award", id=333, total_loan_value=3333)
    award4 = mommy.make("awards.Award", id=444, total_loan_value=4444)
    fed_acct1 = mommy.make("accounts.FederalAccount", account_title="gifts", federal_account_code="000-0000", id=21)
    tre_acct1 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        federal_account=fed_acct1,
        tas_rendering_label="2020/99",
        account_title="flowers",
        treasury_account_identifier=22,
    )
    tre_acct2 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        federal_account=fed_acct1,
        tas_rendering_label="2020/98",
        account_title="evergreens",
        treasury_account_identifier=23,
    )
    tre_acct3 = mommy.make(
        "accounts.TreasuryAppropriationAccount",
        federal_account=fed_acct1,
        tas_rendering_label="2020/52",
        account_title="ferns",
        treasury_account_identifier=24,
    )
    sub1 = mommy.make(
        "submissions.SubmissionAttributes",
        reporting_period_start="2020-05-15",
        reporting_period_end="2020-05-29",
        reporting_fiscal_year=2020,
        reporting_fiscal_quarter=3,
        reporting_fiscal_period=7,
        quarter_format_flag=False,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub1,
        transaction_obligated_amount=100,
        gross_outlay_amount_by_award_cpe=111,
        disaster_emergency_fund__code="M",
        treasury_account=tre_acct1,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub1,
        award=award1,
        disaster_emergency_fund__code="L",
        treasury_account=tre_acct2,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub1,
        award=award2,
        disaster_emergency_fund__code="9",
        treasury_account=tre_acct2,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub1,
        award=award3,
        disaster_emergency_fund__code="O",
        treasury_account=tre_acct2,
    )
    mommy.make(
        "awards.FinancialAccountsByAwards",
        submission=sub1,
        award=award4,
        disaster_emergency_fund__code="N",
        treasury_account=tre_acct3,
    )


@pytest.mark.django_db
def test_federal_account_loans_success(client, account_data, monkeypatch, helpers):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M"])
    expected_results = []
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results

    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["M", "L", "N", "O"])
    expected_results = [
        {
            "children": [
                {"code": "2020/52", "count": 1, "description": "ferns", "face_value_of_loan": 4444.0, "id": 24},
                {"code": "2020/98", "count": 2, "description": "evergreens", "face_value_of_loan": 4444.0, "id": 23},
            ],
            "code": "000-0000",
            "count": 3,
            "description": "gifts",
            "face_value_of_loan": 8888.0,
            "id": 21,
        }
    ]
    assert resp.status_code == status.HTTP_200_OK
    assert resp.json()["results"] == expected_results


@pytest.mark.django_db
def test_federal_account_loans_empty(client, monkeypatch, helpers, account_data):
    helpers.patch_datetime_now(monkeypatch, 2022, 12, 31)
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["A"])
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 0


@pytest.mark.django_db
def test_federal_account_loans_invalid_defc(client, account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes=["ZZ"])
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Field 'filter|def_codes' is outside valid values ['9', 'A', 'L', 'M', 'N', 'O']"


@pytest.mark.django_db
def test_federal_account_loans_invalid_defc_type(client, account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url, def_codes="100")
    assert resp.status_code == status.HTTP_400_BAD_REQUEST
    assert resp.data["detail"] == "Invalid value in 'filter|def_codes'. '100' is not a valid type (array)"


@pytest.mark.django_db
def test_federal_account_loans_missing_defc(client, account_data, helpers):
    resp = helpers.post_for_spending_endpoint(client, url)
    assert resp.status_code == status.HTTP_422_UNPROCESSABLE_ENTITY
    assert resp.data["detail"] == "Missing value: 'filter|def_codes' is a required field"
