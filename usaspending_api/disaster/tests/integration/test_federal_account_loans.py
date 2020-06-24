import pytest
import json
from model_mommy import mommy
from rest_framework import status

url = "/api/v2/disaster/federal_account/loans/"


@pytest.fixture
def account_data():
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
        "submissions.SubmissionAttributes", reporting_period_start="2020-05-15", reporting_period_end="2020-05-29"
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
def test_federal_account_loans_success(client, account_data):
    body = {"filter": {"def_codes": ["M"]}}
    resp = client.post(url, content_type="application/json", data=json.dumps(body))
    expected_results = []
    assert resp.status_code == status.HTTP_200_OK
    print(json.dumps(resp.json()))
    assert resp.json()["results"] == expected_results

    body = {"filter": {"def_codes": ["M", "L", "N", "O"]}}
    resp = client.post(url, content_type="application/json", data=json.dumps(body))
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
def test_federal_account_loans_empty(client, account_data):
    body = {"filter": {"def_codes": ["A"]}}
    resp = client.post(url, content_type="application/json", data=json.dumps(body))
    assert resp.status_code == status.HTTP_200_OK
    assert len(resp.json()["results"]) == 0
