import pytest
from model_mommy import mommy
from rest_framework import status


@pytest.fixture
def awards_federal_account_data(db):
    mommy.make(
        "awards.Award",
        id=1,
        generated_unique_award_id="CONT_AWD_zzz_whatever",
        piid="zzz",
        fain="abc123",
        type="B",
        total_obligation=1000,
    )

    mommy.make("awards.FinancialAccountsByAwards", financial_accounts_by_awards_id=1, award_id=1)
    mommy.make("awards.FinancialAccountsByAwards", financial_accounts_by_awards_id=2, award_id=1)

    mommy.make(
        "awards.Award",
        id=2,
        generated_unique_award_id="CONT_AWD_aaa_whatever",
        piid="aaa",
        fain="abc123",
        type="B",
        total_obligation=1000,
    )

    mommy.make("awards.FinancialAccountsByAwards", financial_accounts_by_awards_id=3, award_id=2)

    mommy.make(
        "awards.Award",
        id=3,
        generated_unique_award_id="ASST_bbb_abc123",
        piid="bbb",
        fain="abc123",
        type="04",
        total_obligation=1000,
    )


def test_award_success(client, awards_federal_account_data):
    """Test federal_account count endpoint"""

    resp = client.get("/api/v2/awards/federal_account_count/CONT_AWD_zzz_whatever/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["federal_accounts"] == 2

    resp = client.get("/api/v2/awards/federal_account_count/1/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["federal_accounts"] == 2

    resp = client.get("/api/v2/awards/federal_account_count/CONT_AWD_aaa_whatever/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["federal_accounts"] == 1

    resp = client.get("/api/v2/awards/federal_account_count/2/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["federal_accounts"] == 1


def test_award_no_federal_accounts(client, awards_federal_account_data):
    """Test federal_account count endpoint for award with no federal_accounts"""

    resp = client.get("/api/v2/awards/federal_account_count/ASST_bbb_abc123/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["federal_accounts"] == 0

    resp = client.get("/api/v2/awards/federal_account_count/3/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["federal_accounts"] == 0


def test_missing_award(client, awards_federal_account_data):
    """Test federal_account count endpoint for award that does not exist"""

    resp = client.get("/api/v2/awards/federal_account_count/4/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["federal_accounts"] == 0
