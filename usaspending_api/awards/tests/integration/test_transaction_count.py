import pytest
from model_bakery import baker
from rest_framework import status


@pytest.fixture
def awards_transaction_data(db):
    baker.make(
        "search.AwardSearch",
        award_id=1,
        generated_unique_award_id="CONT_AWD_zzz_whatever",
        piid="zzz",
        fain="abc123",
        type="B",
        total_obligation=1000,
    )
    baker.make("search.TransactionSearch", transaction_id=1, award_id=1)

    baker.make(
        "search.AwardSearch",
        award_id=2,
        generated_unique_award_id="CONT_AWD_aaa_whatever",
        piid="aaa",
        fain="abc123",
        type="B",
        total_obligation=1000,
    )
    baker.make("search.TransactionSearch", transaction_id=2, award_id=2)
    baker.make("search.TransactionSearch", transaction_id=3, award_id=2)
    baker.make("search.TransactionSearch", transaction_id=4, award_id=2)

    baker.make(
        "search.AwardSearch",
        award_id=3,
        generated_unique_award_id="ASST_NON_bbb_abc123",
        piid="bbb",
        fain="abc123",
        type="04",
        total_obligation=1000,
    )


@pytest.mark.django_db
def test_award_success(client, awards_transaction_data):
    """Test transaction count endpoint"""

    resp = client.get("/api/v2/awards/count/transaction/CONT_AWD_zzz_whatever/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["transactions"] == 1

    resp = client.get("/api/v2/awards/count/transaction/1/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["transactions"] == 1

    resp = client.get("/api/v2/awards/count/transaction/CONT_AWD_aaa_whatever/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["transactions"] == 3

    resp = client.get("/api/v2/awards/count/transaction/2/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["transactions"] == 3


@pytest.mark.django_db
def test_award_no_transactions(client, awards_transaction_data):
    """Test transaction count endpoint for award with no transactions"""

    resp = client.get("/api/v2/awards/count/transaction/ASST_NON_bbb_abc123/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["transactions"] == 0

    resp = client.get("/api/v2/awards/count/transaction/3/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["transactions"] == 0


@pytest.mark.django_db
def test_missing_award(client, awards_transaction_data):
    """Test transaction count endpoint for award that does not exist"""

    resp = client.get("/api/v2/awards/count/transaction/4/")
    assert resp.status_code == status.HTTP_404_NOT_FOUND
    assert resp.data["detail"] == "No Award found with: '4'"
