import pytest
from model_bakery import baker
from rest_framework import status


@pytest.fixture
def award_subaward_count_data(db):
    baker.make(
        "search.AwardSearch",
        award_id=1,
        generated_unique_award_id="CONT_AWD_zzz_whatever",
        piid="zzz",
        fain="abc123",
        type="B",
        total_obligation=1000,
        subaward_count=10,
    )
    baker.make(
        "search.AwardSearch",
        award_id=2,
        generated_unique_award_id="CONT_AWD_aaa_whatever",
        piid="aaa",
        fain="abc123",
        type="B",
        total_obligation=1000,
        subaward_count=0,
    )


@pytest.mark.django_db
def test_subaward_success(client, award_subaward_count_data):
    """Test subaward count endpoint"""

    resp = client.get("/api/v2/awards/count/subaward/CONT_AWD_zzz_whatever/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["subawards"] == 10

    resp = client.get("/api/v2/awards/count/subaward/1/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["subawards"] == 10

    resp = client.get("/api/v2/awards/count/subaward/CONT_AWD_aaa_whatever/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["subawards"] == 0

    resp = client.get("/api/v2/awards/count/subaward/2/")
    assert resp.status_code == status.HTTP_200_OK
    assert resp.data["subawards"] == 0


@pytest.mark.django_db
def test_missing_award(client, award_subaward_count_data):
    """Test subaward count endpoint for award that does not exist"""

    resp = client.get("/api/v2/awards/count/subaward/4/")
    assert resp.status_code == status.HTTP_404_NOT_FOUND
    assert resp.data["detail"] == "No Award found with: '4'"
