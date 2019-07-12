import pytest

from model_mommy import mommy


@pytest.fixture()
def submissions_data():
    mommy.make("submissions.SubmissionAttributes", _quantity=2)


@pytest.mark.django_db
def test_submissions_list(submissions_data, client):
    """
    Ensure the submissions endpoint lists the right number of entities
    """
    resp = client.get("/api/v1/submissions/")
    assert resp.status_code == 200
    assert len(resp.data["results"]) == 2
