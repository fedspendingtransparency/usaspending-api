import pytest

from model_bakery import baker

from usaspending_api.awards.models import Award


@pytest.fixture
def awards_and_transactions(db):
    baker.make("search.AwardSearch", award_id=1, total_obligation=2000)
    baker.make(
        "search.AwardSearch", award_id=2, type="U", total_obligation=None, date_signed=None, latest_transaction=None
    )
    baker.make("search.AwardSearch", award_id=3, total_obligation=2000)


@pytest.mark.django_db
def test_null_awards(awards_and_transactions):
    """Test the award.nonempty command."""
    assert Award.objects.count() == 3
    assert Award.nonempty.count() == 2
