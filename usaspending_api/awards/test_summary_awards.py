import pytest

from model_mommy import mommy
from usaspending_api.awards.models import Award


@pytest.fixture(scope="session")
def award_models():
    mommy.make('awards.Award', total_obligation="2000", _quantity=2)
    mommy.make(
        'awards.Award',
        type="U",
        total_obligation=None,
        date_signed=None,
        recipient=None)


@pytest.mark.django_db
def test_null_awards(award_models):
    assert Award.objects.count() == 3
    assert Award.nonempty.count() == 2
