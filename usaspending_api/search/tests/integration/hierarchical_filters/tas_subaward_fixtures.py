import pytest
from model_mommy import mommy
from datetime import datetime

from usaspending_api.accounts.models import TreasuryAppropriationAccount


@pytest.fixture
def subaward_with_tas(db, award_with_tas):
    subaward(db, 1)


def subaward(db, award_id):
    mommy.make("awards.Subaward", id=award_id, award_id=award_id, prime_award_type="D")
