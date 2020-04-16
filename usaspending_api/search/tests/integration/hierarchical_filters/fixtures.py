import pytest
from model_mommy import mommy
from datetime import datetime


@pytest.fixture
def award(db):
    award = mommy.make(
        "awards.Award",
        id=1,
        generated_unique_award_id="AWARD_{}",
        type="D",
        date_signed=datetime(2017, 1, 1),
        category="contracts",
        latest_transaction_id=1000 + 1,
        piid="abcdefg",
        fain="xyz",
        uri="abcxyx",
    )
    mommy.make("awards.TransactionNormalized", id=1000 + 1, award=award, action_date=datetime(2017, 12, 1))
