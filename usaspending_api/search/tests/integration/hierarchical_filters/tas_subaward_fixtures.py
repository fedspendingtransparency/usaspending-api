import pytest
from model_mommy import mommy
from datetime import datetime

from usaspending_api.accounts.models import TreasuryAppropriationAccount


@pytest.fixture
def subaward_with_tas(db, award_with_tas):
    subaward(db, 1)


@pytest.fixture
def subaward_with_ata_tas(db, award_with_ata_tas):
    subaward(db, 1)


@pytest.fixture
def subaward_with_bpoa_tas(db, award_with_bpoa_tas):
    subaward(db, 1)


@pytest.fixture
def subaward_with_unintuitive_agency(db, tas_with_nonintuitive_agency):
    subaward(db, 1)


def subaward(db, award_id):
    mommy.make(
        "awards.Subaward",
        funding_toptier_agency_name="test",
        id=award_id,
        award_id=award_id,
        prime_award_type="D",
        action_date=datetime(2017, 12, 1),
        latest_transaction_id=1,
        subaward_number=11111,
        award_type="procurement",
        amount=10000,
    )
