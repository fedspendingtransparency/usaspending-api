import pytest
from model_bakery import baker
from datetime import datetime


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


@pytest.fixture
def subaward_with_multiple_tas(db, award_with_multiple_tas):
    subaward(db, 1)


@pytest.fixture
def subaward_with_no_tas(db, award_without_tas):
    subaward(db, 2)


@pytest.fixture
def multiple_subawards_with_tas(db, multiple_awards_with_tas):
    subaward(db, 1)
    subaward(db, 2)


@pytest.fixture
def multiple_subawards_with_sibling_tas(db, multiple_awards_with_sibling_tas):
    subaward(db, 1)
    subaward(db, 2)


def subaward(db, award_id):
    baker.make(
        "search.SubawardSearch",
        funding_toptier_agency_name="test",
        broker_subaward_id=award_id,
        award_id=award_id,
        prime_award_type="D",
        subaction_date=datetime(2017, 12, 1),
        latest_transaction_id=1,
        subaward_number=11111,
        prime_award_group="procurement",
        subaward_amount=10000,
    )
