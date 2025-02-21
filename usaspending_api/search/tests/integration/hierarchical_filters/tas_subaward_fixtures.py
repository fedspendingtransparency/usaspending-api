import pytest
from model_bakery import baker
from datetime import datetime
from usaspending_api.search.tests.integration.hierarchical_filters.tas_fixtures import (
    BASIC_TAS,
    ATA_TAS,
    BPOA_TAS,
    SISTER_TAS,
    UNINTUITIVE_BASIC_TAS,
)


@pytest.fixture
def subaward_with_tas(db, award_with_tas):
    subaward(db, 1, [BASIC_TAS])


@pytest.fixture
def subaward_with_ata_tas(db, award_with_ata_tas):
    subaward(db, 1, [ATA_TAS])


@pytest.fixture
def subaward_with_bpoa_tas(db, award_with_bpoa_tas):
    subaward(db, 1, [BPOA_TAS])


@pytest.fixture
def subaward_with_unintuitive_agency(db, tas_with_nonintuitive_agency):
    subaward(db, 1, [UNINTUITIVE_BASIC_TAS])


@pytest.fixture
def subaward_with_multiple_tas(db, award_with_multiple_tas):
    subaward(db, 1, [BASIC_TAS, ATA_TAS])


@pytest.fixture
def subaward_with_no_tas(db, award_without_tas):
    subaward(db, 2, [])


@pytest.fixture
def multiple_subawards_with_tas(db, multiple_awards_with_tas):
    subaward(db, 1, [BASIC_TAS])
    subaward(db, 2, [ATA_TAS])


@pytest.fixture
def multiple_subawards_with_sibling_tas(db, multiple_awards_with_sibling_tas):
    subaward(db, 1, [SISTER_TAS[0]])
    subaward(db, 2, [SISTER_TAS[1]])


def subaward(db, award_id, treasury_account_identifiers):
    baker.make(
        "search.SubawardSearch",
        funding_toptier_agency_name="test",
        broker_subaward_id=award_id,
        award_id=award_id,
        prime_award_type="D",
        sub_action_date=datetime(2017, 12, 1),
        action_date="2017-12-01",
        latest_transaction_id=1,
        subaward_number=11111,
        prime_award_group="procurement",
        subaward_amount=10000,
        treasury_account_identifiers=treasury_account_identifiers,
        unique_award_key="AWARD_" + str(award_id),
    )
