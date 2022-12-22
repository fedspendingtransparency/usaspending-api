from model_bakery import baker
from datetime import datetime
import pytest

from usaspending_api.download.lookups import CFO_CGACS

# intentionally out of order to test CFO agency sorting
arbitrary_cfo_cgac_sample = [2, 1, 3, 13, 7]


@pytest.fixture
def basic_agency(db):
    _complete_agency(1)


@pytest.fixture
def cfo_agencies(db):
    [_complete_agency(int(CFO_CGACS[id])) for id in arbitrary_cfo_cgac_sample]


@pytest.fixture
def non_cfo_agencies(db):
    for i in range(1, 100):
        if str(i).zfill(3) not in CFO_CGACS:
            _complete_agency(i)


@pytest.fixture
def unsupported_agencies(db):
    for i in range(101, 200):
        if str(i).zfill(3) not in CFO_CGACS:
            _setup_agency(i)


@pytest.fixture
def multiple_federal_accounts(db, basic_agency):
    _complete_fa(2, 1)
    _complete_fa(3, 1)
    _complete_fa(4, 1)


@pytest.fixture
def multiple_tas(db, basic_agency):
    _matching_tas(2, 1)
    _matching_tas(3, 1)
    _matching_tas(4, 1)


@pytest.fixture
def fa_with_multiple_tas(db, multiple_tas):
    _setup_fa(1, 1)


@pytest.fixture
def agency_with_unsupported_fa(db):
    _setup_agency(1)
    _setup_fa(1, 1)


@pytest.fixture
def fa_with_unsupported_tas(db, agency_with_unsupported_fa):
    _setup_tas(1, 1)


def _complete_agency(id):
    _setup_agency(id)
    _complete_fa(id, id)


def _complete_fa(id, agency):
    _setup_fa(id, agency)
    _matching_tas(id, id)


def _matching_tas(id, fa):
    _setup_tas(id, fa)
    _setup_faba(id)


def _setup_agency(id):
    baker.make(
        "references.ToptierAgency",
        toptier_agency_id=id,
        name=f"Agency {str(id).zfill(3)}",
        abbreviation=str(id).zfill(3),
        toptier_code=str(id).zfill(3),
        create_date=datetime(2010, 1, 1, 12, 0, 0),
    )


def _setup_fa(id, agency):
    baker.make(
        "accounts.FederalAccount",
        id=id,
        federal_account_code=str(id).zfill(4),
        account_title=f"Fed Account {str(id).zfill(4)}",
        parent_toptier_agency_id=agency,
    )


def _setup_tas(id, fa):
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        treasury_account_identifier=id,
        federal_account_id=fa,
        tas_rendering_label=str(id).zfill(5),
        account_title=f"TAS {str(id).zfill(5)}",
    )


def _setup_faba(id):
    baker.make("search.AwardSearch", award_id=id)
    baker.make("awards.FinancialAccountsByAwards", treasury_account_id=id, award_id=id)
