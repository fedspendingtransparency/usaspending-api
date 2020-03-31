from model_mommy import mommy
from datetime import datetime
import pytest

from usaspending_api.download.lookups import CFO_CGACS

# intentionally out of order to test CFO agency sorting
aribitrary_cfo_cgac_sample = [2, 1, 3, 13, 7]


@pytest.fixture
def basic_agency(db):
    _complete_agency(1)


@pytest.fixture
def cfo_agencies(db):
    [_complete_agency(int(CFO_CGACS[id])) for id in aribitrary_cfo_cgac_sample]


@pytest.fixture
def non_cfo_agencies(db):
    for i in range(1, 100):
        if str(i).zfill(3) not in CFO_CGACS:
            _complete_agency(i)


@pytest.fixture
def unsupported_agencies(db):
    for i in range(101, 200):
        if str(i).zfill(3) not in CFO_CGACS:
            _unsupported_agency(i)


def _complete_agency(id):
    _setup_agency(id)
    _complete_fa(id)


def _unsupported_agency(id):
    _setup_agency(id)


def _complete_fa(id):
    _setup_fa(id, id)
    _matching_tas(id)


def _matching_tas(id):
    _setup_tas(id, id)
    _setup_faba(id)


def _setup_agency(id):
    mommy.make(
        "references.ToptierAgency",
        toptier_agency_id=id,
        name=f"Agency {str(id).zfill(3)}",
        toptier_code=str(id).zfill(3),
        create_date=datetime(2010, 1, 1, 12, 0, 0),
    )


def _setup_fa(id, agency):
    mommy.make(
        "accounts.FederalAccount",
        id=id,
        federal_account_code=str(id).zfill(3),
        account_title=f"Fed Account {str(id).zfill(3)}",
        parent_toptier_agency_id=agency,
    )


def _setup_tas(id, fa):
    mommy.make(
        "accounts.TreasuryAppropriationAccount",
        treasury_account_identifier=id,
        federal_account_id=fa,
        tas_rendering_label=str(id).zfill(3),
        account_title=f"TAS {str(id).zfill(3)}",
    )


def _setup_faba(id):
    mommy.make("awards.FinancialAccountsByAwards", treasury_account_id=id)
