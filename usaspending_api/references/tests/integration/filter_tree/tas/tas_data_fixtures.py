from model_mommy import mommy
import pytest

from usaspending_api.download.lookups import CFO_CGACS

# intentionally out of order to test CFO agency sorting
aribitrary_cfo_cgac_sample = [2, 1, 3, 13, 7]


@pytest.fixture
def basic_agencies(db):
    _complete_agency(1)


@pytest.fixture
def cfo_agencies(db):
    [_complete_agency(int(CFO_CGACS[id])) for id in aribitrary_cfo_cgac_sample]


def _complete_agency(id):
    _setup_agency(id)
    _complete_fa(id)


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
