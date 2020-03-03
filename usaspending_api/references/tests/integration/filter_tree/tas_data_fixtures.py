from model_mommy import mommy
import pytest

from usaspending_api.download.lookups import CFO_CGACS
from usaspending_api.references.constants import DOD_SUBSUMED_CGAC, DOD_CGAC, DHS_SUBSUMED_CGAC

# intentionally out of order to test CFO agency sorting
aribitrary_cfo_cgac_sample = [2, 1, 3, 13, 7]


@pytest.fixture
def basic_agencies(db):
    _setup_basic_cgac(1)
    _setup_basic_frec(1)


@pytest.fixture
def basic_tas(db):
    mommy.make("accounts.TreasuryAppropriationAccount", agency_id="001")


@pytest.fixture
def tas_for_frec(db, basic_agencies):
    _setup_basic_frec_tas(1)


@pytest.fixture
def frec_tas_with_no_match(db, basic_agencies):
    mommy.make("accounts.TreasuryAppropriationAccount", agency_id="001", fr_entity_code="not gonna match")


@pytest.fixture
def basic_cfo_agencies(db):
    for i in aribitrary_cfo_cgac_sample:
        _setup_basic_cgac(CFO_CGACS[i], "CFO ")


@pytest.fixture
def basic_cfo_and_non_cfo_agencies(db, basic_cfo_agencies):
    for x in range(1, 100):
        if (
            str(x).zfill(3) not in CFO_CGACS
            and str(x).zfill(3) not in DOD_SUBSUMED_CGAC
            and str(x).zfill(3) not in DHS_SUBSUMED_CGAC
        ):
            _setup_basic_cgac(x)


@pytest.fixture
def one_tas_per_agency(db, basic_cfo_and_non_cfo_agencies):
    for i in aribitrary_cfo_cgac_sample:
        _setup_basic_cgac_tas(CFO_CGACS[i])
    for x in range(1, 100):
        if (
            str(x).zfill(3) not in CFO_CGACS
            and str(x).zfill(3) not in DOD_SUBSUMED_CGAC
            and str(x).zfill(3) not in DHS_SUBSUMED_CGAC
        ):
            _setup_basic_cgac_tas(x)


@pytest.fixture
def basic_dod_agencies(db):
    _setup_basic_cgac(DOD_CGAC, "Department of Defense")
    for cgac in DOD_SUBSUMED_CGAC:
        _setup_basic_cgac(cgac, "DoD subsumed ")


@pytest.fixture
def tas_for_dod_subs(db, basic_dod_agencies):
    for cgac in DOD_SUBSUMED_CGAC:
        _setup_basic_cgac_tas(cgac)


@pytest.fixture
def tas_for_dod(db, basic_dod_agencies):
    _setup_basic_cgac_tas(DOD_CGAC)


def _setup_basic_cgac(id, name_prefix=""):
    mommy.make(
        "references.ToptierAgency", toptier_code=str(id).zfill(3), name=f"{name_prefix}Agency {str(id).zfill(3)}"
    )


def _setup_basic_cgac_tas(id):
    mommy.make("accounts.TreasuryAppropriationAccount", agency_id=str(id).zfill(3))


def _setup_basic_frec(id, name_prefix=""):
    mommy.make(
        "references.ToptierAgency", toptier_code=str(id).zfill(4), name=f"{name_prefix}FREC Agency {str(id).zfill(4)}"
    )


def _setup_basic_frec_tas(id):
    mommy.make(
        "accounts.TreasuryAppropriationAccount",
        agency_id="Some CGAC that should not be used",
        fr_entity_code=str(id).zfill(4),
    )
