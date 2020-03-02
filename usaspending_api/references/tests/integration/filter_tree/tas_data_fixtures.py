from model_mommy import mommy
import pytest


@pytest.fixture
def basic_agencies(db):
    mommy.make("references.ToptierAgency", toptier_code="001", name="Agency 001")
    mommy.make("references.ToptierAgency", toptier_code="0001", name="FREC Agency 0001")


@pytest.fixture
def basic_tas(db):
    mommy.make("accounts.TreasuryAppropriationAccount", agency_id="001")
