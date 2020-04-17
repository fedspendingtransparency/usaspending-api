import pytest
from model_mommy import mommy
from datetime import datetime


@pytest.fixture
def award_with_tas(db):
    award(db, 1)
    tas(db, 1)


def award(db, id):
    # most values are just defined in order to match on all the default filters; we aren't testing those here
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


def tas(db, award_id):
    mommy.make("accounts.FederalAccount", id=1)
    mommy.make(
        "accounts.TreasuryAppropriationAccount",
        treasury_account_identifier=1,
        agency_id="123",
        main_account_code="2345",
        sub_account_code="000",
        availability_type_code="X",
        federal_account_id=1,
    )
    mommy.make("awards.FinancialAccountsByAwards", award_id=award_id, treasury_account_id=1)
