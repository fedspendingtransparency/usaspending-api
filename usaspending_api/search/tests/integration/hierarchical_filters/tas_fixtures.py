import pytest
from model_bakery import baker
from datetime import datetime

from usaspending_api.accounts.models import TreasuryAppropriationAccount

UNINTUITIVE_AGENCY = "898"
# ensures that tests aren't failing for having the wrong TAS. We trust functionality of tas_rendering_label_to_component_dictionary because it is tested elsewhere
BASIC_TAS = 0
ATA_TAS = 1
BPOA_TAS = 2
ATA_BPOA_TAS = 3
SISTER_TAS = [1, 4, 5]
TAS_STRINGS = [
    "000-X-0126-000",
    "010-024-X-8445-002",
    "012-2000/2000-1231-000",
    "020-012-2000/2000-1231-000",
    "010-024-X-8445-552",
    "010-024-X-8445-578",
]
TAS_DICTIONARIES = [
    TreasuryAppropriationAccount.tas_rendering_label_to_component_dictionary(tas) for tas in TAS_STRINGS
]


@pytest.fixture
def award_with_tas(db):
    award(db, 1)
    tas_with_agency(db, 1, BASIC_TAS)


@pytest.fixture
def award_with_bpoa_tas(db):
    award(db, 1)
    tas_with_agency(db, 1, BPOA_TAS)


@pytest.fixture
def award_with_ata_tas(db):
    award(db, 1)
    tas_with_agency(db, 1, ATA_TAS)


@pytest.fixture
def tas_with_nonintuitive_agency(db):
    award(db, 1)
    agency(db, 1, UNINTUITIVE_AGENCY)  # none of the tas have ata or aid 898
    tas_with_fa(db, award_id=1, agency=1, index=BASIC_TAS)


@pytest.fixture
def award_with_multiple_tas(db):
    award(db, 1)
    agency(db, 1, TAS_DICTIONARIES[BASIC_TAS]["aid"])
    agency(db, 2, TAS_DICTIONARIES[ATA_TAS]["ata"])
    tas_with_fa(db, award_id=1, agency=1, index=BASIC_TAS)
    tas_with_fa(db, award_id=1, agency=2, index=ATA_TAS)


@pytest.fixture
def award_without_tas(db):
    award(db, 2)


@pytest.fixture
def multiple_awards_with_tas(db):
    award(db, 1)
    tas_with_agency(db, 1, BASIC_TAS)
    award(db, 2)
    tas_with_agency(db, 2, ATA_TAS)


@pytest.fixture
def multiple_awards_with_sibling_tas(db):
    award(db, 1)
    agency(db, 1, TAS_DICTIONARIES[SISTER_TAS[0]]["aid"])
    tas_with_fa(db, award_id=1, agency=1, index=SISTER_TAS[0])
    award(db, 2)
    tas(db, award_id=2, fa_id=1, index=SISTER_TAS[1])
    award(db, 3)
    tas(db, award_id=3, fa_id=1, index=SISTER_TAS[2])


def award(db, id):
    # most values are just defined in order to match on all the default filters; we aren't testing those here
    award = baker.make(
        "awards.Award",
        id=id,
        generated_unique_award_id=f"AWARD_{id}",
        type="D",
        date_signed=datetime(2017, 1, 1),
        category="contracts",
        latest_transaction_id=1000 + id,
        piid="abcdefg",
        fain="xyz",
        uri="abcxyx",
    )
    baker.make("awards.TransactionNormalized", id=1000 + id, award=award, action_date=datetime(2017, 12, 1))


def agency(db, agency_id, toptier_code):
    baker.make("references.ToptierAgency", toptier_agency_id=agency_id, toptier_code=toptier_code)


def tas(db, award_id, fa_id, index):
    baker.make(
        "accounts.TreasuryAppropriationAccount",
        treasury_account_identifier=index,
        allocation_transfer_agency_id=TAS_DICTIONARIES[index].get("ata"),
        agency_id=TAS_DICTIONARIES[index]["aid"],
        main_account_code=TAS_DICTIONARIES[index]["main"],
        sub_account_code=TAS_DICTIONARIES[index]["sub"],
        availability_type_code=TAS_DICTIONARIES[index].get("a"),
        beginning_period_of_availability=TAS_DICTIONARIES[index].get("bpoa"),
        ending_period_of_availability=TAS_DICTIONARIES[index].get("epoa"),
        tas_rendering_label=TreasuryAppropriationAccount.generate_tas_rendering_label(
            TAS_DICTIONARIES[index].get("ata"),
            TAS_DICTIONARIES[index]["aid"],
            TAS_DICTIONARIES[index].get("a"),
            TAS_DICTIONARIES[index].get("bpoa"),
            TAS_DICTIONARIES[index].get("epoa"),
            TAS_DICTIONARIES[index]["main"],
            TAS_DICTIONARIES[index]["sub"],
        ),
        federal_account_id=fa_id,
    )
    baker.make("awards.FinancialAccountsByAwards", award_id=award_id, treasury_account_id=index)


def tas_with_fa(db, award_id, agency, index):
    baker.make(
        "accounts.FederalAccount",
        id=index,
        parent_toptier_agency_id=int(agency),
        agency_identifier=TAS_DICTIONARIES[index]["aid"],
        main_account_code=TAS_DICTIONARIES[index]["main"],
        federal_account_code=f"{TAS_DICTIONARIES[index]['aid']}-{TAS_DICTIONARIES[index]['main']}",
    )
    tas(db, award_id, index, index)


def tas_with_agency(db, award_id, index):
    agency(db, agency_id=award_id, toptier_code=TAS_DICTIONARIES[index]["aid"])
    tas_with_fa(db, award_id=award_id, agency=award_id, index=index)
