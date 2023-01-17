import pytest
from model_bakery import baker
from datetime import datetime

from usaspending_api.accounts.models import TreasuryAppropriationAccount, FederalAccount
from usaspending_api.references.models import ToptierAgency

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
    _award_with_tas([BASIC_TAS])


def _award_with_tas(indexes, award_id=1, toptier_code=None):
    tas_components = []
    tas_paths = []
    count = 0
    for index in indexes:
        aid = TAS_DICTIONARIES[index]["aid"]
        ata = TAS_DICTIONARIES[index].get("ata")
        main = TAS_DICTIONARIES[index]["main"]
        sub = TAS_DICTIONARIES[index]["sub"]
        bpoa = TAS_DICTIONARIES[index].get("bpoa")
        epoa = TAS_DICTIONARIES[index].get("epoa")
        a = TAS_DICTIONARIES[index].get("a")
        if toptier_code is None:
            toptier_code = aid
        existing_ta = ToptierAgency.objects.filter(toptier_code=toptier_code).first()
        ta = (
            baker.make("references.ToptierAgency", toptier_agency_id=count + award_id, toptier_code=toptier_code)
            if existing_ta is None
            else existing_ta
        )
        existing_fa = FederalAccount.objects.filter(agency_identifier=aid, main_account_code=main).first()
        fa = (
            baker.make(
                "accounts.FederalAccount",
                id=index,
                parent_toptier_agency_id=ta.toptier_agency_id,
                agency_identifier=aid,
                main_account_code=main,
                federal_account_code=f"{aid}-{main}",
            )
            if existing_fa is None
            else existing_fa
        )
        baker.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=index,
            allocation_transfer_agency_id=ata,
            agency_id=aid,
            main_account_code=main,
            sub_account_code=sub,
            availability_type_code=a,
            beginning_period_of_availability=bpoa,
            ending_period_of_availability=epoa,
            tas_rendering_label=TreasuryAppropriationAccount.generate_tas_rendering_label(
                ata,
                aid,
                a,
                bpoa,
                epoa,
                main,
                sub,
            ),
            federal_account_id=fa.id,
        )

        baker.make("awards.FinancialAccountsByAwards", award_id=award_id, treasury_account_id=index)
        tas_components.append(f"aid={aid}main={main}ata={ata}sub={sub}bpoa={bpoa}epoa{epoa}=a={a}")
        tas_paths.append(
            f"agency={aid}faaid={aid}famain={fa.main_account_code}aid={aid}main={main}ata={ata}sub={sub}bpoa={bpoa}epoa={epoa}a={a}"
        )
        count = count + 1
    baker.make(
        "search.AwardSearch",
        award_id=award_id,
        generated_unique_award_id=f"AWARD_{award_id}",
        type="D",
        date_signed=datetime(2017, 1, 1),
        category="contracts",
        latest_transaction_id=1000 + award_id,
        piid="abcdefg",
        display_award_id="abcdefg",
        fain="xyz",
        uri="abcxyx",
        action_date=datetime(2017, 12, 1),
        tas_components=tas_components,
        tas_paths=tas_paths,
        treasury_account_identifiers=indexes,
    )


@pytest.fixture
def award_with_bpoa_tas(db):
    _award_with_tas([BPOA_TAS])


@pytest.fixture
def award_with_ata_tas(db):
    _award_with_tas([ATA_TAS])


@pytest.fixture
def tas_with_nonintuitive_agency(db):
    _award_with_tas([BASIC_TAS], toptier_code=UNINTUITIVE_AGENCY)


@pytest.fixture
def award_with_multiple_tas(db):
    _award_with_tas([BASIC_TAS, ATA_TAS])


@pytest.fixture
def award_without_tas(db):
    award(db, 2)


@pytest.fixture
def multiple_awards_with_tas(db):
    _award_with_tas([BASIC_TAS], award_id=1)
    _award_with_tas([ATA_TAS], award_id=2)


@pytest.fixture
def multiple_awards_with_sibling_tas(db):
    _award_with_tas([SISTER_TAS[0]], award_id=1)
    _award_with_tas([SISTER_TAS[1]], award_id=2)
    _award_with_tas([SISTER_TAS[2]], award_id=2)
    # award(db, 1)
    # agency(db, 1, TAS_DICTIONARIES[SISTER_TAS[0]]["aid"])
    # tas_with_fa(db, award_id=1, agency=1, index=SISTER_TAS[0])
    # award(db, 2)
    # tas(db, award_id=2, fa_id=1, index=SISTER_TAS[1])
    # award(db, 3)
    # tas(db, award_id=3, fa_id=1, index=SISTER_TAS[2])


def award(db, id):
    # most values are just defined in order to match on all the default filters; we aren't testing those here
    award = baker.make(
        "search.AwardSearch",
        award_id=id,
        generated_unique_award_id=f"AWARD_{id}",
        type="D",
        date_signed=datetime(2017, 1, 1),
        category="contracts",
        latest_transaction_id=1000 + id,
        piid="abcdefg",
        fain="xyz",
        uri="abcxyx",
        action_date=datetime(2017, 12, 1),
    )
    baker.make("search.TransactionSearch", transaction_id=1000 + id, award=award, action_date=datetime(2017, 12, 1))


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
