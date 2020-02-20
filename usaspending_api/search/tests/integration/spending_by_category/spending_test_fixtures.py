from model_mommy import mommy
import pytest


@pytest.fixture
def basic_agencies(db):
    _setup_agency(1, [], "Awarding")

    _setup_agency(4, [], "Funding")


@pytest.fixture
def basic_award(db, basic_agencies):
    mommy.make("awards.Award", id=1, latest_transaction_id=1)
    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        awarding_agency_id=1001,
        funding_agency_id=1004,
        federal_action_obligation=5,
        action_date="2020-01-01",
    )


@pytest.fixture
def agencies_with_subagencies(db):
    """Create some agencies with more than one subtier to toptier"""
    _setup_agency(3, [5], "Awarding")

    _setup_agency(2, [6], "Funding")


@pytest.fixture
def subagency_award(db, agencies_with_subagencies):
    mommy.make("awards.Award", id=2, latest_transaction_id=2)

    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=2,
        awarding_agency_id=1005,
        funding_agency_id=1006,
        federal_action_obligation=10,
        action_date="2020-01-02",
    )


@pytest.fixture
def basic_cfda(db, basic_agencies):
    mommy.make("references.Cfda", id=101, program_number="101.0", program_title="One hundred and one")
    mommy.make("references.Cfda", id=102, program_number="102.0", program_title="One hundred and two")


@pytest.fixture
def cfda_award(db, basic_award, basic_cfda):
    mommy.make(
        "awards.TransactionNormalized",
        id=333,
        award_id=1,
        awarding_agency_id=1001,
        funding_agency_id=1004,
        federal_action_obligation=10,
        action_date="2010-01-02",
    )
    mommy.make(
        "awards.TransactionFABS", transaction_id=333, cfda_number="101.0",
    )

    mommy.make(
        "awards.TransactionNormalized",
        id=334,
        award_id=1,
        awarding_agency_id=1001,
        funding_agency_id=1004,
        federal_action_obligation=10,
        action_date="2010-01-02",
    )
    mommy.make(
        "awards.TransactionFABS", transaction_id=334, cfda_number="102.0",
    )


@pytest.fixture
def single_cfda_2_awards(db, basic_award, basic_cfda):
    mommy.make("awards.Award", id=2, latest_transaction_id=334)
    mommy.make(
        "awards.TransactionNormalized",
        id=333,
        award_id=1,
        awarding_agency_id=1001,
        funding_agency_id=1004,
        federal_action_obligation=10,
        action_date="2010-01-02",
    )
    mommy.make(
        "awards.TransactionFABS", transaction_id=333, cfda_number="101.0",
    )

    mommy.make(
        "awards.TransactionNormalized",
        id=334,
        award_id=2,
        awarding_agency_id=1001,
        funding_agency_id=1004,
        federal_action_obligation=10,
        action_date="2010-01-02",
    )
    mommy.make(
        "awards.TransactionFABS", transaction_id=334, cfda_number="101.0",
    )


def _setup_agency(id, subtiers, special_name):
    mommy.make(
        "references.ToptierAgency",
        toptier_agency_id=id + 2000,
        name=f"{special_name} Toptier Agency {id}",
        abbreviation=f"TA{id}",
    )
    mommy.make(
        "references.SubtierAgency",
        subtier_agency_id=id + 3000,
        name=f"{special_name} Subtier Agency {id}",
        abbreviation=f"SA{id}",
    )
    mommy.make(
        "references.Agency", id=id + 1000, toptier_agency_id=id + 2000, subtier_agency_id=id + 3000, toptier_flag=True
    )

    for sub_id in subtiers:
        mommy.make(
            "references.SubtierAgency",
            subtier_agency_id=sub_id + 3000,
            name=f"{special_name} Subtier Agency {sub_id}",
            abbreviation=f"SA{sub_id}",
        )
        mommy.make(
            "references.Agency",
            id=sub_id + 1000,
            toptier_agency_id=id + 2000,
            subtier_agency_id=sub_id + 3000,
            toptier_flag=False,
        )
