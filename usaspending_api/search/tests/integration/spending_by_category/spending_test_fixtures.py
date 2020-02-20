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


def setup_country_data():
    mommy.make("references.RefCountryCode", country_code="USA", country_name="UNITED STATES")
    mommy.make("references.RefCountryCode", country_code="IRQ", country_name="IRAQ")


def setup_country_transactions():
    mommy.make("awards.TransactionFABS", transaction_id=1, place_of_perform_country_c="IRQ")
    mommy.make("awards.TransactionFABS", transaction_id=2, place_of_perform_country_c="USA")
    mommy.make(
        "awards.TransactionNormalized", id=1, federal_action_obligation=5, action_date="2020-01-01",
    )
    mommy.make(
        "awards.TransactionNormalized", id=2, federal_action_obligation=10, action_date="2020-01-02",
    )
