from model_mommy import mommy


def setup_basic_agency_tree():
    mommy.make("references.ToptierAgency", toptier_agency_id=2001, name="Awarding Toptier Agency 1", abbreviation="TA1")
    mommy.make("references.SubtierAgency", subtier_agency_id=3001, name="Awarding Subtier Agency 1", abbreviation="SA1")
    mommy.make("references.ToptierAgency", toptier_agency_id=2003, name="Awarding Toptier Agency 3", abbreviation="TA3")
    mommy.make("references.SubtierAgency", subtier_agency_id=3003, name="Awarding Subtier Agency 3", abbreviation="SA3")
    mommy.make("references.SubtierAgency", subtier_agency_id=3005, name="Awarding Subtier Agency 5", abbreviation="SA5")

    mommy.make("references.ToptierAgency", toptier_agency_id=2002, name="Funding Toptier Agency 2", abbreviation="TA2")
    mommy.make("references.SubtierAgency", subtier_agency_id=3002, name="Funding Subtier Agency 2", abbreviation="SA2")
    mommy.make("references.ToptierAgency", toptier_agency_id=2004, name="Funding Toptier Agency 4", abbreviation="TA4")
    mommy.make("references.SubtierAgency", subtier_agency_id=3004, name="Funding Subtier Agency 4", abbreviation="SA4")
    mommy.make("references.SubtierAgency", subtier_agency_id=3006, name="Funding Subtier Agency 6", abbreviation="SA6")

    mommy.make("references.Agency", id=1001, toptier_agency_id=2001, subtier_agency_id=3001, toptier_flag=True)
    mommy.make("references.Agency", id=1002, toptier_agency_id=2002, subtier_agency_id=3002, toptier_flag=False)
    mommy.make("references.Agency", id=1006, toptier_agency_id=2002, subtier_agency_id=3006, toptier_flag=True)

    mommy.make("references.Agency", id=1003, toptier_agency_id=2003, subtier_agency_id=3003, toptier_flag=False)
    mommy.make("references.Agency", id=1004, toptier_agency_id=2004, subtier_agency_id=3004, toptier_flag=True)
    mommy.make("references.Agency", id=1005, toptier_agency_id=2003, subtier_agency_id=3005, toptier_flag=True)


def setup_basic_awards_and_transactions():
    mommy.make("awards.Award", id=1, latest_transaction_id=1)
    mommy.make("awards.Award", id=2, latest_transaction_id=2)

    mommy.make(
        "awards.TransactionNormalized",
        id=1,
        award_id=1,
        awarding_agency_id=1001,
        funding_agency_id=1002,
        federal_action_obligation=5,
        action_date="2020-01-01",
    )
    mommy.make(
        "awards.TransactionNormalized",
        id=2,
        award_id=2,
        awarding_agency_id=1003,
        funding_agency_id=1004,
        federal_action_obligation=10,
        action_date="2020-01-02",
    )
