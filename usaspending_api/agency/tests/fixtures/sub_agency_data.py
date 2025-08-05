import pytest

from model_bakery import baker

from usaspending_api.search.models import TransactionSearch


@pytest.fixture
def sub_agency_data_1():

    # Submission
    dsws = baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_reveal_date="2021-04-09",
        submission_fiscal_year=2021,
        submission_fiscal_month=7,
        submission_fiscal_quarter=3,
        is_quarter=False,
        period_start_date="2021-03-01",
        period_end_date="2021-04-01",
    )
    baker.make("submissions.SubmissionAttributes", toptier_code="001", submission_window=dsws)
    baker.make("submissions.SubmissionAttributes", toptier_code="002", submission_window=dsws)
    baker.make("submissions.SubmissionAttributes", toptier_code="003", submission_window=dsws)

    # Toptier and Awarding Agency
    toptier_agency_1 = baker.make("references.ToptierAgency", toptier_code="001", name="Agency 1", _fill_optional=True)
    toptier_agency_2 = baker.make("references.ToptierAgency", toptier_code="002", name="Agency 2", _fill_optional=True)
    toptier_agency_3 = baker.make("references.ToptierAgency", toptier_code="003", name="Agency 3", _fill_optional=True)
    subtier_agency_1 = baker.make(
        "references.SubtierAgency",
        subtier_code="0001",
        name="Sub-Agency 1",
        abbreviation="A1",
    )
    subtier_agency_2 = baker.make(
        "references.SubtierAgency", subtier_code="0002", name="Sub-Agency 2", abbreviation="A2"
    )
    subtier_agency_3 = baker.make(
        "references.SubtierAgency", subtier_code="0003", name="Sub-Agency 3", abbreviation="A3"
    )
    awarding_agency_1 = baker.make(
        "references.Agency", toptier_agency=toptier_agency_1, subtier_agency=subtier_agency_1, toptier_flag=True
    )
    awarding_agency_2 = baker.make(
        "references.Agency", toptier_agency=toptier_agency_2, subtier_agency=subtier_agency_2, toptier_flag=True
    )
    awarding_agency_3 = baker.make(
        "references.Agency", toptier_agency=toptier_agency_3, subtier_agency=subtier_agency_3, toptier_flag=True
    )
    baker.make("references.Office", office_code="0001", office_name="Office 1")
    baker.make("references.Office", office_code="0002", office_name="Office 2")

    # Awards
    award_contract = baker.make(
        "search.AwardSearch",
        award_id=1,
        category="contract",
        date_signed="2021-04-01",
    )
    award_idv = baker.make(
        "search.AwardSearch",
        award_id=2,
        category="idv",
        date_signed="2020-04-01",
    )
    award_grant = baker.make(
        "search.AwardSearch",
        award_id=3,
        category="grant",
        date_signed="2021-04-01",
    )
    award_loan = baker.make(
        "search.AwardSearch",
        award_id=5,
        category="loans",
        date_signed="2021-04-01",
    )
    award_dp = baker.make(
        "search.AwardSearch",
        award_id=6,
        category="direct payment",
        date_signed="2021-04-01",
    )

    baker.make(
        TransactionSearch,
        transaction_id=1,
        award_id=award_contract.award_id,
        award_category="contract",
        federal_action_obligation=101,
        generated_pragmatic_obligation=101,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        award_date_signed="2021-04-01",
        awarding_agency_id=awarding_agency_1.id,
        awarding_toptier_agency_id=awarding_agency_1.id,
        funding_agency_id=awarding_agency_1.id,
        funding_toptier_agency_id=awarding_agency_1.id,
        is_fpds=True,
        type="A",
        awarding_agency_code="001",
        awarding_toptier_agency_name="Agency 1",
        funding_agency_code="001",
        funding_toptier_agency_name="Agency 1",
        awarding_sub_tier_agency_c="0001",
        awarding_subtier_agency_name="Sub-Agency 1",
        awarding_subtier_agency_abbreviation="A1",
        funding_sub_tier_agency_co="0001",
        funding_subtier_agency_name="Sub-Agency 1",
        funding_subtier_agency_abbreviation="A1",
        awarding_office_name="Office 1",
        awarding_office_code="0001",
        funding_office_name="Office 2",
        funding_office_code="0002",
    )

    baker.make(
        TransactionSearch,
        transaction_id=2,
        award_id=award_contract.award_id,
        award_category="contract",
        federal_action_obligation=110,
        generated_pragmatic_obligation=110,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        award_date_signed="2021-04-01",
        awarding_agency_id=awarding_agency_3.id,
        awarding_toptier_agency_id=awarding_agency_3.id,
        funding_agency_id=awarding_agency_3.id,
        funding_toptier_agency_id=awarding_agency_3.id,
        is_fpds=True,
        type="B",
        awarding_agency_code="003",
        awarding_toptier_agency_name="Agency 3",
        funding_agency_code="003",
        funding_toptier_agency_name="Agency 3",
        awarding_sub_tier_agency_c="0003",
        awarding_subtier_agency_name="Sub-Agency 3",
        awarding_subtier_agency_abbreviation="A3",
        funding_sub_tier_agency_co="0003",
        funding_subtier_agency_name="Sub-Agency 3",
        funding_subtier_agency_abbreviation="A3",
        awarding_office_name=None,
        awarding_office_code=None,
        funding_office_name=None,
        funding_office_code=None,
    )
    baker.make(
        TransactionSearch,
        transaction_id=3,
        award_id=award_idv.award_id,
        award_category="idv",
        federal_action_obligation=102,
        generated_pragmatic_obligation=102,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        award_date_signed="2020-04-01",
        awarding_agency_id=awarding_agency_1.id,
        awarding_toptier_agency_id=awarding_agency_1.id,
        is_fpds=True,
        type="IDV_A",
        awarding_agency_code="001",
        awarding_toptier_agency_name="Agency 1",
        awarding_sub_tier_agency_c="0001",
        awarding_subtier_agency_name="Sub-Agency 1",
        awarding_subtier_agency_abbreviation="A1",
        awarding_office_name="Office 1",
        awarding_office_code="0001",
    )
    baker.make(
        TransactionSearch,
        transaction_id=4,
        award_id=award_grant.award_id,
        award_category="grant",
        federal_action_obligation=103,
        generated_pragmatic_obligation=103,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        award_date_signed="2021-04-01",
        awarding_agency_id=awarding_agency_1.id,
        awarding_toptier_agency_id=awarding_agency_1.id,
        is_fpds=False,
        type="04",
        awarding_agency_code="001",
        awarding_toptier_agency_name="Agency 1",
        awarding_sub_tier_agency_c="0001",
        awarding_subtier_agency_name="Sub-Agency 1",
        awarding_subtier_agency_abbreviation="A1",
        awarding_office_name="Office 2",
        awarding_office_code="0002",
    )
    baker.make(
        TransactionSearch,
        transaction_id=5,
        award_id=award_loan.award_id,
        award_category="loans",
        federal_action_obligation=104,
        generated_pragmatic_obligation=104,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        award_date_signed="2021-04-01",
        awarding_agency_id=awarding_agency_1.id,
        awarding_toptier_agency_id=awarding_agency_1.id,
        is_fpds=False,
        type="09",
        awarding_agency_code="001",
        awarding_toptier_agency_name="Agency 1",
        awarding_sub_tier_agency_c="0001",
        awarding_subtier_agency_name="Sub-Agency 1",
        awarding_subtier_agency_abbreviation="A1",
        awarding_office_name="Office 2",
        awarding_office_code="0002",
    )
    baker.make(
        TransactionSearch,
        transaction_id=6,
        award_id=award_dp.award_id,
        award_category="direct payment",
        federal_action_obligation=105,
        generated_pragmatic_obligation=105,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        award_date_signed="2021-04-01",
        awarding_agency_id=awarding_agency_1.id,
        awarding_toptier_agency_id=awarding_agency_1.id,
        is_fpds=False,
        type="10",
        awarding_agency_code="001",
        awarding_toptier_agency_name="Agency 1",
        awarding_sub_tier_agency_c="0001",
        awarding_subtier_agency_name="Sub-Agency 1",
        awarding_subtier_agency_abbreviation="A1",
        awarding_office_name="Office 2",
        awarding_office_code="0002",
    )
    # Alternate Year
    baker.make(
        TransactionSearch,
        transaction_id=7,
        award_id=award_idv.award_id,
        award_category="direct payment",
        federal_action_obligation=300,
        generated_pragmatic_obligation=300,
        action_date="2020-04-01",
        fiscal_action_date="2020-07-01",
        award_date_signed="2020-04-01",
        awarding_agency_id=awarding_agency_1.id,
        awarding_toptier_agency_id=awarding_agency_1.id,
        is_fpds=True,
        awarding_agency_code="001",
        awarding_toptier_agency_name="Agency 1",
        awarding_sub_tier_agency_c="0001",
        awarding_subtier_agency_name="Sub-Agency 1",
        awarding_subtier_agency_abbreviation="A1",
        awarding_office_name="Office 1",
        awarding_office_code="0001",
    )
    # Alternate Agency
    baker.make(
        TransactionSearch,
        transaction_id=8,
        award_id=award_idv.award_id,
        award_category="idv",
        federal_action_obligation=400,
        generated_pragmatic_obligation=400,
        action_date="2021-04-01",
        fiscal_action_date="2021-07-01",
        award_date_signed="2020-04-01",
        awarding_agency_id=awarding_agency_2.id,
        awarding_toptier_agency_id=awarding_agency_2.id,
        is_fpds=True,
        awarding_agency_code="002",
        awarding_toptier_agency_name="Agency 2",
        awarding_sub_tier_agency_c="0002",
        awarding_subtier_agency_name="Sub-Agency 2",
        awarding_subtier_agency_abbreviation="A2",
        awarding_office_name="Office 2",
        awarding_office_code="0002",
    )
