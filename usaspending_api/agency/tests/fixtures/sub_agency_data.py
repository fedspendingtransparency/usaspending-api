import pytest

from model_bakery import baker

from usaspending_api.awards.models import TransactionNormalized, TransactionFPDS, TransactionFABS


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
    toptier_agency_1 = baker.make("references.ToptierAgency", toptier_code="001", name="Agency 1")
    toptier_agency_2 = baker.make("references.ToptierAgency", toptier_code="002", name="Agency 2")
    toptier_agency_3 = baker.make("references.ToptierAgency", toptier_code="003", name="Agency 3")
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
        "awards.Award",
        category="contract",
        date_signed="2021-04-01",
    )
    award_idv = baker.make(
        "awards.Award",
        category="idv",
        date_signed="2020-04-01",
    )
    award_grant = baker.make(
        "awards.Award",
        category="grant",
        date_signed="2021-04-01",
    )
    award_loan = baker.make(
        "awards.Award",
        category="loans",
        date_signed="2021-04-01",
    )
    award_dp = baker.make(
        "awards.Award",
        category="direct payment",
        date_signed="2021-04-01",
    )

    contract_transaction = baker.make(
        TransactionNormalized,
        award=award_contract,
        federal_action_obligation=101,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        funding_agency=awarding_agency_1,
        is_fpds=True,
        type="A",
    )
    baker.make(
        TransactionFPDS,
        transaction=contract_transaction,
        awarding_agency_code="001",
        funding_agency_code="001",
        awarding_sub_tier_agency_c="0001",
        funding_sub_tier_agency_co="0001",
        awarding_office_code="0001",
        funding_office_code="0002",
    )

    no_office_transaction = baker.make(
        TransactionNormalized,
        award=award_contract,
        federal_action_obligation=110,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_3,
        funding_agency=awarding_agency_3,
        is_fpds=True,
        type="B",
    )
    baker.make(
        TransactionFPDS,
        transaction=no_office_transaction,
        awarding_agency_code="003",
        funding_agency_code="003",
        awarding_sub_tier_agency_c="0003",
        funding_sub_tier_agency_co="0003",
        awarding_office_name=None,
        awarding_office_code=None,
        funding_office_name=None,
        funding_office_code=None,
    )

    idv_transaction = baker.make(
        TransactionNormalized,
        award=award_idv,
        federal_action_obligation=102,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        is_fpds=True,
        type="IDV_A",
    )
    baker.make(
        TransactionFPDS,
        transaction=idv_transaction,
        awarding_agency_code="001",
        awarding_sub_tier_agency_c="0001",
        awarding_office_code="0001",
    )

    grant_transaction = baker.make(
        TransactionNormalized,
        award=award_grant,
        federal_action_obligation=103,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        is_fpds=False,
        type="04",
    )
    baker.make(
        TransactionFABS,
        transaction=grant_transaction,
        awarding_agency_code="001",
        awarding_sub_tier_agency_c="0001",
        awarding_office_code="0002",
    )
    loan_transaction = baker.make(
        TransactionNormalized,
        award=award_loan,
        federal_action_obligation=104,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        is_fpds=False,
        type="09",
    )
    baker.make(
        TransactionFABS,
        transaction=loan_transaction,
        awarding_agency_code="001",
        awarding_sub_tier_agency_c="0001",
        awarding_office_name="Office 2",
        awarding_office_code="0002",
    )
    directpayment_transaction = baker.make(
        TransactionNormalized,
        award=award_dp,
        federal_action_obligation=105,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_1,
        is_fpds=False,
        type="10",
    )
    baker.make(
        TransactionFABS,
        transaction=directpayment_transaction,
        awarding_agency_code="001",
        awarding_sub_tier_agency_c="0001",
        awarding_office_code="0002",
    )

    # Alternate Year
    idv_2 = baker.make(
        TransactionNormalized,
        award=award_idv,
        federal_action_obligation=300,
        action_date="2020-04-01",
        awarding_agency=awarding_agency_1,
        is_fpds=True,
    )
    baker.make(
        TransactionFPDS,
        transaction=idv_2,
        awarding_agency_code="001",
        awarding_sub_tier_agency_c="0001",
        awarding_office_code="0001",
    )

    # Alternate Agency
    idv_3 = baker.make(
        TransactionNormalized,
        award=award_idv,
        federal_action_obligation=400,
        action_date="2021-04-01",
        awarding_agency=awarding_agency_2,
        is_fpds=True,
    )
    baker.make(
        TransactionFPDS,
        transaction=idv_3,
        awarding_agency_code="002",
        awarding_sub_tier_agency_c="0002",
        awarding_office_code="0002",
    )
