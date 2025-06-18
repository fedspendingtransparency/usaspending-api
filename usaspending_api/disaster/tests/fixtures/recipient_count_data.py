import pytest

from model_bakery import baker

from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.fixture
def basic_fabs_award(award_count_sub_schedule, award_count_submission, defc_codes):
    _normal_faba(_normal_fabs(1))
    baker.make(
        "recipient.RecipientLookup",
        recipient_hash="c229f674-92c9-1128-7bdf-292fb3e4226b",
        uei="1",
        legal_business_name="Recipient 1",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_hash="c229f674-92c9-1128-7bdf-292fb3e4226b",
        uei="1",
        recipient_level="R",
    )


@pytest.fixture
def basic_fpds_award(award_count_sub_schedule, award_count_submission, defc_codes):
    transaction_fpds = baker.make(
        "search.TransactionSearch",
        transaction_id=2,
        type="A",
        award_id=100,
        action_date="2022-05-01",
        fiscal_action_date="2022-09-01",
        is_fpds=True,
        recipient_hash="053cca9b-ad9c-09ee-4b4b-243fa59f0be2",
        recipient_uei="fpds",
        recipient_name="Recipient FPDS",
        recipient_name_raw="Recipient FPDS",
        recipient_levels=["R"],
    )
    _normal_faba(
        baker.make(
            "search.AwardSearch",
            award_id=100,
            latest_transaction_id=transaction_fpds.transaction_id,
            type="A",
            is_fpds=True,
            action_date="2020-10-01",
            recipient_hash="053cca9b-ad9c-09ee-4b4b-243fa59f0be2",
            recipient_uei="fpds",
            recipient_name="Recipient FPDS",
            recipient_levels=["R"],
            disaster_emergency_fund_codes=["M"],
            total_covid_outlay=8,
            total_covid_obligation=0,
            spending_by_defc=[{"defc": "M", "outlay": 8, "obligation": 0}],
        )
    )
    baker.make(
        "recipient.RecipientLookup",
        recipient_hash="053cca9b-ad9c-09ee-4b4b-243fa59f0be2",
        uei="fpds",
        legal_business_name="Recipient FPDS",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_hash="053cca9b-ad9c-09ee-4b4b-243fa59f0be2",
        uei="fpds",
        recipient_level="R",
    )


@pytest.fixture
def double_fpds_awards_with_distinct_recipients(award_count_sub_schedule, award_count_submission, defc_codes):

    transaction_fpds_1 = baker.make(
        "search.TransactionSearch",
        transaction_id=3,
        award_id=200,
        action_date="2022-05-01",
        fiscal_action_date="2022-09-01",
        is_fpds=True,
        recipient_unique_id="1",
        recipient_hash="053cca9b-ad9c-09ee-4b4b-243fa59f0be2",
        recipient_uei="fpds",
        recipient_levels=["R"],
    )
    _normal_faba(
        baker.make(
            "search.AwardSearch",
            award_id=200,
            latest_transaction_id=transaction_fpds_1.transaction_id,
            type="A",
            action_date="2020-10-01",
            is_fpds=True,
            recipient_unique_id="1",
            recipient_hash="053cca9b-ad9c-09ee-4b4b-243fa59f0be2",
            recipient_uei="fpds",
            recipient_name="Recipient A",
            disaster_emergency_fund_codes=["M"],
            total_covid_outlay=8,
            total_covid_obligation=0,
            spending_by_defc=[{"defc": "M", "outlay": 8, "obligation": 0}],
        )
    )

    baker.make(
        "recipient.RecipientLookup",
        recipient_hash="053cca9b-ad9c-09ee-4b4b-243fa59f0be2",
        uei="fpds",
        legal_business_name="Recipient FPDS",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_hash="053cca9b-ad9c-09ee-4b4b-243fa59f0be2",
        uei="fpds",
        recipient_level="R",
    )

    transaction_fpds_2 = baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        award_id=300,
        action_date="2022-05-01",
        fiscal_action_date="2022-09-01",
        is_fpds=True,
        recipient_unique_id="2",
        recipient_uei="2",
        recipient_hash="dfdbbb4f-1a81-1232-84b0-341e93d0acb1",
        recipient_levels=["R"],
    )
    _normal_faba(
        baker.make(
            "search.AwardSearch",
            award_id=300,
            latest_transaction_id=transaction_fpds_2.transaction_id,
            type="A",
            action_date="2020-02-02",
            is_fpds=True,
            recipient_unique_id="2",
            recipient_uei="2",
            recipient_hash="dfdbbb4f-1a81-1232-84b0-341e93d0acb1",
            recipient_levels=["R"],
            recipient_name="Recipient 2",
            disaster_emergency_fund_codes=["M"],
            total_covid_outlay=8,
            total_covid_obligation=0,
            spending_by_defc=[{"defc": "M", "outlay": 8, "obligation": 0}],
        )
    )

    baker.make(
        "recipient.RecipientLookup",
        recipient_hash="dfdbbb4f-1a81-1232-84b0-341e93d0acb1",
        uei="2",
        legal_business_name="Recipient 2",
    )
    baker.make(
        "recipient.RecipientProfile",
        recipient_hash="dfdbbb4f-1a81-1232-84b0-341e93d0acb1",
        uei="2",
        recipient_level="R",
    )


@pytest.fixture
def double_fpds_awards_with_same_recipients(award_count_sub_schedule, award_count_submission, defc_codes):
    transaction_fpds_1 = baker.make(
        "search.TransactionSearch",
        transaction_id=4,
        award_id=400,
        action_date="2022-05-01",
        is_fpds=True,
        recipient_unique_id="1",
    )
    _normal_faba(
        baker.make(
            "search.AwardSearch",
            award_id=400,
            latest_transaction_id=transaction_fpds_1.transaction_id,
            type="A",
            action_date="2020-10-01",
            recipient_unique_id="1",
            recipient_hash="1f1f91da-98b8-6f14-c662-55137ba8adec",
            recipient_name="Recipient 1",
            disaster_emergency_fund_codes=["M"],
            total_covid_outlay=8,
            total_covid_obligation=0,
            spending_by_defc=[{"defc": "M", "outlay": 8, "obligation": 0}],
        )
    )
    transaction_fpds_2 = baker.make(
        "search.TransactionSearch",
        transaction_id=6,
        award_id=500,
        action_date="2022-05-01",
        fiscal_action_date="2022-09-01",
        is_fpds=True,
        recipient_unique_id="1",
    )
    _normal_faba(
        baker.make(
            "search.AwardSearch",
            award_id=500,
            latest_transaction_id=transaction_fpds_2.transaction_id,
            type="A",
            action_date="2020-10-01",
            recipient_unique_id="1",
            recipient_hash="1f1f91da-98b8-6f14-c662-55137ba8adec",
            recipient_name="Recipient 1",
            disaster_emergency_fund_codes=["M"],
            total_covid_outlay=8,
            total_covid_obligation=0,
            spending_by_defc=[{"defc": "M", "outlay": 8, "obligation": 0}],
        )
    )


@pytest.fixture
def double_fpds_awards_with_same_special_case_recipients(award_count_sub_schedule, award_count_submission, defc_codes):
    transaction_fpds_1 = baker.make(
        "search.TransactionSearch",
        transaction_id=7,
        award_id=600,
        type="A",
        action_date="2022-05-01",
        fiscal_action_date="2022-09-01",
        is_fpds=True,
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_unique_id="123",
        recipient_hash="01c03484-d1bd-41cc-2aca-4b427a2d0611",
    )
    _normal_faba(
        baker.make(
            "search.AwardSearch",
            award_id=600,
            latest_transaction_id=transaction_fpds_1.transaction_id,
            type="A",
            action_date="2020-10-01",
            is_fpds=True,
            recipient_name="MULTIPLE RECIPIENTS",
            recipient_unique_id="123",
            recipient_hash="01c03484-d1bd-41cc-2aca-4b427a2d0611",
            disaster_emergency_fund_codes=["M"],
            total_covid_outlay=8,
            total_covid_obligation=0,
            spending_by_defc=[{"defc": "M", "outlay": 8, "obligation": 0}],
        )
    )

    transaction_fpds_2 = baker.make(
        "search.TransactionSearch",
        transaction_id=8,
        award_id=700,
        type="A",
        action_date="2022-05-01",
        fiscal_action_date="2022-09-01",
        is_fpds=True,
        recipient_name="MULTIPLE RECIPIENTS",
        recipient_unique_id="456",
    )
    _normal_faba(
        baker.make(
            "search.AwardSearch",
            award_id=700,
            latest_transaction_id=transaction_fpds_2.transaction_id,
            type="A",
            action_date="2020-10-01",
            is_fpds=True,
            recipient_name="MULTIPLE RECIPIENTS",
            recipient_unique_id="456",
            recipient_hash="1c4e7c2a-efe3-1b7e-2190-6f4487f808ac",
            disaster_emergency_fund_codes=["M"],
            total_covid_outlay=8,
            total_covid_obligation=0,
            spending_by_defc=[{"defc": "M", "outlay": 8, "obligation": 0}],
        )
    )


@pytest.fixture
def award_with_no_outlays(award_count_sub_schedule, award_count_submission, defc_codes):
    baker.make(
        "awards.FinancialAccountsByAwards",
        award=_normal_fabs(1),
        piid="piid 1",
        parent_award_id="same parent award",
        fain="fain 1",
        uri="uri 1",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        gross_outlay_amount_by_award_cpe=0,
    )


@pytest.fixture
def fabs_award_with_quarterly_submission(award_count_sub_schedule, award_count_quarterly_submission, defc_codes):
    _normal_faba(_normal_fabs(1))


@pytest.fixture
def fabs_award_with_old_submission(defc_codes, award_count_sub_schedule):
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=20220300,
        is_quarter=False,
        submission_fiscal_year=2022,
        submission_fiscal_quarter=3,
        submission_fiscal_month=3,
        submission_reveal_date="2020-5-15",
    )
    old_submission = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=3,
        quarter_format_flag=False,
        reporting_period_start="2022-04-01",
        submission_window_id=20220300,
    )
    ta1 = baker.make(
        "references.ToptierAgency",
        name=f"Agency 001",
        toptier_code=f"001",
    )
    baker.make("references.Agency", toptier_agency=ta1, toptier_flag=True, _fill_optional=True)
    defc_m = baker.make(
        "references.DisasterEmergencyFundCode",
        code="M",
        public_law="PUBLIC LAW FOR CODE M",
        title="TITLE FOR CODE M",
        group_name="covid_19",
    )
    fa1 = baker.make(
        "accounts.FederalAccount", federal_account_code="001-0000", account_title="FA 1", parent_toptier_agency=ta1
    )
    tas1 = baker.make(
        "accounts.TreasuryAppropriationAccount",
        budget_function_code=100,
        budget_function_title="NAME 1",
        budget_subfunction_code=1100,
        budget_subfunction_title="NAME 1A",
        account_title="TA 1",
        tas_rendering_label="001-X-0000-000",
        federal_account=fa1,
        funding_toptier_agency=ta1,
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award=_normal_fabs(1),
        treasury_account=tas1,
        piid="piid 1",
        parent_award_id="same parent award",
        fain="fain 1",
        uri="uri 1",
        disaster_emergency_fund=defc_m,
        submission=old_submission,
        transaction_obligated_amount=8,
    )


@pytest.fixture
def fabs_award_with_unclosed_submission(defc_codes, award_count_sub_schedule):
    old_submission = baker.make(
        "submissions.SubmissionAttributes",
        reporting_fiscal_year=2022,
        reporting_fiscal_period=11,
        quarter_format_flag=False,
        reporting_period_start="2022-11-01",
    )
    baker.make(
        "awards.FinancialAccountsByAwards",
        award=_normal_fabs(1),
        piid="piid 1",
        parent_award_id="same parent award",
        fain="fain 1",
        uri="uri 1",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=old_submission,
        gross_outlay_amount_by_award_cpe=8,
    )


def _normal_faba(award):
    ta1 = baker.make(
        "references.ToptierAgency",
        name=f"Agency 00{award.award_id}",
        toptier_code=f"00{award.award_id}",
    )
    baker.make("references.Agency", toptier_agency=ta1, toptier_flag=True, _fill_optional=True)
    defc_m = baker.make(
        "references.DisasterEmergencyFundCode",
        code="M",
        public_law="PUBLIC LAW FOR CODE M",
        title="TITLE FOR CODE M",
        group_name="covid_19",
    )
    fa1 = baker.make(
        "accounts.FederalAccount", federal_account_code="001-0000", account_title="FA 1", parent_toptier_agency=ta1
    )
    tas1 = baker.make(
        "accounts.TreasuryAppropriationAccount",
        budget_function_code=100,
        budget_function_title="NAME 1",
        budget_subfunction_code=1100,
        budget_subfunction_title="NAME 1A",
        account_title="TA 1",
        tas_rendering_label="001-X-0000-000",
        federal_account=fa1,
        funding_toptier_agency=ta1,
    )
    return baker.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        treasury_account=tas1,
        piid="piid 1",
        parent_award_id="same parent award",
        fain="fain 1",
        uri="uri 1",
        disaster_emergency_fund=defc_m,
        submission=SubmissionAttributes.objects.all().first(),
        gross_outlay_amount_by_award_cpe=8,
    )


def _normal_fabs(id):
    fabs_award = baker.make(
        "search.AwardSearch",
        award_id=id,
        latest_transaction_id=id,
        type="07",
        is_fpds=False,
        action_date="2020-10-01",
        recipient_uei="1",
        recipient_hash="c229f674-92c9-1128-7bdf-292fb3e4226b",
        recipient_name="Recipient 1",
        recipient_levels=["R"],
        disaster_emergency_fund_codes=["M"],
        total_covid_outlay=8,
        total_covid_obligation=0,
        spending_by_defc=[{"defc": "M", "outlay": 8, "obligation": 0}],
    )
    baker.make(
        "search.TransactionSearch",
        type="07",
        transaction_id=id,
        award_id=fabs_award.award_id,
        action_date="2022-05-01",
        fiscal_action_date="2022-05-01",
        recipient_uei="1",
        recipient_hash="c229f674-92c9-1128-7bdf-292fb3e4226b",
        recipient_name="Recipient 1",
        recipient_levels=["R"],
        is_fpds=False,
    )
    return fabs_award
