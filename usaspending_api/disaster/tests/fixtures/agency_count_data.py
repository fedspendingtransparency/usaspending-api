import pytest

from model_bakery import baker

from usaspending_api.references.models import ToptierAgency
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.references.models import Agency


@pytest.fixture
def faba_with_toptier_agencies(award_count_sub_schedule, award_count_submission, defc_codes):
    toptier_agency(1)
    award1 = award_with_toptier_agency(1, 8, 0)

    toptier_agency(2)
    award2 = award_with_toptier_agency(2, 0, 7)
    award3 = baker.make(
        "search.AwardSearch",
        award_id=1,
        type="A",
        funding_agency_id=Agency.objects.first().id,
        funding_toptier_agency_code="001",
        total_loan_value=0,
        disaster_emergency_fund_codes=["M"],
        total_covid_obligation=8,
        total_covid_outlay=0,
        spending_by_defc=[{"defc": "M", "outlay": 0, "obligaton": 8}],
        action_date="2020-10-01",
    )

    faba_for_award(award1, 8, 0)
    faba_for_award(award2, 0, 7)
    faba_for_award(award3, 8, 0)


@pytest.fixture
def faba_with_toptier_agencies_that_cancel_out_in_toa(award_count_sub_schedule, award_count_submission, defc_codes):
    toptier_agency(1)
    award1 = award_with_toptier_agency(1)

    faba_for_award(award1, 8, 0)
    faba_for_award(award1, -5, 0)
    faba_for_award(award1, -3, 0)


@pytest.fixture
def faba_with_toptier_agencies_that_cancel_out_in_outlay(award_count_sub_schedule, award_count_submission, defc_codes):
    toptier_agency(1)
    award1 = award_with_toptier_agency(1)

    faba_for_award(award1, 0, 8)
    faba_for_award(award1, 0, -5)
    faba_for_award(award1, 0, -3)


def faba_for_award(award, toa, outlay):
    defc_m = baker.make(
        "references.DisasterEmergencyFundCode",
        code="M",
        public_law="PUBLIC LAW FOR CODE M",
        title="TITLE FOR CODE M",
        group_name="covid_19",
    )
    fa1 = baker.make(
        "accounts.FederalAccount",
        federal_account_code="001-0000",
        account_title="FA 1",
        parent_toptier_agency=ToptierAgency.objects.get(
            pk=Agency.objects.get(id=award.funding_agency_id).toptier_agency_id
        ),
    )
    tas1 = baker.make(
        "accounts.TreasuryAppropriationAccount",
        funding_toptier_agency=ToptierAgency.objects.get(
            pk=Agency.objects.get(id=award.funding_agency_id).toptier_agency_id
        ),
        budget_function_code=100,
        budget_function_title="NAME 1",
        budget_subfunction_code=1100,
        budget_subfunction_title="NAME 1A",
        federal_account=fa1,
        account_title="TA 1",
        tas_rendering_label="001-X-0000-000",
    )
    return baker.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        treasury_account=tas1,
        parent_award_id="basic award",
        disaster_emergency_fund=defc_m,
        submission=SubmissionAttributes.objects.filter(reporting_fiscal_year=2022, reporting_fiscal_period=8).first(),
        transaction_obligated_amount=toa,
        gross_outlay_amount_by_award_cpe=outlay,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=0,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )


def toptier_agency(id):
    return baker.make(
        "references.ToptierAgency",
        pk=id,
        name=f"Agency {id}",
        toptier_code=f"{id}",
    )


def award_with_toptier_agency(id, toa=0, outlay=0):
    agency = baker.make("references.Agency", toptier_agency_id=id, toptier_flag=True, _fill_optional=True)
    a1 = baker.make(
        "search.AwardSearch",
        award_id=id,
        type="A",
        funding_agency_id=agency.id,
        funding_toptier_agency_code=f"00{agency.id}",
        total_loan_value=0,
        latest_transaction_id=id,
        disaster_emergency_fund_codes=["M"],
        total_covid_obligation=toa,
        total_covid_outlay=outlay,
        spending_by_defc=[{"defc": "M", "outlay": outlay, "obligaton": toa}],
        action_date="2020-10-01",
    )
    baker.make(
        "search.TransactionSearch",
        transaction_id=id,
        award=a1,
        action_date="2020-04-01",
        is_fpds=True,
        funding_agency_id=agency.id,
    )

    return a1
