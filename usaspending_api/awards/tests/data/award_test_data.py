"""
The goal of this file is to create test data that can be used for
all test_awards files similar to what idv_test_data.py does for
IDV tests. Any changes to this file will need to be addressed in
the test_awards files found within /awards/tests/.
"""

from model_bakery import baker
import pytest

from usaspending_api.submissions.models.dabs_submission_window_schedule import DABSSubmissionWindowSchedule

AWARD_COUNT = 4
RECIPIENT_HASH_PREFIX = "d0de516c-54af-4999-abda-428ce877"
AGENCY_COUNT_BY_AWARD_ID = {
    0: {"awarding": 0, "funding": 0},
    1: {"awarding": 1, "funding": 0},
    2: {"awarding": 0, "funding": 1},
    3: {"awarding": 0, "funding": 0},
    4: {"awarding": 1, "funding": 1},
}
OBLIGATED_AMOUNT_BY_AWARD_ID = {0: 0.0, 1: 110011.0, 2: 430043.0, 3: 960096.0, 4: 1700170.0}

DATE_IN_THE_PAST = "1776-07-04"
DATE_IN_THE_FUTURE = "2553-04-01"


@pytest.fixture
def create_award_test_data():
    standard_sub_window_schedule(DATE_IN_THE_PAST)

    for award_id in range(1, AWARD_COUNT + 1):
        award_from_id(award_id)


@pytest.fixture
def award_with_unreleased_submissions():
    standard_sub_window_schedule(DATE_IN_THE_FUTURE)

    award_from_id(1)


@pytest.fixture
def award_with_released_submissions():
    standard_sub_window_schedule(DATE_IN_THE_PAST)

    award_from_id(1)


def award_from_id(award_id):
    # This is intended to be grafted into strings so we will pad with
    # zeros in case there's any sorting going on.
    string_award_id = str(award_id).zfill(3)

    # Awarding agency
    awarding_toptier_agency = baker.make(
        "references.ToptierAgency",
        toptier_agency_id=8500 + award_id,
        toptier_code=str(award_id).zfill(3),
        name="Toptier Awarding Agency Name %s" % (8500 + award_id),
    )

    awarding_agency = baker.make(
        "references.Agency",
        id=8000 + award_id,
        toptier_flag=True,
        toptier_agency_id=awarding_toptier_agency.toptier_agency_id,
    )

    # Funding agency
    funding_toptier_agency = baker.make(
        "references.ToptierAgency",
        toptier_agency_id=9500 + award_id,
        toptier_code=str(100 + award_id).zfill(3),
        name="Toptier Funding Agency Name %s" % (9500 + award_id),
    )

    funding_agency = baker.make(
        "references.Agency",
        id=9000 + award_id,
        toptier_flag=True,
        toptier_agency_id=funding_toptier_agency.toptier_agency_id,
    )

    baker.make(
        "search.TransactionSearch",
        transaction_id=7000 + award_id,
        award_id=award_id,
        funding_toptier_agency_name="subtier_funding_agency_name_%s" % (7000 + award_id),
        ordering_period_end_date="2018-01-%02d" % award_id,
        recipient_unique_id="duns_%s" % (7000 + award_id),
        period_of_perf_potential_e="2018-08-%02d" % award_id,
    )

    baker.make(
        "search.AwardSearch",
        award_id=award_id,
        generated_unique_award_id="GENERATED_UNIQUE_AWARD_ID_%s" % string_award_id,
        piid="piid_%s" % string_award_id,
        type_description="type_description_%s" % string_award_id,
        description="description_%s" % string_award_id,
        fpds_agency_id="fpds_agency_id_%s" % string_award_id,
        awarding_agency_id=awarding_agency.id if award_id % 3 == 1 else None,
        funding_agency_id=funding_agency.id if (award_id + 1) % 2 == 1 else None,
        latest_transaction_id=7000 + award_id,
        total_obligation=100000 + award_id,
        base_and_all_options_value=500000 + award_id,
        period_of_performance_current_end_date="2018-03-%02d" % award_id,
        period_of_performance_start_date="2018-02-%02d" % award_id,
    )

    submission_attributes = baker.make(
        "submissions.SubmissionAttributes",
        submission_id=1100 + award_id,
        submission_window=DABSSubmissionWindowSchedule.objects.filter(
            submission_fiscal_month=award_id % 12 + 1
        ).first(),
        reporting_fiscal_year=2100,
        reporting_fiscal_period=award_id % 12 + 1,
        reporting_fiscal_quarter=(award_id % 12 + 3) // 3,
        quarter_format_flag=bool(award_id % 2),
        toptier_code=str(100 + award_id).zfill(3),
    )

    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=1000 + award_id,
        submission_window=DABSSubmissionWindowSchedule.objects.filter(
            submission_fiscal_month=award_id % 12 + 1
        ).first(),
        reporting_fiscal_year=2100,
        reporting_fiscal_period=award_id % 12 + 1,
        reporting_fiscal_quarter=(award_id % 12 + 3) // 3,
        quarter_format_flag=bool(award_id % 2),
        toptier_code=str(award_id).zfill(3),
    )

    # Create variable number of federal accounts for variance in awards
    for federal_account_count in range(1, award_id + 1):
        federal_account_id = award_id * 10 + federal_account_count
        string_federal_account_id = str(federal_account_id).zfill(3)

        federal_account = baker.make(
            "accounts.FederalAccount",
            id=2000 + federal_account_id,
            agency_identifier=funding_toptier_agency.toptier_code,
            main_account_code=str(federal_account_id).zfill(4),
            account_title="federal_account_title_%s" % (2000 + federal_account_id),
            federal_account_code=funding_toptier_agency.toptier_code + "-" + str(federal_account_id).zfill(4),
        )

        treasury_appropriation_account = baker.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=3000 + federal_account_id,
            federal_account_id=federal_account.id,
            reporting_agency_id=awarding_toptier_agency.toptier_code,
            reporting_agency_name="reporting_agency_name_%s" % awarding_toptier_agency.toptier_code,
            agency_id=federal_account.agency_identifier,
            main_account_code=federal_account.main_account_code,
            account_title="treasury_appropriation_account_title_%s" % string_federal_account_id,
            awarding_toptier_agency_id=awarding_toptier_agency.toptier_agency_id,
            funding_toptier_agency_id=funding_toptier_agency.toptier_agency_id,
        )

        ref_program_activity = baker.make(
            "references.RefProgramActivity",
            id=4000 + federal_account_id,
            program_activity_code=str(4000 + federal_account_id),
            program_activity_name="program_activity_%s" % (4000 + federal_account_id),
        )

        object_class = baker.make(
            "references.ObjectClass",
            id=5000 + federal_account_id,
            object_class=5000 + federal_account_id,
            object_class_name="object_class_%s" % (5000 + federal_account_id),
        )

        baker.make(
            "awards.FinancialAccountsByAwards",
            financial_accounts_by_awards_id=6000 + federal_account_id,
            award_id=award_id,
            submission_id=submission_attributes.submission_id,
            treasury_account_id=treasury_appropriation_account.treasury_account_identifier,
            program_activity_id=ref_program_activity.id,
            object_class_id=object_class.id,
            transaction_obligated_amount=10000 * federal_account_id + federal_account_id,
        )

    baker.make(
        "recipient.RecipientLookup",
        id=7000 + award_id,
        recipient_hash=RECIPIENT_HASH_PREFIX + str(7000 + award_id),
        legal_business_name="recipient_name_%s" % (7000 + award_id),
        duns="duns_%s" % (7000 + award_id),
    )

    baker.make(
        "recipient.RecipientProfile",
        id=8000 + award_id,
        recipient_hash=RECIPIENT_HASH_PREFIX + str(7000 + award_id),
        recipient_level="R",
        recipient_name="recipient_name_%s" % (7000 + award_id),
        recipient_unique_id="duns_%s" % (7000 + award_id),
    )


def standard_sub_window_schedule(date):
    for month in range(1, 13):
        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            submission_fiscal_year=2100,
            submission_fiscal_month=month,
            submission_fiscal_quarter=month // 3,
            is_quarter=True,
            submission_reveal_date=date,
        )

        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            submission_fiscal_year=2100,
            submission_fiscal_month=month,
            submission_fiscal_quarter=month // 3,
            is_quarter=False,
            submission_reveal_date=date,
        )
