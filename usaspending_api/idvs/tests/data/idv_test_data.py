"""
The goal here is to create test data for ALL IDV TESTS so we don't end up
with a bunch of similar test data creation code floating around.

You'll have to use your imagination a bit with these budget tree drawings.
These are the two hierarchies being built by this function.  "I" means IDV.
"C" means contract.  The number is the award id.  So in this drawing, I1 is
the parent of I3, I4, I5, and C6.  I2 is the grandparent of C11, C12, C13,
C14, and C15.  Please note that the C9 -> C15 relationship is actually invalid
in the IDV world but has been added for testing purposes.  Hope this helps.

          I1                                        I2
  I3   I4   I5   C6                  I7        I8        C9        C10
                                  C11 C12   C13 C14      C15
"""
from model_mommy import mommy


AWARD_COUNT = 15
IDVS = (1, 2, 3, 4, 5, 7, 8)
PARENTS = {3: 1, 4: 1, 5: 1, 6: 1, 7: 2, 8: 2, 9: 2, 10: 2, 11: 7, 12: 7, 13: 8, 14: 8, 15: 9}
RECIPIENT_HASH_PREFIX = "d0de516c-54af-4999-abda-428ce877"


def create_idv_test_data():
    # mommy.make('references.Agency', id=12000)

    # You'll see a bunch of weird math and such in the code that follows.  The
    # goal is to try to mix values up a bit.  We don't want values to overlap
    # TOO much else it becomes difficult to ensure that a value came from a
    # specific source.  For example, if every dollar figure returned $100, how
    # would we know for sure the $100 returned by our API endpoint actually came
    # from base_and_all_options and not base_exercised_options_val?
    for award_id in range(1, AWARD_COUNT + 1):

        parent_award_id = PARENTS.get(award_id)

        # These are intended to be grafted into strings so we will pad with
        # zeros in case there's any sorting going on.
        string_parent_award_id = str(parent_award_id).zfill(3) if parent_award_id else None
        string_award_id = str(award_id).zfill(3)

        # Awarding agency
        awarding_toptier_agency = mommy.make(
            "references.ToptierAgency",
            toptier_agency_id=8500 + award_id,
            cgac_code=str(award_id).zfill(3),
            name="toptier_awarding_agency_name_%s" % (8500 + award_id),
        )

        awarding_agency = mommy.make(
            "references.Agency",
            id=8000 + award_id,
            toptier_flag=True,
            toptier_agency_id=awarding_toptier_agency.toptier_agency_id,
        )

        # Funding agency
        funding_toptier_agency = mommy.make(
            "references.ToptierAgency",
            toptier_agency_id=9500 + award_id,
            cgac_code=str(100 + award_id).zfill(3),
            name="toptier_funding_agency_name_%s" % (9500 + award_id),
        )

        funding_agency = mommy.make(
            "references.Agency",
            id=9000 + award_id,
            toptier_flag=True,
            toptier_agency_id=funding_toptier_agency.toptier_agency_id,
        )

        transaction_normalized = mommy.make("awards.TransactionNormalized", id=7000 + award_id, award_id=award_id)

        mommy.make(
            "awards.TransactionFPDS",
            transaction_id=transaction_normalized.id,
            funding_agency_name="subtier_funding_agency_name_%s" % transaction_normalized.id,
            ordering_period_end_date="2018-01-%02d" % award_id,
            awardee_or_recipient_uniqu="duns_%s" % (7000 + award_id),
            period_of_perf_potential_e="2018-08-%02d" % award_id,
        )

        mommy.make(
            "awards.Award",
            id=award_id,
            generated_unique_award_id="GENERATED_UNIQUE_AWARD_ID_%s" % string_award_id,
            type=("IDV_%s" if award_id in IDVS else "CONTRACT_%s") % string_award_id,
            piid="piid_%s" % string_award_id,
            type_description="type_description_%s" % string_award_id,
            description="description_%s" % string_award_id,
            fpds_agency_id="fpds_agency_id_%s" % string_award_id,
            parent_award_piid=("piid_%s" % string_parent_award_id) if string_parent_award_id else None,
            fpds_parent_agency_id=("fpds_agency_id_%s" % string_parent_award_id) if string_parent_award_id else None,
            awarding_agency_id=awarding_agency.id,
            funding_agency_id=funding_agency.id,
            latest_transaction_id=transaction_normalized.id,
            total_obligation=100000 + award_id,
            base_and_all_options_value=500000 + award_id,
            period_of_performance_current_end_date="2018-03-%02d" % award_id,
            period_of_performance_start_date="2018-02-%02d" % award_id,
        )

        submission_attributes = mommy.make(
            "submissions.SubmissionAttributes",
            submission_id=1000 + award_id,
            reporting_fiscal_year=2000 + award_id,
            reporting_fiscal_quarter=award_id % 4 + 1,
        )

        federal_account = mommy.make(
            "accounts.FederalAccount",
            id=2000 + award_id,
            agency_identifier=funding_toptier_agency.cgac_code,
            main_account_code=str(award_id).zfill(4),
            account_title="federal_account_title_%s" % (2000 + award_id),
            federal_account_code=funding_toptier_agency.cgac_code + "-" + str(award_id).zfill(4),
        )

        treasury_appropriation_account = mommy.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=3000 + award_id,
            federal_account_id=federal_account.id,
            reporting_agency_id=awarding_toptier_agency.cgac_code,
            reporting_agency_name="reporting_agency_name_%s" % awarding_toptier_agency.cgac_code,
            agency_id=federal_account.agency_identifier,
            main_account_code=federal_account.main_account_code,
            account_title="treasury_appropriation_account_title_%s" % string_award_id,
            awarding_toptier_agency_id=awarding_toptier_agency.toptier_agency_id,
            funding_toptier_agency_id=funding_toptier_agency.toptier_agency_id,
        )

        ref_program_activity = mommy.make(
            "references.RefProgramActivity",
            id=4000 + award_id,
            program_activity_code=str(4000 + award_id),
            program_activity_name="program_activity_%s" % (4000 + award_id),
        )

        object_class = mommy.make(
            "references.ObjectClass",
            id=5000 + award_id,
            object_class=5000 + award_id,
            object_class_name="object_class_%s" % (5000 + award_id),
        )

        mommy.make(
            "awards.FinancialAccountsByAwards",
            financial_accounts_by_awards_id=6000 + award_id,
            award_id=award_id,
            submission_id=submission_attributes.submission_id,
            treasury_account_id=treasury_appropriation_account.treasury_account_identifier,
            program_activity_id=ref_program_activity.id,
            object_class_id=object_class.id,
            transaction_obligated_amount=200000 + award_id,
        )

        mommy.make(
            "recipient.RecipientLookup",
            id=7000 + award_id,
            recipient_hash=RECIPIENT_HASH_PREFIX + str(7000 + award_id),
            legal_business_name="recipient_name_%s" % (7000 + award_id),
            duns="duns_%s" % (7000 + award_id),
        )

        mommy.make(
            "recipient.RecipientProfile",
            id=8000 + award_id,
            recipient_hash=RECIPIENT_HASH_PREFIX + str(7000 + award_id),
            recipient_level="R",
            recipient_name="recipient_name_%s" % (7000 + award_id),
            recipient_unique_id="duns_%s" % (7000 + award_id),
        )

    # We'll need some parent_awards.  We "hard code" values here rather than
    # generate them using the restock_parent_award script because we do a lot
    # of procedurally generated testing which would be difficult to do if
    # things were calculated from actual award values, if that makes any sense.
    # We're not testing the restock_parent_award script here, we're testing
    # what the endpoints return.
    for award_id in IDVS:
        string_award_id = str(award_id).zfill(3)
        mommy.make(
            "awards.ParentAward",
            award_id=award_id,
            generated_unique_award_id="GENERATED_UNIQUE_AWARD_ID_%s" % string_award_id,
            rollup_total_obligation=300000 + award_id,
            parent_award_id=PARENTS.get(award_id),
            rollup_contract_count=400000 + award_id,
        )
