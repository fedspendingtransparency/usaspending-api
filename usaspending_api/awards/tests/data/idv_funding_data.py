from model_mommy import mommy

AWARD_COUNT = 15
IDVS = (1, 2, 3, 4, 5, 7, 8)
PARENTS = {3: 1, 4: 1, 5: 1, 6: 1, 7: 2, 8: 2, 9: 2, 10: 2, 11: 7, 12: 7, 13: 8, 14: 8, 15: 9}


def create_funding_data_tree():

    """
    You'll have to use your imagination a bit with my budget tree drawings.
    These are the two hierarchies being built by this function.  "I" means
    IDV.  "C" means contract.  The number is the award id.  So in this
    drawing, I1 is the parent of I3, I4, I5, and C6.  I2 is the grandparent
    of C11, C12, C13, C14, and C15.  Please note that the C9 -> C15
    relationship is actually invalid in the IDV world.  I've added it for
    testing purposes, however.  Anyhow, hope this helps.  There's a reason
    I'm in software and not showing my wares at an art gallery somewhere.

              I1                                        I2
      I3   I4   I5   C6                  I7        I8        C9        C10
                                      C11 C12   C13 C14      C15
    """

    mommy.make(
            'references.Agency',
            id=12000
    )
    for _id in range(1, AWARD_COUNT + 1):
        _pid = PARENTS.get(_id)

        # To ensure our text sorts happen in the correct order, pad numbers with zeros.
        _spid = str(_pid).zfill(3) if _pid else None
        _sid = str(_id).zfill(3)

        mommy.make(
            'references.Agency',
            id=9000 + _id,
            toptier_flag=True,
            toptier_agency_id=9500 + _id
        )

        mommy.make(
            'references.ToptierAgency',
            toptier_agency_id=9500 + _id,
            cgac_code=str(_id).zfill(3),
            name='toptier_funding_agency_name_%s' % (9500 + _id),
        )

        mommy.make(
            'references.Agency',
            id=8000 + _id,
            toptier_flag=True,
            toptier_agency_id=8500 + _id
        )

        mommy.make(
            'references.ToptierAgency',
            toptier_agency_id=8500 + _id,
            name='toptier_awarding_agency_name_%s' % (8500 + _id),
        )

        # Kirk and I have no idea why agency_id has to be done this way instead of passing
        # it inline like all the other if/else's below, but it definitely breaks
        # if you don't... bug in model mommy maybe? Inquiring minds want to know...
        mommy.make(
            'awards.Award',
            id=_id,
            generated_unique_award_id='GENERATED_UNIQUE_AWARD_ID_%s' % _sid,
            type=('IDV_%s' if _id in IDVS else 'CONTRACT_%s') % _sid,
            piid='piid_%s' % _sid,
            fpds_agency_id='fpds_agency_id_%s' % _sid,
            parent_award_piid='piid_%s' % _spid if _spid else None,
            fpds_parent_agency_id='fpds_agency_id_%s' % _spid if _spid else None,
            awarding_agency_id=8000 + _id,
            funding_agency_id=9000 + _id,
        )

        if _id in IDVS:
            mommy.make(
                'awards.ParentAward',
                award_id=_id,
                generated_unique_award_id='GENERATED_UNIQUE_AWARD_ID_%s' % _sid,
                parent_award_id=_pid
            )

        mommy.make(
            'submissions.SubmissionAttributes',
            submission_id=_id,
            reporting_fiscal_year=2000 + _id,
            reporting_fiscal_quarter=_id % 4 + 1
        )

        mommy.make(
            'accounts.FederalAccount',
            id=_id,
            account_title='FederalAccount account title %s' % _sid,
            federal_account_code=str(_id).zfill(3)+"-"+str(_id).zfill(4),
        )
        mommy.make(
            'accounts.TreasuryAppropriationAccount',
            treasury_account_identifier=_id,
            federal_account_id=_id,
            reporting_agency_id=str(_id).zfill(3),
            reporting_agency_name='reporting agency name %s' % _sid,
            agency_id=str(_id).zfill(3),
            main_account_code=str(_id).zfill(4),
            account_title='TreasuryAppropriationAccount account title %s' % _sid,
            awarding_toptier_agency_id=8500 + _id,
            funding_toptier_agency_id=9500 + _id,
        )

        mommy.make(
            'references.RefProgramActivity',
            id=_id,
            program_activity_code=_sid,
            program_activity_name='program activity %s' % _sid
        )

        mommy.make(
            'references.ObjectClass',
            id=_id,
            object_class='1' + _sid,
            object_class_name='object class %s' % _sid
        )

        mommy.make(
            'awards.FinancialAccountsByAwards',
            financial_accounts_by_awards_id=_id,
            award_id=_id,
            submission_id=_id,
            treasury_account_id=_id,
            program_activity_id=_id,
            object_class_id=_id,
            transaction_obligated_amount=_id * 10000 + _id + _id / 100
        )
