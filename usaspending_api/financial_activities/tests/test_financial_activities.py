from decimal import Decimal

import pytest

from model_mommy import mommy

from usaspending_api.financial_activities.models import (
    FinancialAccountsByProgramActivityObjectClass)


@pytest.mark.django_db
def test_quarterly_data():
    sub1 = mommy.make('submissions.SubmissionAttributes')
    sub2 = mommy.make('submissions.SubmissionAttributes', previous_submission=sub1)
    pa1 = mommy.make('references.RefProgramActivity')
    oc1 = mommy.make('references.ObjectClass')
    tas1 = mommy.make('accounts.TreasuryAppropriationAccount')

    mommy.make(
        'financial_activities.FinancialAccountsByProgramActivityObjectClass',
        submission=sub1,
        treasury_account=tas1,
        object_class=oc1,
        program_activity=pa1,
        gross_outlays_delivered_orders_paid_total_cpe=10.10,
        obligations_incurred_by_program_object_class_cpe=10,
        obligations_undelivered_orders_unpaid_total_fyb=99.99,
        ussgl490800_authority_outlayed_not_yet_disbursed_fyb=-30
    )
    mommy.make(
        'financial_activities.FinancialAccountsByProgramActivityObjectClass',
        submission=sub2,
        treasury_account=tas1,
        object_class=oc1,
        program_activity=pa1,
        gross_outlays_delivered_orders_paid_total_cpe=20.60,
        obligations_incurred_by_program_object_class_cpe=5,
        obligations_undelivered_orders_unpaid_total_fyb=99.99,
        ussgl490800_authority_outlayed_not_yet_disbursed_fyb=30
    )

    # retrieve all quarterly numbers and test results

    quarters = FinancialAccountsByProgramActivityObjectClass.get_quarterly_numbers()
    # number of quarterly adjusted records should = number of records
    # in FinancialAccountsByProgramActivityObjectClass
    assert len(list(quarters)) == 2

    for q in quarters:
        if q.submission == sub1:
            # qtrly values for year's first submission should remain unchanged
            assert q.gross_outlays_delivered_orders_paid_total_cpe == Decimal('10.10')
            assert q.obligations_incurred_by_program_object_class_cpe == Decimal('10.00')
            assert q.obligations_undelivered_orders_unpaid_total_fyb == Decimal('99.99')
            assert q.ussgl490800_authority_outlayed_not_yet_disbursed_fyb == Decimal('-30.00')
        else:
            # qtrly values for year's 2nd submission should be equal to 2nd
            # submission values - first submission values
            assert q.gross_outlays_delivered_orders_paid_total_cpe == Decimal('10.50')
            assert q.obligations_incurred_by_program_object_class_cpe == Decimal('-5.00')
            assert q.obligations_undelivered_orders_unpaid_total_fyb == Decimal('0.00')
            # TODO: check on correct logic where previous submission is negative
            assert q.ussgl490800_authority_outlayed_not_yet_disbursed_fyb == Decimal('60.00')

    # test getting quarterly results for a specific submission

    quarters = FinancialAccountsByProgramActivityObjectClass.get_quarterly_numbers(
        sub2.submission_id)
    # number of quarterly adjusted records should = number of records
    # in FinancialAccountsByProgramActivityObjectClass
    assert len(list(quarters)) == 1

    q = quarters[0]
    # qtrly values for year's 2nd submission should return the same
    # values as the test above
    assert q.gross_outlays_delivered_orders_paid_total_cpe == Decimal('10.50')
    assert q.obligations_incurred_by_program_object_class_cpe == Decimal('-5.00')
    assert q.obligations_undelivered_orders_unpaid_total_fyb == Decimal('0.00')
    assert q.ussgl490800_authority_outlayed_not_yet_disbursed_fyb == Decimal('60.00')

    # requesting data for non-existent submission returns zero records
    quarters = FinancialAccountsByProgramActivityObjectClass.get_quarterly_numbers(-888)
    assert len(list(quarters)) == 0
