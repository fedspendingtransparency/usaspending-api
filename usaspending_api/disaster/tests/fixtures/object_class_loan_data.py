import pytest

from model_mommy import mommy

from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.disaster.tests.fixtures.object_class_data import major_object_class_with_children


@pytest.fixture
def basic_object_class_faba_with_loan_value(award_count_sub_schedule, award_count_submission, defc_codes):
    award = _normal_award()
    award_loan = _loan_award()

    basic_object_class = major_object_class_with_children("001", [1])

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award,
        parent_award_id="basic award",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[0],
        gross_outlays_delivered_orders_paid_total_cpe=8,
        transaction_obligated_amount=1,
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award_loan,
        parent_award_id="basic loan",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[0],
        gross_outlays_delivered_orders_paid_total_cpe=9,
        transaction_obligated_amount=1,
    )


@pytest.fixture
def basic_object_class_multiple_faba_with_loan_value_with_single_object_class(
    award_count_sub_schedule, award_count_submission, defc_codes
):
    award1 = _loan_award()
    award2 = _loan_award()

    object_class1 = major_object_class_with_children("001", [1])

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award1,
        parent_award_id="basic award",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=object_class1[0],
        gross_outlays_delivered_orders_paid_total_cpe=8,
        transaction_obligated_amount=1,
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award2,
        parent_award_id="basic award 2",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=object_class1[0],
        gross_outlays_delivered_orders_paid_total_cpe=8,
        transaction_obligated_amount=1,
    )


@pytest.fixture
def basic_object_class_multiple_faba_with_loan_value_with_two_object_classes(
    award_count_sub_schedule, award_count_submission, defc_codes
):
    award1 = _loan_award()
    award2 = _loan_award()

    object_class1 = major_object_class_with_children("001", [1])
    object_class2 = major_object_class_with_children("002", [2])

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award1,
        parent_award_id="basic award",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=object_class1[0],
        gross_outlays_delivered_orders_paid_total_cpe=8,
        transaction_obligated_amount=1,
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        award=award2,
        parent_award_id="basic award",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=object_class2[0],
        gross_outlays_delivered_orders_paid_total_cpe=8,
        transaction_obligated_amount=1,
    )


def _normal_award():
    return mommy.make("awards.Award", type="A", total_loan_value=0)


def _loan_award():
    return mommy.make("awards.Award", type="07", total_loan_value=5)
