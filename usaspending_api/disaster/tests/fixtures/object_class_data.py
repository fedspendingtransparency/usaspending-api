import pytest

from model_mommy import mommy

from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.disaster.tests.fixtures.award_count_data import _normal_award


@pytest.fixture
def basic_faba_with_object_class(award_count_sub_schedule, award_count_submission, defc_codes):
    basic_object_class = major_object_class_with_children("001", [1])

    award = _normal_award()

    mommy.make(
        "awards.FinancialAccountsByAwards",
        parent_award_id="basic award",
        award=award,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[0],
        transaction_obligated_amount=1,
    )


@pytest.fixture
def basic_fa_by_object_class_with_object_class(award_count_sub_schedule, award_count_submission, defc_codes):
    basic_object_class = major_object_class_with_children("001", [1])

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[0],
        obligations_incurred_by_program_object_class_cpe=19,
        gross_outlay_amount_by_program_object_class_cpe=0,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-10,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=-10,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=10,
    )


@pytest.fixture
def basic_fa_by_object_class_with_multpile_object_class(
    award_count_sub_schedule, award_count_quarterly_submission, defc_codes
):
    major_object_class_1 = major_object_class_with_children("001", [1, 2, 3])

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=major_object_class_1[0],
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=4,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-90,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=-9,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=7,
    )

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=major_object_class_1[1],
        obligations_incurred_by_program_object_class_cpe=10,
        gross_outlay_amount_by_program_object_class_cpe=200,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-10,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=-50,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=-130,
    )

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=major_object_class_1[2],
        obligations_incurred_by_program_object_class_cpe=3,
        gross_outlay_amount_by_program_object_class_cpe=0,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-2,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=9,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=-9,
    )


@pytest.fixture
def basic_fa_by_object_class_with_multpile_object_class_of_same_code(
    award_count_sub_schedule, award_count_quarterly_submission, defc_codes
):
    class1 = mommy.make(
        "references.ObjectClass",
        id=9,
        major_object_class="major",
        major_object_class_name=f"major name",
        object_class=f"0001",
        object_class_name=f"0001 name",
    )

    class2 = mommy.make(
        "references.ObjectClass",
        id=10,
        major_object_class="major",
        major_object_class_name=f"major name",
        object_class=f"0001",
        object_class_name=f"0001 name",
    )

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=class1,
        obligations_incurred_by_program_object_class_cpe=30,
        gross_outlay_amount_by_program_object_class_cpe=992,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-20,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=-90,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=-900,
    )

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=class2,
        obligations_incurred_by_program_object_class_cpe=100,
        gross_outlay_amount_by_program_object_class_cpe=5,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-100,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=15,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=0,
    )


@pytest.fixture
def basic_fa_by_object_class_with_object_class_but_no_obligations(
    award_count_sub_schedule, award_count_submission, defc_codes
):
    basic_object_class = major_object_class_with_children("001", [1])

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[0],
        obligations_incurred_by_program_object_class_cpe=333,
        gross_outlay_amount_by_program_object_class_cpe=0,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-333,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=9,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=-9,
    )


@pytest.fixture
def faba_with_object_class_and_two_awards(award_count_sub_schedule, award_count_submission, defc_codes):
    basic_object_class = major_object_class_with_children("001", [1])

    award1 = _normal_award()
    award2 = _normal_award()

    mommy.make(
        "awards.FinancialAccountsByAwards",
        parent_award_id="basic award 1",
        award=award1,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[0],
        transaction_obligated_amount=1,
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        parent_award_id="basic award 2",
        award=award2,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[0],
        transaction_obligated_amount=1,
    )


@pytest.fixture
def faba_with_two_object_classes_and_two_awards(award_count_sub_schedule, award_count_submission, defc_codes):
    object_class1 = major_object_class_with_children("001", [1])
    object_class2 = major_object_class_with_children("002", [2])

    award1 = _normal_award()
    award2 = _normal_award()

    mommy.make(
        "awards.FinancialAccountsByAwards",
        parent_award_id="basic award",
        award=award1,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=object_class1[0],
        transaction_obligated_amount=1,
    )

    mommy.make(
        "awards.FinancialAccountsByAwards",
        parent_award_id="basic award",
        award=award2,
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=object_class2[0],
        transaction_obligated_amount=1,
    )


def major_object_class_with_children(major_code, minor_codes):
    retval = []
    for minor_code in minor_codes:
        retval.append(
            mommy.make(
                "references.ObjectClass",
                id=minor_code,
                major_object_class=major_code,
                major_object_class_name=f"{major_code} name",
                object_class=f"000{minor_code}",
                object_class_name=f"000{minor_code} name",
            )
        )
    return retval
