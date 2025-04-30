from decimal import Decimal

import pytest
from model_bakery import baker

from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.fixture
def basic_faba_with_object_class():
    baker.make("references.DisasterEmergencyFundCode", code="A", public_law="Public law for A")
    baker.make("references.DisasterEmergencyFundCode", code="M", public_law="Public law for M")
    baker.make("references.DisasterEmergencyFundCode", code="N", public_law="Public law for N")
    baker.make("references.DisasterEmergencyFundCode", code="O", public_law="Public law for O")

    baker.make(
        "disaster.CovidFABASpending",
        spending_level="object_class",
        funding_major_object_class_id="001",
        funding_major_object_class_code="001",
        funding_major_object_class_name="001 name",
        funding_object_class_id=1,
        funding_object_class_code="0001",
        funding_object_class_name="0001 name",
        defc="M",
        award_type="A",
        award_count=1,
        obligation_sum=Decimal(1.0),
        outlay_sum=Decimal(0.0),
    )
    baker.make(
        "disaster.CovidFABASpending",
        spending_level="object_class",
        funding_major_object_class_id="002",
        funding_major_object_class_code="002",
        funding_major_object_class_name="002 name",
        funding_object_class_id=2,
        funding_object_class_code="0002",
        funding_object_class_name="0002 name",
        defc="N",
        award_type="B",
        award_count=2,
        obligation_sum=Decimal(1.0),
        outlay_sum=Decimal(0.0),
    )
    baker.make(
        "disaster.CovidFABASpending",
        spending_level="object_class",
        funding_major_object_class_id="003",
        funding_major_object_class_code="003",
        funding_major_object_class_name="003 name",
        funding_object_class_id=3,
        funding_object_class_code="00031",
        funding_object_class_name="00031 name",
        defc="O",
        award_type="B",
        award_count=2,
        obligation_sum=Decimal(1.0),
        outlay_sum=Decimal(10.0),
    )
    baker.make(
        "disaster.CovidFABASpending",
        spending_level="object_class",
        funding_major_object_class_id="003",
        funding_major_object_class_code="003",
        funding_major_object_class_name="003 name",
        funding_object_class_id=4,
        funding_object_class_code="00032",
        funding_object_class_name="00032 name",
        defc="O",
        award_type="C",
        award_count=3,
        obligation_sum=Decimal(1.0),
        outlay_sum=Decimal(20.0),
    )
    baker.make(
        "disaster.CovidFABASpending",
        spending_level="object_class",
        funding_major_object_class_id="003",
        funding_major_object_class_code="003",
        funding_major_object_class_name="003 name",
        funding_object_class_id=5,
        funding_object_class_code="00033",
        funding_object_class_name="00033 name",
        defc="O",
        award_type="D",
        award_count=2,
        obligation_sum=Decimal(1.0),
        outlay_sum=Decimal(30.0),
    )


@pytest.fixture
def basic_fa_by_object_class_with_object_class(award_count_sub_schedule, award_count_submission, defc_codes):
    basic_object_class = major_object_class_with_children("001", [1, 2, 3])

    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[0],
        obligations_incurred_by_program_object_class_cpe=1000,
        gross_outlay_amount_by_program_object_class_cpe=200,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-10,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=-10,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=5,
        prior_year_adjustment="B",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="N").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[1],
        obligations_incurred_by_program_object_class_cpe=900,
        gross_outlay_amount_by_program_object_class_cpe=300,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-10,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=-10,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=5,
        prior_year_adjustment="P",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="O").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[2],
        obligations_incurred_by_program_object_class_cpe=800,
        gross_outlay_amount_by_program_object_class_cpe=400,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-10,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=-10,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=5,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )
    baker.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="O").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[2],
        obligations_incurred_by_program_object_class_cpe=400,
        gross_outlay_amount_by_program_object_class_cpe=800,
        deobligations_recoveries_refund_pri_program_object_class_cpe=-10,
        ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe=-10,
        ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe=5,
        prior_year_adjustment="X",
        ussgl480100_undelivered_orders_obligations_unpaid_cpe=15,
        ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe=20,
        ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe=15,
        ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe=5,
        ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe=6,
        ussgl490100_delivered_orders_obligations_unpaid_cpe=7,
        ussgl490200_delivered_orders_obligations_paid_cpe=8,
        ussgl490800_authority_outlayed_not_yet_disbursed_cpe=100,
        ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe=13,
        ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe=9,
        ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe=14,
        ussgl480110_rein_undel_ord_cpe=75,
        ussgl490110_rein_deliv_ord_cpe=63,
    )


@pytest.fixture
def basic_fa_by_object_class_with_multpile_object_class(
    award_count_sub_schedule, award_count_quarterly_submission, defc_codes
):
    major_object_class_1 = major_object_class_with_children("001", [1, 2, 3])

    baker.make(
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

    baker.make(
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

    baker.make(
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
    class1 = baker.make(
        "references.ObjectClass",
        id=9,
        major_object_class="major",
        major_object_class_name="major name",
        object_class="0001",
        object_class_name="0001 name",
    )

    class2 = baker.make(
        "references.ObjectClass",
        id=10,
        major_object_class="major",
        major_object_class_name="major name",
        object_class="0001",
        object_class_name="0001 name",
    )

    baker.make(
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

    baker.make(
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

    baker.make(
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


def major_object_class_with_children(major_code, minor_codes):
    retval = []
    for minor_code in minor_codes:
        retval.append(
            baker.make(
                "references.ObjectClass",
                id=minor_code,
                major_object_class=major_code,
                major_object_class_name=f"{major_code} name",
                object_class=f"000{minor_code}",
                object_class_name=f"000{minor_code} name",
            )
        )
    return retval
