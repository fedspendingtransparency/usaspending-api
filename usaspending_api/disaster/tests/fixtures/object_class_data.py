import pytest

from model_mommy import mommy

from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import SubmissionAttributes


@pytest.fixture
def basic_faba_with_object_class(award_count_sub_schedule, award_count_submission, defc_codes):
    basic_object_class = _major_object_class("001", ["0001"])

    mommy.make(
        "awards.FinancialAccountsByAwards",
        parent_award_id="basic award",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[0],
    )


@pytest.fixture
def basic_fa_by_object_class_with_object_class(award_count_sub_schedule, award_count_submission, defc_codes):
    basic_object_class = _major_object_class("001", ["0001"])

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[0],
        obligations_incurred_by_program_object_class_cpe=9,
        gross_outlay_amount_by_program_object_class_cpe=0,
    )


@pytest.fixture
def basic_fa_by_object_class_with_multpile_object_class(
    award_count_sub_schedule, award_count_quarterly_submission, defc_codes
):
    major_object_class_1 = _major_object_class("001", ["0001", "0002", "0003"])

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=major_object_class_1[0],
        obligations_incurred_by_program_object_class_cpe=10,
        gross_outlay_amount_by_program_object_class_cpe=2,
    )

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=major_object_class_1[1],
        obligations_incurred_by_program_object_class_cpe=0,
        gross_outlay_amount_by_program_object_class_cpe=20,
    )

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=major_object_class_1[2],
        obligations_incurred_by_program_object_class_cpe=1,
        gross_outlay_amount_by_program_object_class_cpe=0,
    )


@pytest.fixture
def basic_fa_by_object_class_with_object_class_but_no_obligations(
    award_count_sub_schedule, award_count_submission, defc_codes
):
    basic_object_class = _major_object_class("001", ["0001"])

    mommy.make(
        "financial_activities.FinancialAccountsByProgramActivityObjectClass",
        disaster_emergency_fund=DisasterEmergencyFundCode.objects.filter(code="M").first(),
        submission=SubmissionAttributes.objects.all().first(),
        object_class=basic_object_class[0],
        obligations_incurred_by_program_object_class_cpe=0,
        gross_outlay_amount_by_program_object_class_cpe=0,
    )


def _major_object_class(major_code, minor_codes):
    retval = []
    for minor_code in minor_codes:
        retval.append(
            mommy.make(
                "references.ObjectClass",
                major_object_class=major_code,
                major_object_class_name=f"{major_code} name",
                object_class=minor_code,
                object_class_name=f"{minor_code} name",
            )
        )
    return retval
