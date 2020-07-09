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
        gross_outlays_delivered_orders_paid_total_cpe=8,
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
