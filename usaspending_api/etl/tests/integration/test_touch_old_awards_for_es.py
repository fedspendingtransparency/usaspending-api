import pytest
import random

from datetime import datetime, timezone, timedelta
from django.core.management import call_command
from django.db.models import Max
from model_mommy import mommy

from usaspending_api.awards.models import Award, FinancialAccountsByAwards

OLD_DATE = "2020-04-05"


@pytest.fixture
def submissions(db):
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2020061,
        submission_reveal_date="2020-04-01",
        submission_fiscal_year=2020,
        submission_fiscal_month=6,
        is_quarter=True,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2020091,
        submission_fiscal_year=2020,
        submission_fiscal_month=9,
        submission_reveal_date="2020-07-01",
        is_quarter=True,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2030031,
        submission_fiscal_year=2030,
        submission_fiscal_month=3,
        submission_reveal_date="2030-01-01",
        is_quarter=True,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2020080,
        submission_fiscal_year=2020,
        submission_fiscal_month=8,
        submission_reveal_date="2020-06-01",
        is_quarter=False,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2020090,
        submission_fiscal_year=2020,
        submission_fiscal_month=9,
        submission_reveal_date="2020-07-01",
        is_quarter=False,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2020100,
        submission_fiscal_year=2020,
        submission_fiscal_month=10,
        submission_reveal_date="2020-08-01",
        is_quarter=False,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2030,
        submission_fiscal_month=3,
        id=2030030,
        submission_reveal_date="2030-01-01",
        is_quarter=False,
    )

    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=20,
        reporting_period_start="2020-08-01",
        is_final_balances_for_fy=False,
        submission_window_id=2020061,
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=21,
        reporting_period_start="2020-08-01",
        is_final_balances_for_fy=True,
        submission_window_id=2020091,
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=9,
        reporting_period_start="2020-06-01",
        is_final_balances_for_fy=False,
        submission_window_id=2020080,
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=10,
        reporting_period_start="2020-07-01",
        is_final_balances_for_fy=False,
        submission_window_id=2020090,
    )
    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=11,
        reporting_period_start="2020-08-01",
        is_final_balances_for_fy=True,
        submission_window_id=2020100,
    )


@pytest.fixture
def award_data1(db):
    award_id = 111  # used below
    defc = mommy.make("references.DisasterEmergencyFundCode", code="L", group_name="covid_19")

    aw1 = mommy.make("awards.Award", id=award_id)
    mommy.make("awards.FinancialAccountsByAwards", submission_id=10, award=aw1, disaster_emergency_fund=defc)

    yield award_id


@pytest.fixture
def award_data2(db):
    defc = mommy.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19")

    award_id = 987
    yesterday = datetime.now() - timedelta(days=1)
    piids = ["abc", "def", "ghi", "jki"]

    awards = [
        mommy.make("awards.Award", piid=random.choice(piids), id=award_id),
        *mommy.make("awards.Award", piid=random.choice(piids), _quantity=9),
    ]

    for index, award in enumerate(awards):
        mommy.make("awards.FinancialAccountsByAwards", submission_id=10, award=award, disaster_emergency_fund=defc)
        if index % 2 == 0:
            Award.objects.filter(pk=award.id).update(update_date=OLD_DATE)  # convoluted line to sidestep auto_now()
        else:
            Award.objects.filter(pk=award.id).update(update_date=yesterday)  # convoluted line to sidestep auto_now()

    yield award_id, yesterday


@pytest.fixture
def award_data3(db):
    defc = mommy.make("references.DisasterEmergencyFundCode", code="P", group_name="covid_19")

    aw1 = mommy.make("awards.Award", fain="abc123")
    aw2 = mommy.make("awards.Award", fain="zyx987")

    Award.objects.filter(pk=aw1.id).update(update_date=OLD_DATE)  # convoluted line to sidestep auto_now()
    Award.objects.filter(pk=aw2.id).update(update_date=OLD_DATE)  # convoluted line to sidestep auto_now()

    mommy.make("awards.FinancialAccountsByAwards", submission_id=10, award=aw1, disaster_emergency_fund=defc)
    mommy.make("awards.FinancialAccountsByAwards", submission_id=10, award=aw2, disaster_emergency_fund=defc)
    mommy.make("awards.FinancialAccountsByAwards", submission_id=11, award=aw1, disaster_emergency_fund=defc)

    yield aw1, aw2


@pytest.fixture
def award_data4(db):
    defc = mommy.make("references.DisasterEmergencyFundCode", code="P", group_name="covid_19")

    aw1 = mommy.make("awards.Award", fain="abc123")
    aw2 = mommy.make("awards.Award", fain="zyx987")

    Award.objects.filter(pk=aw1.id).update(update_date=OLD_DATE)  # convoluted line to sidestep auto_now()
    Award.objects.filter(pk=aw2.id).update(update_date=OLD_DATE)  # convoluted line to sidestep auto_now()

    mommy.make("awards.FinancialAccountsByAwards", submission_id=9, award=aw1, disaster_emergency_fund=defc)
    mommy.make("awards.FinancialAccountsByAwards", submission_id=9, award=aw2, disaster_emergency_fund=defc)
    mommy.make("awards.FinancialAccountsByAwards", submission_id=11, award=aw1, disaster_emergency_fund=defc)

    yield aw1, aw2


@pytest.fixture
def award_mixed_periods(db):
    defc = mommy.make("references.DisasterEmergencyFundCode", code="O", group_name="covid_19")

    aw1 = mommy.make("awards.Award", piid="abc123")
    aw2 = mommy.make("awards.Award", piid="zyx987")
    aw3 = mommy.make("awards.Award", piid="ABC321")
    aw4 = mommy.make("awards.Award", piid="ZYX789")

    Award.objects.filter(pk=aw1.id).update(update_date=OLD_DATE)  # convoluted line to sidestep auto_now()
    Award.objects.filter(pk=aw2.id).update(update_date=OLD_DATE)  # convoluted line to sidestep auto_now()
    Award.objects.filter(pk=aw3.id).update(update_date=OLD_DATE)  # convoluted line to sidestep auto_now()
    Award.objects.filter(pk=aw4.id).update(update_date=OLD_DATE)  # convoluted line to sidestep auto_now()

    mommy.make("awards.FinancialAccountsByAwards", submission_id=10, award=aw1, disaster_emergency_fund=defc)
    mommy.make("awards.FinancialAccountsByAwards", submission_id=10, award=aw2, disaster_emergency_fund=defc)
    mommy.make("awards.FinancialAccountsByAwards", submission_id=11, award=aw1, disaster_emergency_fund=defc)

    mommy.make("awards.FinancialAccountsByAwards", submission_id=20, award=aw3, disaster_emergency_fund=defc)
    mommy.make("awards.FinancialAccountsByAwards", submission_id=20, award=aw4, disaster_emergency_fund=defc)
    mommy.make("awards.FinancialAccountsByAwards", submission_id=21, award=aw3, disaster_emergency_fund=defc)

    yield aw1, aw2, aw3, aw4


def test_happy_path(submissions, award_data1):
    """Simple test case: update single award record"""
    today = datetime.now(timezone.utc)

    Award.objects.filter(pk=award_data1).update(update_date=OLD_DATE)
    original_datetime = Award.objects.get(id=award_data1)

    call_command("update_missing_covid_awards")

    after = Award.objects.get(id=award_data1)

    assert original_datetime.update_date.strftime("%Y-%m-%d") == OLD_DATE, "Sanity check failed"
    assert original_datetime.update_date != after.update_date, "Award was not properly updated"
    assert after.update_date - today < timedelta(minutes=1), "New datetime isn't today"


def test_multiple_awards_update_date(submissions, award_data2):
    """Simple test case: update 5 award records, excluding 5 based on update_date"""
    award_id, yesterday = award_data2
    today = datetime.now(timezone.utc)

    assert Award.objects.count() == 10, "Missing Award objects"
    assert Award.objects.filter(update_date__lt="2020-05-01").count() == 5, "Incorrect number of stale awards"
    assert Award.objects.filter(update_date__gt=yesterday).count() == 0, "Incorrect number of recent awards"

    call_command("update_missing_covid_awards")

    assert Award.objects.filter(update_date__gt=yesterday).count() == 5, "Incorrect number of awards updated"
    assert Award.objects.get(id=award_id).update_date - today < timedelta(minutes=1), "Expected award wasn't updated"


def test_multiple_awards_monthly(submissions, award_data3):
    """Update 1 "missing" award, leave the other untouched"""

    current_award, old_award = award_data3

    assert FinancialAccountsByAwards.objects.filter(award__fain=old_award.fain).count() == 1
    assert FinancialAccountsByAwards.objects.filter(award__fain=current_award.fain).count() == 2

    old_award_before = Award.objects.filter(fain=old_award.fain).aggregate(Max("update_date"))
    new_award_before = Award.objects.filter(fain=current_award.fain).aggregate(Max("update_date"))

    assert old_award_before["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "mock data doesn't meet expectations"
    assert new_award_before["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "mock data doesn't meet expectations"

    call_command("update_missing_covid_awards")

    old_award_after = Award.objects.filter(fain=old_award.fain).aggregate(Max("update_date"))
    new_award_after = Award.objects.filter(fain=current_award.fain).aggregate(Max("update_date"))

    assert old_award_after["update_date__max"].strftime("%Y-%m-%d") > OLD_DATE, "Missing award was not updated"
    assert new_award_after["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "Present award was incorrectly updated"


def test_multiple_awards_monthly_requiring_all(submissions, award_data4):
    """Update 1 older award, leave the other untouched"""

    current_award, old_award = award_data4

    assert FinancialAccountsByAwards.objects.filter(award__fain=old_award.fain).count() == 1
    assert FinancialAccountsByAwards.objects.filter(award__fain=current_award.fain).count() == 2

    old_award_before = Award.objects.filter(fain=old_award.fain).aggregate(Max("update_date"))
    new_award_before = Award.objects.filter(fain=current_award.fain).aggregate(Max("update_date"))

    assert old_award_before["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "mock data doesn't meet expectations"
    assert new_award_before["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "mock data doesn't meet expectations"

    call_command("update_missing_covid_awards")

    old_award_after = Award.objects.filter(fain=old_award.fain).aggregate(Max("update_date"))
    new_award_after = Award.objects.filter(fain=current_award.fain).aggregate(Max("update_date"))

    assert old_award_after["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "Award was incorrectly updated"
    assert new_award_after["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "Award was incorrectly updated"

    call_command("update_missing_covid_awards", "--all")

    old_award_after = Award.objects.filter(fain=old_award.fain).aggregate(Max("update_date"))
    new_award_after = Award.objects.filter(fain=current_award.fain).aggregate(Max("update_date"))

    assert old_award_after["update_date__max"].strftime("%Y-%m-%d") > OLD_DATE, "Missing award was not updated"
    assert new_award_after["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "Present award was incorrectly updated"


def test_multiple_awards_different_submissions(submissions, award_mixed_periods):
    """Update 2 older awards from different submissions, leave the others untouched"""

    current_monthly_award, old_monthly_award, current_quarterly_award, old_quarterly_award = award_mixed_periods

    assert FinancialAccountsByAwards.objects.filter(award__piid=old_monthly_award.piid).count() == 1
    assert FinancialAccountsByAwards.objects.filter(award__piid=current_monthly_award.piid).count() == 2

    old_monthly_award_before = Award.objects.filter(piid=old_monthly_award.piid).aggregate(Max("update_date"))
    new_monthly_award_before = Award.objects.filter(piid=current_monthly_award.piid).aggregate(Max("update_date"))
    old_quarterly_award_before = Award.objects.filter(piid=old_quarterly_award.piid).aggregate(Max("update_date"))
    new_quarterly_award_before = Award.objects.filter(piid=current_quarterly_award.piid).aggregate(Max("update_date"))

    assert old_monthly_award_before["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE
    assert new_monthly_award_before["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE
    assert old_quarterly_award_before["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE
    assert new_quarterly_award_before["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE

    call_command("update_missing_covid_awards")

    old_monthly_award_after = Award.objects.filter(piid=old_monthly_award.piid).aggregate(Max("update_date"))
    new_monthly_award_after = Award.objects.filter(piid=current_monthly_award.piid).aggregate(Max("update_date"))
    old_quarterly_award_after = Award.objects.filter(piid=old_quarterly_award.piid).aggregate(Max("update_date"))
    new_quarterly_award_after = Award.objects.filter(piid=current_quarterly_award.piid).aggregate(Max("update_date"))

    assert old_monthly_award_after["update_date__max"].strftime("%Y-%m-%d") > OLD_DATE
    assert new_monthly_award_after["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE
    assert old_quarterly_award_after["update_date__max"].strftime("%Y-%m-%d") > OLD_DATE
    assert new_quarterly_award_after["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE
