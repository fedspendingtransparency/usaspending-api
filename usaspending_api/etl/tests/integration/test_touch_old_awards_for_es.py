import pytest
import random

from datetime import datetime, timezone, timedelta
from django.core.management import call_command
from model_mommy import mommy

from usaspending_api.awards.models.award import Award


@pytest.fixture
def submissions(db):
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2020051,
        submission_reveal_date="2020-05-01",
        submission_fiscal_year=2020,
        submission_fiscal_month=5,
        is_quarter=True,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2020071,
        submission_fiscal_year=2020,
        submission_fiscal_month=7,
        submission_reveal_date="2020-07-01",
        is_quarter=True,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2030011,
        submission_fiscal_year=2030,
        submission_fiscal_month=1,
        submission_reveal_date="2030-01-01",
        is_quarter=True,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2020050,
        submission_fiscal_year=2020,
        submission_fiscal_month=5,
        submission_reveal_date="2010-05-01",
        is_quarter=False,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2020060,
        submission_fiscal_year=2020,
        submission_fiscal_month=6,
        submission_reveal_date="2020-06-01",
        is_quarter=False,
    )
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2030,
        submission_fiscal_month=1,
        id=2030010,
        submission_reveal_date="2030-01-01",
        is_quarter=False,
    )

    mommy.make(
        "submissions.SubmissionAttributes",
        submission_id=1,
        reporting_period_start="2020-04-01",
        is_final_balances_for_fy=False,
        submission_window_id=2020050,
    )


@pytest.fixture
def award_data1(db):
    award_id = 111  # used below
    defc = mommy.make("references.DisasterEmergencyFundCode", code="L", group_name="covid_19")

    aw1 = mommy.make("awards.Award", id=award_id)
    mommy.make("awards.FinancialAccountsByAwards", submission_id=1, award=aw1, disaster_emergency_fund=defc)

    yield award_id


@pytest.fixture
def award_data2(db):
    defc = mommy.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19")

    award_id = 987
    old_date = "2020-04-05"
    yesterday = datetime.now() - timedelta(days=1)
    piids = ["abc", "def", "ghi", "jki"]

    awards = [
        mommy.make("awards.Award", piid=random.choice(piids), id=award_id),
        *mommy.make("awards.Award", piid=random.choice(piids), _quantity=9),
    ]

    for index, award in enumerate(awards):
        mommy.make("awards.FinancialAccountsByAwards", submission_id=1, award=award, disaster_emergency_fund=defc)
        if index % 2 == 0:
            Award.objects.filter(pk=award.id).update(update_date=old_date)  # convoluted line to sidestep auto_now()
        else:
            Award.objects.filter(pk=award.id).update(update_date=yesterday)  # convoluted line to sidestep auto_now()

    yield award_id, yesterday


def test_happy_path(submissions, award_data1):
    """Simple test case: update single award record"""
    today = datetime.now(timezone.utc)
    past_date = "2007-05-11"

    Award.objects.filter(pk=award_data1).update(update_date=past_date)
    original_datetime = Award.objects.get(id=award_data1)

    call_command("update_missing_covid_awards")

    after = Award.objects.get(id=award_data1)

    assert original_datetime.update_date.strftime("%Y-%m-%d") == past_date, "Sanity check failed"
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


def test_multiple_awards_monthly(submissions, award_data2):
    """Simple test case: update 5 award records, excluding 5 based on update_date"""
    award_id, yesterday = award_data2
    today = datetime.now(timezone.utc)



    call_command("update_missing_covid_awards")
