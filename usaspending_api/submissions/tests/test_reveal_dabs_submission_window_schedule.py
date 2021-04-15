import pytest

from datetime import datetime, timezone
from model_mommy import mommy
from django.core.management import call_command
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule

SCHEDULE_FILE = "usaspending_api/data/testing_data/test_dabs_submission_window_schedule.csv"

FUTURE_DATE = datetime(9999, 12, 31, tzinfo=timezone.utc)
PAST_DATE = datetime(2000, 1, 1, tzinfo=timezone.utc)


@pytest.mark.django_db
def test_not_ready_to_reveal(client):
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=1,
        certification_due_date=FUTURE_DATE,
        submission_reveal_date=FUTURE_DATE,
        is_quarter=True,
    )

    call_command("reveal_dabs_submission_window_schedule")
    schedule = DABSSubmissionWindowSchedule.objects.get(id=1)

    # Submission Reveal Date should not be updated
    assert schedule.submission_reveal_date == FUTURE_DATE


@pytest.mark.django_db
def test_ready_to_reveal(client):
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=1,
        submission_reveal_date=FUTURE_DATE,
        certification_due_date=PAST_DATE,
    )

    call_command("reveal_dabs_submission_window_schedule")
    schedule = DABSSubmissionWindowSchedule.objects.get(id=1)

    assert schedule.submission_reveal_date < FUTURE_DATE


@pytest.mark.django_db
def test_already_revealed(client):
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=1,
        submission_reveal_date=PAST_DATE,
        certification_due_date=PAST_DATE,
    )

    call_command("reveal_dabs_submission_window_schedule")
    schedule = DABSSubmissionWindowSchedule.objects.get(id=1)

    assert schedule.submission_reveal_date == PAST_DATE


@pytest.mark.django_db
def test_quarter_vs_month(client):

    # Not ready to reveal month
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=10,
        submission_reveal_date=FUTURE_DATE,
        certification_due_date=PAST_DATE,
        submission_due_date=FUTURE_DATE,
        is_quarter=False,
    )

    # Ready to reveal month
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=11,
        submission_reveal_date=FUTURE_DATE,
        certification_due_date=FUTURE_DATE,
        submission_due_date=PAST_DATE,
        is_quarter=False,
    )

    # Not ready to reveal quarter
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=20,
        submission_reveal_date=FUTURE_DATE,
        certification_due_date=FUTURE_DATE,
        submission_due_date=PAST_DATE,
        is_quarter=True,
    )

    # Ready to reveal quarter
    mommy.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=21,
        submission_reveal_date=FUTURE_DATE,
        certification_due_date=PAST_DATE,
        submission_due_date=FUTURE_DATE,
        is_quarter=True,
    )

    call_command("reveal_dabs_submission_window_schedule")

    month_schedule_unrevealed = DABSSubmissionWindowSchedule.objects.get(id=10)
    month_schedule_revealed = DABSSubmissionWindowSchedule.objects.get(id=11)

    quarter_schedule_unrevealed = DABSSubmissionWindowSchedule.objects.get(id=20)
    quarter_schedule_revealed = DABSSubmissionWindowSchedule.objects.get(id=21)

    assert month_schedule_revealed.submission_reveal_date < FUTURE_DATE
    assert month_schedule_unrevealed.submission_reveal_date == FUTURE_DATE

    assert quarter_schedule_revealed.submission_reveal_date < FUTURE_DATE
    assert quarter_schedule_unrevealed.submission_reveal_date == FUTURE_DATE
