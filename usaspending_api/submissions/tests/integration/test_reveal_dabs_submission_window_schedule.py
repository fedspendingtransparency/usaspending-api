import pytest

from datetime import datetime, timezone
from model_bakery import baker
from django.core.management import call_command
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule

SCHEDULE_FILE = "usaspending_api/data/testing_data/test_dabs_submission_window_schedule.csv"

FUTURE_DATE = datetime.max.replace(tzinfo=timezone.utc)
PAST_DATE = datetime(2000, 1, 1, tzinfo=timezone.utc)


@pytest.mark.django_db
def test_not_ready_to_reveal(client):
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=1,
        submission_due_date=FUTURE_DATE,
        submission_reveal_date=FUTURE_DATE,
        is_quarter=True,
    )

    call_command("reveal_dabs_submission_window_schedule")
    schedule = DABSSubmissionWindowSchedule.objects.get(id=1)

    # Submission Reveal Date should not be updated
    assert schedule.submission_reveal_date == FUTURE_DATE


@pytest.mark.django_db
def test_ready_to_reveal(client):
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=1,
        submission_reveal_date=FUTURE_DATE,
        submission_due_date=PAST_DATE,
    )

    call_command("reveal_dabs_submission_window_schedule")
    schedule = DABSSubmissionWindowSchedule.objects.get(id=1)

    assert schedule.submission_reveal_date < FUTURE_DATE


@pytest.mark.django_db
def test_already_revealed(client):
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=1,
        submission_reveal_date=PAST_DATE,
        submission_due_date=PAST_DATE,
    )

    call_command("reveal_dabs_submission_window_schedule")
    schedule = DABSSubmissionWindowSchedule.objects.get(id=1)

    assert schedule.submission_reveal_date == PAST_DATE
