import pytest

from datetime import datetime, timezone
from model_bakery import baker
from django.core.management import call_command
from usaspending_api.submissions.models import DABSSubmissionWindowSchedule

SCHEDULE_FILE = "usaspending_api/data/testing_data/test_dabs_submission_window_schedule.csv"

FUTURE_DATE = datetime.max.replace(tzinfo=timezone.utc)
CURRENT_REVEAL_DATE = datetime(2000, 1, 1, tzinfo=timezone.utc)


@pytest.mark.django_db
def test_schedule_is_updated(client):
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2020121,
        submission_reveal_date="2020-12-23",
        submission_due_date="2020-12-23",
        submission_fiscal_year=99,
    )

    call_command("load_dabs_submission_window_schedule", file=SCHEDULE_FILE)
    schedule = DABSSubmissionWindowSchedule.objects.get(id=2020121)

    assert schedule.submission_fiscal_year == 2020

    # Submission Reveal Date should not be updated
    assert schedule.submission_reveal_date == datetime(2020, 12, 23, tzinfo=timezone.utc)


@pytest.mark.django_db
def test_future_schedule_is_updated(client):
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2777121,
        submission_reveal_date="2076-12-23",
        submission_due_date="2076-12-23",
        submission_fiscal_year=2076,
    )

    call_command("load_dabs_submission_window_schedule", file=SCHEDULE_FILE)
    schedule = DABSSubmissionWindowSchedule.objects.get(id=2777121)

    assert schedule.submission_fiscal_year == 2777

    # Submission Reveal Date is updated
    assert schedule.submission_reveal_date == FUTURE_DATE


@pytest.mark.django_db
def test_non_matching_schedule_is_removed(client):
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=2010121,
        submission_reveal_date="2010-12-23",
        submission_due_date="2010-12-23",
        submission_fiscal_year=2010,
    )

    call_command("load_dabs_submission_window_schedule", file=SCHEDULE_FILE)
    schedule_count = DABSSubmissionWindowSchedule.objects.all().filter(id=2010121).count()
    assert schedule_count == 0


@pytest.mark.django_db
def test_schedule_is_created(client):
    call_command("load_dabs_submission_window_schedule", file=SCHEDULE_FILE)
    schedule = DABSSubmissionWindowSchedule.objects.get(id=2888121)

    # Record exists and Submission Reveal Date is set to future date
    assert schedule.submission_reveal_date == FUTURE_DATE


@pytest.mark.django_db
def test_load_from_broker(client, monkeypatch):
    """
    Tests that both a new record is added and an old record is updated when loading from Broker.
    Important to note that the "load_dabs_submission_window_schedule" does not control the "reveal"
    of the Submission Window; that is controlled by "reveal_dabs_submission_window_schedules".
    A default of datetime.max is used to ensure new Submission Windows are not revealed until intended.
    """
    baker.make(
        DABSSubmissionWindowSchedule,
        id=2020121,
        period_start_date="2020-07-01",
        period_end_date="2020-09-30",
        submission_start_date="2020-10-19",
        submission_due_date="2020-11-17",
        certification_due_date="2020-11-17",
        submission_reveal_date="2021-08-15",
        submission_fiscal_year="2020",
        submission_fiscal_quarter=4,
        submission_fiscal_month=12,
        is_quarter=True,
    )

    def patched_generate_schedules_from_broker():
        return [
            DABSSubmissionWindowSchedule(
                id=2020090,
                period_start_date=datetime(2020, 6, 1, 0, 0, 0),
                period_end_date=datetime(2020, 6, 30, 0, 0, 0),
                submission_start_date=datetime(2020, 7, 17, 0, 0, 0),
                submission_due_date=datetime(2020, 7, 31, 0, 0, 0),
                certification_due_date=datetime(2020, 8, 15, 0, 0, 0),
                submission_reveal_date=None,
                submission_fiscal_year=2020,
                submission_fiscal_quarter=3,
                submission_fiscal_month=9,
                is_quarter=False,
            ),
            DABSSubmissionWindowSchedule(
                id=2020121,
                period_start_date=datetime(2020, 7, 1, 0, 0, 0),
                period_end_date=datetime(2020, 9, 30, 0, 0, 0),
                submission_start_date=datetime(2020, 10, 19, 0, 0, 0),
                submission_due_date=datetime(2020, 11, 17, 0, 0, 0),
                certification_due_date=datetime(2020, 11, 30, 0, 0, 0),
                submission_reveal_date=datetime(2021, 8, 15, 0, 0, 0),
                submission_fiscal_year=2020,
                submission_fiscal_quarter=4,
                submission_fiscal_month=12,
                is_quarter=True,
            ),
        ]

    monkeypatch.setattr(
        "usaspending_api.submissions.management.commands.load_dabs_submission_window_schedule.Command.generate_schedules_from_broker",
        lambda _: patched_generate_schedules_from_broker(),
    )

    call_command("load_dabs_submission_window_schedule")

    new_dabs_schedule = DABSSubmissionWindowSchedule.objects.filter(id=2020090).first()
    updated_dabs_schedule = DABSSubmissionWindowSchedule.objects.filter(id=2020121).first()

    # Check that the default values for "submission_reveal_date" was used appropriately
    assert new_dabs_schedule.submission_reveal_date == FUTURE_DATE

    # Check that the "certification_due_date" was updated for existing record
    assert updated_dabs_schedule.certification_due_date == datetime(2020, 11, 30, 0, 0, 0, tzinfo=timezone.utc)
