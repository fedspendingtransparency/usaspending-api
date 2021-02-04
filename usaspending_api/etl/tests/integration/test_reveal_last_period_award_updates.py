import pytest
import io

from datetime import datetime, timezone, timedelta
from django.core.management import call_command
from model_mommy import mommy

from usaspending_api.awards.models import Award


OLD_DATE = "2020-04-05"
SCRIPT_NAME = "reveal_last_period_award_updates"


@pytest.fixture
def award_data(db):
    defc = mommy.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19")

    award_id = 987

    awards = [
        mommy.make("awards.Award", id=award_id),
        *mommy.make("awards.Award", _quantity=9),
    ]

    for index, award in enumerate(awards):
        # Create faba records associated with the latest closed monthly and quarterly submissions
        if index % 2 == 0:
            mommy.make("awards.FinancialAccountsByAwards", submission_id=11, award=award, disaster_emergency_fund=defc)
        else:
            mommy.make("awards.FinancialAccountsByAwards", submission_id=21, award=award, disaster_emergency_fund=defc)
        Award.objects.filter(pk=award.id).update(update_date=OLD_DATE)  # convoluted line to sidestep auto_now()

    yield award_id


@pytest.fixture
def award_data_old_and_new(db):
    defc = mommy.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19")

    award_id_too_old = 988
    award_id_too_new = 989

    awards = [
        mommy.make("awards.Award", id=award_id_too_old),
        mommy.make("awards.Award", id=award_id_too_new),
        *mommy.make("awards.Award", _quantity=9),
    ]

    for index, award in enumerate(awards):
        if index % 2 == 0:
            mommy.make("awards.FinancialAccountsByAwards", submission_id=10, award=award, disaster_emergency_fund=defc)
        else:
            mommy.make("awards.FinancialAccountsByAwards", submission_id=12, award=award, disaster_emergency_fund=defc)
        Award.objects.filter(pk=award.id).update(update_date=OLD_DATE)  # convoluted line to sidestep auto_now()

    yield award_id_too_old, award_id_too_new


def test_awards_updated(submissions, award_data):

    today = datetime.now(timezone.utc)
    original_datetime = Award.objects.get(id=award_data)

    command_response = io.StringIO()
    call_command(SCRIPT_NAME, stdout=command_response)
    command_response = command_response.getvalue().strip()

    after = Award.objects.get(id=award_data)

    assert original_datetime.update_date.strftime("%Y-%m-%d") == OLD_DATE, "Sanity check failed"
    assert original_datetime.update_date != after.update_date, "Award was not properly updated"

    assert after.update_date - today < timedelta(minutes=1), "New datetime isn't today"
    assert command_response == "10"

    # Call a second time to make sure nothing is updated
    command_response = io.StringIO()
    call_command(SCRIPT_NAME, stdout=command_response)
    command_response = command_response.getvalue().strip()

    after2 = Award.objects.get(id=award_data)

    assert command_response == "0"
    assert after.update_date == after2.update_date


def test_no_awards_updated(submissions, award_data_old_and_new):
    """
    'too_old' is a faba record associated with a submission already revealed, but not the newest
    'too_new' is a faba record associated with a submission that has not yet been revealed
    """
    award_id_too_new, award_id_too_old = award_data_old_and_new

    original_datetime_new = Award.objects.get(id=award_id_too_new)
    original_datetime_old = Award.objects.get(id=award_id_too_old)

    command_response = io.StringIO()
    call_command(SCRIPT_NAME, stdout=command_response)
    command_response = command_response.getvalue().strip()

    after_new = Award.objects.get(id=award_id_too_new)
    after_old = Award.objects.get(id=award_id_too_old)

    assert original_datetime_new.update_date.strftime("%Y-%m-%d") == OLD_DATE, "Sanity check failed"
    assert original_datetime_new.update_date == after_new.update_date, "Award was updated when it shouldn't be"

    assert original_datetime_old.update_date.strftime("%Y-%m-%d") == OLD_DATE, "Sanity check failed"
    assert original_datetime_old.update_date == after_old.update_date, "Award was updated when it shouldn't be"

    assert command_response == "0"
