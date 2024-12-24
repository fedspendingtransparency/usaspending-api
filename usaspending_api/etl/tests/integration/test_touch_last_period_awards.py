from itertools import cycle

import pytest
import io

from datetime import datetime, timezone, timedelta
from django.core.management import call_command
from model_bakery import baker

from usaspending_api.awards.models import Award
from usaspending_api.search.models import AwardSearch

OLD_DATE = "2020-04-05"
SCRIPT_NAME = "touch_last_period_awards"


@pytest.fixture
def award_data(db):
    defc = baker.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19")

    award_id = 987

    awards = [
        baker.make("search.AwardSearch", award_id=award_id, action_date="2020-01-01"),
        *baker.make(
            "search.AwardSearch", award_id=cycle([1, 2, 3, 4, 5, 6, 7, 8, 9]), _quantity=9, action_date="2020-01-01"
        ),
    ]

    for index, award in enumerate(awards):
        # Create faba records associated with the latest closed monthly and quarterly submissions
        if index % 2 == 0:
            baker.make("awards.FinancialAccountsByAwards", submission_id=11, award=award, disaster_emergency_fund=defc)
        else:
            baker.make("awards.FinancialAccountsByAwards", submission_id=21, award=award, disaster_emergency_fund=defc)
        AwardSearch.objects.filter(award_id=award.award_id).update(
            update_date=OLD_DATE
        )  # convoluted line to sidestep auto_now()

    yield award_id


@pytest.fixture
def award_data_old_and_new(db):
    defc = baker.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19")

    award_id_too_old = 988
    award_id_too_new = 989

    awards = [
        baker.make("search.AwardSearch", award_id=award_id_too_old, action_date="2020-01-01"),
        baker.make("search.AwardSearch", award_id=award_id_too_new, action_date="2020-01-01"),
        *baker.make(
            "search.AwardSearch", award_id=cycle([1, 2, 3, 4, 5, 6, 7, 8, 9]), _quantity=9, action_date="2020-01-01"
        ),
    ]

    for index, award in enumerate(awards):
        if index % 2 == 0:
            baker.make("awards.FinancialAccountsByAwards", submission_id=10, award=award, disaster_emergency_fund=defc)
        else:
            baker.make("awards.FinancialAccountsByAwards", submission_id=12, award=award, disaster_emergency_fund=defc)
        AwardSearch.objects.filter(award_id=award.award_id).update(
            update_date=OLD_DATE
        )  # convoluted line to sidestep auto_now()

    yield award_id_too_old, award_id_too_new


@pytest.fixture
def load_date(db):
    edt = baker.make(
        "broker.ExternalDataType",
        name="touch_last_period_awards",
        external_data_type_id=120,
        update_date="2017-01-01",
    )
    baker.make("broker.ExternalDataLoadDate", external_data_type=edt, last_load_date=datetime(2000, 1, 31))


@pytest.mark.django_db
def test_awards_updated(load_date, submissions, award_data):

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


@pytest.mark.django_db
def test_no_awards_updated(load_date, submissions, award_data_old_and_new):
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
