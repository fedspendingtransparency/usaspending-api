from itertools import cycle

import pytest
import random

from datetime import datetime, timezone, timedelta
from django.core.management import call_command
from django.db.models import Max
from model_bakery import baker

from usaspending_api.awards.models import Award, FinancialAccountsByAwards
from usaspending_api.search.models import AwardSearch

OLD_DATE = "2020-04-05"
SCRIPT_NAME = "update_missing_covid_awards"


@pytest.fixture
def award_data1(db):
    award_id = 111  # used below
    defc = baker.make("references.DisasterEmergencyFundCode", code="L", group_name="covid_19")

    aw1 = baker.make("search.AwardSearch", award_id=award_id, action_date="2020-01-01")
    baker.make("awards.FinancialAccountsByAwards", submission_id=10, award=aw1, disaster_emergency_fund=defc)

    yield award_id


@pytest.fixture
def award_data2(db):
    defc = baker.make("references.DisasterEmergencyFundCode", code="M", group_name="covid_19")

    award_id = 987
    yesterday = datetime.now() - timedelta(days=1)
    piids = ["abc", "def", "ghi", "jki"]
    award_ids = [1, 2, 3, 4, 5, 6, 7, 8, 9]

    awards = [
        baker.make("search.AwardSearch", piid=random.choice(piids), award_id=award_id, action_date="2020-01-01"),
        *baker.make(
            "search.AwardSearch",
            piid=random.choice(piids),
            award_id=cycle(award_ids),
            _quantity=9,
            action_date="2020-01-01",
        ),
    ]

    for index, award in enumerate(awards):
        baker.make("awards.FinancialAccountsByAwards", submission_id=10, award=award, disaster_emergency_fund=defc)
        if index % 2 == 0:
            AwardSearch.objects.filter(award_id=award.award_id).update(
                update_date=OLD_DATE
            )  # convoluted line to sidestep auto_now()
        else:
            AwardSearch.objects.filter(award_id=award.award_id).update(
                update_date=yesterday
            )  # convoluted line to sidestep auto_now()

    yield award_id, yesterday


@pytest.fixture
def award_data3(db):
    defc = baker.make("references.DisasterEmergencyFundCode", code="P", group_name="covid_19")

    aw1 = baker.make("search.AwardSearch", award_id=19834, fain="abc123", action_date="2020-01-01")
    aw2 = baker.make("search.AwardSearch", award_id=10938, fain="zyx987", action_date="2020-01-01")

    AwardSearch.objects.filter(award_id=aw1.award_id).update(
        update_date=OLD_DATE
    )  # convoluted line to sidestep auto_now()
    AwardSearch.objects.filter(award_id=aw2.award_id).update(
        update_date=OLD_DATE
    )  # convoluted line to sidestep auto_now()

    baker.make("awards.FinancialAccountsByAwards", submission_id=10, award=aw1, disaster_emergency_fund=defc)
    baker.make("awards.FinancialAccountsByAwards", submission_id=10, award=aw2, disaster_emergency_fund=defc)
    baker.make("awards.FinancialAccountsByAwards", submission_id=11, award=aw1, disaster_emergency_fund=defc)

    yield aw1, aw2


@pytest.fixture
def award_data4(db):
    defc = baker.make("references.DisasterEmergencyFundCode", code="P", group_name="covid_19")

    aw1 = baker.make("search.AwardSearch", award_id=284, fain="abc123", action_date="2020-01-01")
    aw2 = baker.make("search.AwardSearch", award_id=9081, fain="zyx987", action_date="2020-01-01")

    AwardSearch.objects.filter(award_id=aw1.award_id).update(
        update_date=OLD_DATE
    )  # convoluted line to sidestep auto_now()
    AwardSearch.objects.filter(award_id=aw2.award_id).update(
        update_date=OLD_DATE
    )  # convoluted line to sidestep auto_now()

    baker.make("awards.FinancialAccountsByAwards", submission_id=9, award=aw1, disaster_emergency_fund=defc)
    baker.make("awards.FinancialAccountsByAwards", submission_id=9, award=aw2, disaster_emergency_fund=defc)
    baker.make("awards.FinancialAccountsByAwards", submission_id=11, award=aw1, disaster_emergency_fund=defc)

    yield aw1, aw2


@pytest.fixture
def award_mixed_periods(db):
    defc = baker.make("references.DisasterEmergencyFundCode", code="O", group_name="covid_19")

    aw1 = baker.make("search.AwardSearch", award_id=1, piid="abc123", action_date="2020-01-01")
    aw2 = baker.make("search.AwardSearch", award_id=2, piid="zyx987", action_date="2020-01-01")
    aw3 = baker.make("search.AwardSearch", award_id=3, piid="ABC321", action_date="2020-01-01")
    aw4 = baker.make("search.AwardSearch", award_id=4, piid="ZYX789", action_date="2020-01-01")

    AwardSearch.objects.filter(award_id=aw1.award_id).update(
        update_date=OLD_DATE
    )  # convoluted line to sidestep auto_now()
    AwardSearch.objects.filter(award_id=aw2.award_id).update(
        update_date=OLD_DATE
    )  # convoluted line to sidestep auto_now()
    AwardSearch.objects.filter(award_id=aw3.award_id).update(
        update_date=OLD_DATE
    )  # convoluted line to sidestep auto_now()
    AwardSearch.objects.filter(award_id=aw4.award_id).update(
        update_date=OLD_DATE
    )  # convoluted line to sidestep auto_now()

    baker.make("awards.FinancialAccountsByAwards", submission_id=10, award=aw1, disaster_emergency_fund=defc)
    baker.make("awards.FinancialAccountsByAwards", submission_id=10, award=aw2, disaster_emergency_fund=defc)
    baker.make("awards.FinancialAccountsByAwards", submission_id=11, award=aw1, disaster_emergency_fund=defc)

    baker.make("awards.FinancialAccountsByAwards", submission_id=20, award=aw3, disaster_emergency_fund=defc)
    baker.make("awards.FinancialAccountsByAwards", submission_id=20, award=aw4, disaster_emergency_fund=defc)
    baker.make("awards.FinancialAccountsByAwards", submission_id=21, award=aw3, disaster_emergency_fund=defc)

    yield aw1, aw2, aw3, aw4


def test_happy_path(submissions, award_data1):
    """Simple test case: update single award record"""
    today = datetime.now(timezone.utc)

    AwardSearch.objects.filter(award_id=award_data1).update(update_date=OLD_DATE)
    original_datetime = Award.objects.get(id=award_data1)

    call_command(SCRIPT_NAME)

    after = Award.objects.get(id=award_data1)

    assert original_datetime.update_date.strftime("%Y-%m-%d") == OLD_DATE, "Sanity check failed"
    assert original_datetime.update_date != after.update_date, "Award was not properly updated"
    assert after.update_date - today < timedelta(minutes=1), "New datetime isn't today"


def test_count_only_flag(submissions, award_data1):
    """No awards should be updated when --count-only flag is present"""

    AwardSearch.objects.filter(award_id=award_data1).update(update_date=OLD_DATE)
    original_datetime = Award.objects.get(id=award_data1)

    call_command(SCRIPT_NAME, "--dry-run")
    call_command(SCRIPT_NAME, "--all", "--dry-run")

    after = Award.objects.get(id=award_data1)

    assert original_datetime.update_date == after.update_date, "Award was incorrectly updated"


def test_multiple_awards_update_date(submissions, award_data2):
    """Update 5 award records, excluding 5 based on update_date"""
    award_id, yesterday = award_data2
    today = datetime.now(timezone.utc)

    assert Award.objects.count() == 10, "Missing Award objects"
    assert Award.objects.filter(update_date__lt="2020-05-01").count() == 5, "Incorrect number of stale awards"
    assert Award.objects.filter(update_date__gt=yesterday).count() == 0, "Incorrect number of recent awards"

    call_command(SCRIPT_NAME)

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

    call_command(SCRIPT_NAME)

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

    call_command(SCRIPT_NAME)

    old_award_after = Award.objects.filter(fain=old_award.fain).aggregate(Max("update_date"))
    new_award_after = Award.objects.filter(fain=current_award.fain).aggregate(Max("update_date"))

    assert old_award_after["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "Award was incorrectly updated"
    assert new_award_after["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "Award was incorrectly updated"

    call_command(SCRIPT_NAME, "--all")

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

    assert old_monthly_award_before["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "Incorrect starting date"
    assert new_monthly_award_before["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "Incorrect starting date"
    assert old_quarterly_award_before["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "Incorrect starting date"
    assert new_quarterly_award_before["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "Incorrect starting date"

    call_command(SCRIPT_NAME)

    old_monthly_award_after = Award.objects.filter(piid=old_monthly_award.piid).aggregate(Max("update_date"))
    new_monthly_award_after = Award.objects.filter(piid=current_monthly_award.piid).aggregate(Max("update_date"))
    old_quarterly_award_after = Award.objects.filter(piid=old_quarterly_award.piid).aggregate(Max("update_date"))
    new_quarterly_award_after = Award.objects.filter(piid=current_quarterly_award.piid).aggregate(Max("update_date"))

    assert old_monthly_award_after["update_date__max"].strftime("%Y-%m-%d") > OLD_DATE, "Update didn't occur"
    assert new_monthly_award_after["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "Incorrectly updated"
    assert old_quarterly_award_after["update_date__max"].strftime("%Y-%m-%d") > OLD_DATE, "Update didn't occur"
    assert new_quarterly_award_after["update_date__max"].strftime("%Y-%m-%d") == OLD_DATE, "Incorrectly updated"
