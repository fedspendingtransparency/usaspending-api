import pytest

from datetime import datetime, timedelta, timezone
from model_mommy import mommy
from usaspending_api.etl.management.helpers.load_submission import calculate_load_submissions_since_datetime


@pytest.mark.django_db
def test_calculate_load_submissions_since_datetime():
    assert calculate_load_submissions_since_datetime() is None

    mommy.make("submissions.SubmissionAttributes", published_date=None)
    assert calculate_load_submissions_since_datetime() is None

    mommy.make("submissions.SubmissionAttributes", published_date="2000-01-01")
    assert calculate_load_submissions_since_datetime() == datetime(2000, 1, 1, tzinfo=timezone.utc) - timedelta(days=7)
