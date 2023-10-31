from datetime import datetime, timezone, timedelta

import pytest
from model_bakery import baker

import usaspending_api.common.helpers.fiscal_year_helpers as fyh


@pytest.mark.django_db
def test_calculate_last_completed_fiscal_quarter():
    now = datetime.now(timezone.utc)
    yesterday = now + timedelta(days=-1)
    tomorrow = now + timedelta(days=1)
    current_fy = fyh.generate_fiscal_year(now)

    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2000,
        submission_reveal_date=now,
        submission_fiscal_quarter=1,
        submission_fiscal_month=3,
        is_quarter=True,
    )
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2000,
        submission_reveal_date=now,
        submission_fiscal_quarter=2,
        submission_fiscal_month=4,
        is_quarter=False,
    )
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=2010,
        submission_reveal_date=tomorrow,
        submission_fiscal_quarter=2,
        submission_fiscal_month=6,
        is_quarter=True,
    )
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=current_fy,
        submission_reveal_date=yesterday,
        submission_fiscal_quarter=3,
        submission_fiscal_month=9,
        is_quarter=True,
    )
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        submission_fiscal_year=current_fy,
        submission_reveal_date=now,
        submission_fiscal_quarter=4,
        submission_fiscal_month=12,
        is_quarter=True,
    )

    assert fyh.calculate_last_completed_fiscal_quarter(2000) == 1  # not 2, since is_quarter=False
    assert fyh.calculate_last_completed_fiscal_quarter(2001) is None  # no row in table for 2001
    assert fyh.calculate_last_completed_fiscal_quarter(2010) is None  # not revealed yet
    assert fyh.calculate_last_completed_fiscal_quarter(current_fy) == 4  # not 3, since both are revealed & quarters
