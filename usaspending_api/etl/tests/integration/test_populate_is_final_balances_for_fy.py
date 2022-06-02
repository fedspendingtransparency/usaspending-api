import pytest

from model_bakery import baker
from django.core.management import call_command

from usaspending_api.submissions.models.submission_attributes import SubmissionAttributes

# No fixtures are used because each test requires a unique set of schedules and submissions.
# NOTE: Some schedules are not accurate to real life in order to include a broader set of
# revealed periods at the time the tests were written


@pytest.mark.django_db
def test_quarterly_followed_by_monthly():

    # WINDOW - Period 10
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=11,
        submission_fiscal_year=2020,
        submission_fiscal_month=10,
        is_quarter=False,
        submission_due_date="2020-08-21",
        submission_reveal_date="2020-08-22",
    )

    # WINDOW - Quarter 3
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=22,
        submission_fiscal_year=2020,
        submission_fiscal_month=9,
        is_quarter=True,
        submission_due_date="2020-08-14",
        submission_reveal_date="2020-08-15",
    )

    # SUBMISSION - Agency A - Period 10
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=1,
        toptier_code="A",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=10,
        quarter_format_flag=False,
        submission_window_id=11,
    )

    # SUBMISSION - Agency A - Quarter 3
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=2,
        toptier_code="A",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=9,
        quarter_format_flag=True,
        submission_window_id=22,
    )

    call_command("populate_is_final_balances_for_fy")

    monthly_submission = SubmissionAttributes.objects.get(submission_id=1)
    quarterly_submission = SubmissionAttributes.objects.get(submission_id=2)

    assert monthly_submission.is_final_balances_for_fy
    assert not quarterly_submission.is_final_balances_for_fy


@pytest.mark.django_db
def test_monthly_followed_by_quarterly():

    # WINDOW - Period 9
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=11,
        submission_fiscal_year=2020,
        submission_fiscal_month=9,
        is_quarter=False,
        submission_due_date="2020-07-30",
        submission_reveal_date="2020-07-31",
    )

    # WINDOW - Quarter 4
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=22,
        submission_fiscal_year=2020,
        submission_fiscal_month=12,
        is_quarter=True,
        submission_due_date="2020-08-16",
        submission_reveal_date="2020-08-17",
    )

    # SUBMISSION - Agency A - Period 9
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=1,
        toptier_code="A",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=9,
        quarter_format_flag=False,
        submission_window_id=11,
    )

    # SUBMISSION - Agency A - Quarter 4
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=2,
        toptier_code="A",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=12,
        quarter_format_flag=True,
        submission_window_id=22,
    )

    call_command("populate_is_final_balances_for_fy")

    monthly_submission = SubmissionAttributes.objects.get(submission_id=1)
    quarterly_submission = SubmissionAttributes.objects.get(submission_id=2)

    assert not monthly_submission.is_final_balances_for_fy
    assert quarterly_submission.is_final_balances_for_fy


@pytest.mark.django_db
def test_period_789_submissions():

    # WINDOW - Period 7
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=11,
        submission_fiscal_year=2020,
        submission_fiscal_month=7,
        is_quarter=False,
        submission_due_date="2020-07-30",
        submission_reveal_date="2020-07-31",
    )

    # WINDOW - Period 8
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=22,
        submission_fiscal_year=2020,
        submission_fiscal_month=8,
        is_quarter=False,
        submission_due_date="2020-07-30",
        submission_reveal_date="2020-07-31",
    )

    # WINDOW - Period 9
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=33,
        submission_fiscal_year=2020,
        submission_fiscal_month=9,
        is_quarter=False,
        submission_due_date="2020-07-30",
        submission_reveal_date="2020-07-31",
    )

    # SUBMISSION - Agency A - Period 7
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=1,
        toptier_code="A",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=7,
        quarter_format_flag=False,
        submission_window_id=11,
    )

    # SUBMISSION - Agency A - Period 8
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=2,
        toptier_code="A",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        submission_window_id=22,
    )

    # SUBMISSION - Agency A - Period 9
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=3,
        toptier_code="A",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=9,
        quarter_format_flag=False,
        submission_window_id=33,
    )

    call_command("populate_is_final_balances_for_fy")

    period_1_submission = SubmissionAttributes.objects.get(submission_id=1)
    period_2_submission = SubmissionAttributes.objects.get(submission_id=2)
    period_3_submission = SubmissionAttributes.objects.get(submission_id=3)

    assert not period_1_submission.is_final_balances_for_fy
    assert not period_2_submission.is_final_balances_for_fy
    assert period_3_submission.is_final_balances_for_fy


@pytest.mark.django_db
def test_no_final_balances_for_agency():
    """
    This unit test demonstrates that if an agency has made submissions, but none of
    those submissions are associated with the latest closed periods, the submissions
    it has made, won't be flagged as 'is_final_balances_for_fy'
    """

    # WINDOW - Period 8
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=11,
        submission_fiscal_year=2020,
        submission_fiscal_month=8,
        is_quarter=False,
        submission_due_date="2020-07-30",
        submission_reveal_date="2020-07-31",
    )

    # WINDOW - Period 10
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=22,
        submission_fiscal_year=2020,
        submission_fiscal_month=10,
        is_quarter=False,
        submission_due_date="2020-08-23",
        submission_reveal_date="2020-08-24",
    )

    # WINDOW - Quarter 3
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=33,
        submission_fiscal_year=2020,
        submission_fiscal_month=9,
        is_quarter=True,
        submission_due_date="2020-08-14",
        submission_reveal_date="2020-08-15",
    )

    # WINDOW - Quarter 4
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=44,
        submission_fiscal_year=2020,
        submission_fiscal_month=12,
        is_quarter=True,
        submission_due_date="2020-08-16",
        submission_reveal_date="2020-08-17",
    )

    # SUBMISSION - Agency A - Period 8
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=1,
        toptier_code="A",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=8,
        quarter_format_flag=False,
        submission_window_id=11,
    )

    # SUBMISSION - Agency A - Quarter 3
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=2,
        toptier_code="A",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=9,
        quarter_format_flag=True,
        submission_window_id=33,
    )

    call_command("populate_is_final_balances_for_fy")

    period_8_submission = SubmissionAttributes.objects.get(submission_id=1)
    quarter_3_submission = SubmissionAttributes.objects.get(submission_id=2)

    assert not period_8_submission.is_final_balances_for_fy
    assert not quarter_3_submission.is_final_balances_for_fy


@pytest.mark.django_db
def test_diff_agencies_using_monthly_and_quarterly():
    """
    This test demonstrates that different agencies may make quarterly and monthly
    submissions, and calculations are still performed correctly
    """

    # WINDOW - Period 9
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=11,
        submission_fiscal_year=2020,
        submission_fiscal_month=9,
        is_quarter=False,
        submission_due_date="2020-07-30",
        submission_reveal_date="2020-07-31",
    )

    # WINDOW - Period 10
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=22,
        submission_fiscal_year=2020,
        submission_fiscal_month=10,
        is_quarter=False,
        submission_due_date="2020-08-16",
        submission_reveal_date="2020-08-17",
    )

    # WINDOW - Quarter 3
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=33,
        submission_fiscal_year=2020,
        submission_fiscal_month=9,
        is_quarter=True,
        submission_due_date="2020-08-14",
        submission_reveal_date="2020-08-15",
    )

    # WINDOW - Quarter 4
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=44,
        submission_fiscal_year=2020,
        submission_fiscal_month=12,
        is_quarter=True,
        submission_due_date="2020-08-16",
        submission_reveal_date="2020-08-17",
    )

    # SUBMISSION - Agency A - Period 9
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=1,
        toptier_code="A",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=9,
        quarter_format_flag=True,
        submission_window_id=33,
    )

    # SUBMISSION - Agency A - Quarter 4
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=2,
        toptier_code="A",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=12,
        quarter_format_flag=True,
        submission_window_id=44,
    )

    # SUBMISSION - Agency B - Period 10
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=3,
        toptier_code="B",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=10,
        quarter_format_flag=False,
        submission_window_id=22,
    )

    # SUBMISSION - Agency B - Quarter 3
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=4,
        toptier_code="B",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=9,
        quarter_format_flag=True,
        submission_window_id=33,
    )

    call_command("populate_is_final_balances_for_fy")

    agency_a_period_9 = SubmissionAttributes.objects.get(submission_id=1)
    agency_a_quarter_4 = SubmissionAttributes.objects.get(submission_id=2)
    agency_b_period_10 = SubmissionAttributes.objects.get(submission_id=3)
    agency_b_quarter_3 = SubmissionAttributes.objects.get(submission_id=4)

    assert not agency_a_period_9.is_final_balances_for_fy
    assert agency_a_quarter_4.is_final_balances_for_fy
    assert agency_b_period_10.is_final_balances_for_fy
    assert not agency_b_quarter_3.is_final_balances_for_fy


@pytest.mark.django_db
def test_agency_across_years():

    # WINDOW 2019 Period 9
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=11,
        submission_fiscal_year=2019,
        submission_fiscal_month=9,
        is_quarter=False,
        submission_due_date="2019-07-30",
        submission_reveal_date="2019-07-31",
    )

    # WINDOW 2020 Period 9
    baker.make(
        "submissions.DABSSubmissionWindowSchedule",
        id=22,
        submission_fiscal_year=2020,
        submission_fiscal_month=9,
        is_quarter=False,
        submission_due_date="2020-07-30",
        submission_reveal_date="2020-07-31",
    )

    # SUBMISSION - Agency A - 2019 Period 9
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=1,
        toptier_code="A",
        reporting_fiscal_year=2019,
        reporting_fiscal_period=9,
        quarter_format_flag=False,
        submission_window_id=11,
    )

    # SUBMISSION - Agency A - 2020 Period 9
    baker.make(
        "submissions.SubmissionAttributes",
        submission_id=2,
        toptier_code="A",
        reporting_fiscal_year=2020,
        reporting_fiscal_period=9,
        quarter_format_flag=False,
        submission_window_id=22,
    )

    call_command("populate_is_final_balances_for_fy")

    period_2019_9 = SubmissionAttributes.objects.get(submission_id=1)
    period_2020_9 = SubmissionAttributes.objects.get(submission_id=2)

    assert period_2019_9.is_final_balances_for_fy
    assert period_2020_9.is_final_balances_for_fy
