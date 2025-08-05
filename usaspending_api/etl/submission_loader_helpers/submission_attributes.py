import logging

from django.core.management import call_command
from usaspending_api.etl.helpers import get_fiscal_quarter
from usaspending_api.etl.management.load_base import load_data_into_model
from usaspending_api.references.helpers import retrive_agency_name_from_code
from usaspending_api.submissions.models import SubmissionAttributes, DABSSubmissionWindowSchedule


logger = logging.getLogger("script")


def attempt_submission_update_only(submission_data):
    """
    If nothing about the submission has changed except maybe the certified_date then there's no
    sense reloading the record.  Update certified_date if necessary and return True letting the
    caller know a reload isn't necessary.  Otherwise return False.
    """
    submission_id = submission_data["submission_id"]

    submission = SubmissionAttributes.objects.filter(submission_id=submission_id).first()

    if (
        not submission
        or submission.submission_id != submission_id
        or submission.published_date != submission_data["published_date"]
        or submission.toptier_code != submission_data["toptier_code"]
        or submission.reporting_period_start != submission_data["reporting_start_date"]
        or submission.reporting_period_end != submission_data["reporting_end_date"]
        or submission.reporting_fiscal_year != submission_data["reporting_fiscal_year"]
        or submission.reporting_fiscal_period != submission_data["reporting_fiscal_period"]
        or submission.quarter_format_flag != submission_data["is_quarter_format"]
    ):
        return False

    if submission.certified_date != submission_data["certified_date"]:
        SubmissionAttributes.objects.filter(submission_id=submission_id).update(
            certified_date=submission_data["certified_date"], history=submission_data["history"]
        )

    return True


def get_submission_attributes(submission_id, submission_data):
    """
    For a specified broker submission, return the existing corresponding usaspending submission record or
    create and return a new one.
    """

    dabs_window = DABSSubmissionWindowSchedule.objects.filter(
        submission_fiscal_year=submission_data["reporting_fiscal_year"],
        submission_fiscal_month=submission_data["reporting_fiscal_period"],
        is_quarter=submission_data["is_quarter_format"],
    ).first()

    if not dabs_window:
        raise RuntimeError(f"Missing DABS Window record necessary for {submission_id}")

    # check if we already have an entry for this submission id; if not, create one
    submission_attributes, created = SubmissionAttributes.objects.get_or_create(
        submission_id=submission_id, defaults={"submission_window": dabs_window}
    )

    if created:
        # this is the first time we're loading this submission
        logger.info(f"Creating submission {submission_id}")

    else:
        # we've already loaded this submission, so delete it before reloading
        logger.info(f"Submission {submission_id} already exists. It will be deleted.")
        call_command("rm_submission", submission_id)

    submission_data["reporting_agency_name"] = retrive_agency_name_from_code(submission_data["toptier_code"])

    # Update and save submission attributes
    field_map = {
        "reporting_period_start": "reporting_start_date",
        "reporting_period_end": "reporting_end_date",
        "quarter_format_flag": "is_quarter_format",
    }

    # Create our value map - specific data to load
    value_map = {"reporting_fiscal_quarter": get_fiscal_quarter(submission_data["reporting_fiscal_period"])}

    new_submission = load_data_into_model(
        submission_attributes, submission_data, field_map=field_map, value_map=value_map, save=True
    )

    return new_submission
