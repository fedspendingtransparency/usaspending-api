import contextlib
import csv
import os
import tempfile

import pytest
from model_mommy import mommy

from usaspending_api.etl import helpers
from usaspending_api.submissions.models import SubmissionAttributes


@contextlib.contextmanager
def mutated_csv(filename, mutator):
    """Returns tempfile copied from filename, but mutated.

    If `filename` points to a CSV, then we load it,
    apply the `mutator` function to each row, and save
    the changed data to a tempfile, which is returned.

    Used to create small, specific variants on test CSVs.
    """

    outfile = tempfile.NamedTemporaryFile(mode="w+t", delete=False)
    with open(filename) as infile:
        reader = csv.DictReader(infile)
        writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
        writer.writeheader()
        for row in reader:
            writer.writerow(mutator(row))
    outfile.close()
    yield outfile
    os.unlink(outfile.name)


@pytest.mark.django_db
def test_get_previous_submission():
    """Test the process for determining the most recent submission in the current FY."""
    # set up some related submissions
    sub1 = mommy.make(
        SubmissionAttributes,
        toptier_code="073",
        reporting_fiscal_year=2017,
        reporting_fiscal_period=9,
        quarter_format_flag=True,
    )
    mommy.make(
        SubmissionAttributes,
        toptier_code="073",
        reporting_fiscal_year=2017,
        reporting_fiscal_period=6,
        quarter_format_flag=True,
    )

    # Submission for same CGAC + a later period should return sub1 as previous submission
    assert helpers.get_previous_submission("073", 2017, 12) == sub1
    # Previous submission lookup should not find a match for an earlier submission
    assert helpers.get_previous_submission("073", 2017, 3) is None
    # Previous submission lookup should not match against a different fiscal year
    assert helpers.get_previous_submission("073", 2018, 3) is None
    # Previous submission lookup should not match against a different agency (CGAC)
    assert helpers.get_previous_submission("ABC", 2017, 12) is None

    mommy.make(
        SubmissionAttributes,
        toptier_code="020",
        reporting_fiscal_year=2016,
        reporting_fiscal_period=6,
        quarter_format_flag=False,
    )

    # Previous submission lookup should only match a quarterly submission
    assert helpers.get_previous_submission("020", 2016, 9) is None
