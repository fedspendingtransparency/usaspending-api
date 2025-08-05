"""
Jira Ticket Number(s): DEV-5343

    Helper functions/classes for CARES Act test data generation.

Expected CLI:

    None.  This is a helper file, silly.

Purpose:

    This script generates or participates in the generation of sample CARES Act data for testing
    and development purposes.  It generates these data from existing data by duplicating and
    modifying existing submissions and File A/B/C records.  Data points are adjusted in an attempt
    to make them seem realistic and true to their actual source submissions.

    These data will not be perfect, obviously, but they should be sufficient for testing.

Life expectancy:

    This file should live until CARES Act features have gone live.

    Be sure to delete all files/directories associated with this ticket:
        - job_archive/management/commands/generate_cares_act_test_copy_submissions.py
        - job_archive/management/commands/generate_cares_act_test_def_codes.py
        - job_archive/management/commands/generate_cares_act_test_helpers.py
        - job_archive/management/commands/generate_cares_act_test_monthly_submissions.py
        - job_archive/management/commands/generate_cares_act_test_data_sqls

"""

import logging
import re

from argparse import ArgumentTypeError
from collections import namedtuple
from datetime import timedelta, date
from pathlib import Path
from usaspending_api.common.helpers.sql_helpers import execute_sql_return_single_value, execute_dml_sql
from usaspending_api.common.helpers.timing_helpers import ScriptTimer
from usaspending_api.references.models import DisasterEmergencyFundCode
from usaspending_api.submissions.models import SubmissionAttributes


logger = logging.getLogger("script")


class OneLineTimer(ScriptTimer):
    def log_starting_message(self):
        pass

    def log_success_message(self):
        pass


def add_argument_destination_fiscal_period(parser):
    parser.add_argument(
        "destination fiscal period", type=argparse_fiscal_period, help="Fiscal period to copy submissions to."
    )


def add_argument_destination_fiscal_year(parser):
    parser.add_argument(
        "destination fiscal year", type=argparse_fiscal_year, help="Fiscal year to copy submissions to."
    )


def add_argument_fiscal_period(parser):
    parser.add_argument("fiscal period", type=argparse_fiscal_period, help='Fiscal period to be "enhanced".')


def add_argument_fiscal_quarter(parser):
    parser.add_argument("fiscal quarter", type=argparse_fiscal_quarter, help='Fiscal quarter to be "enhanced".')


def add_argument_fiscal_year(parser):
    parser.add_argument("fiscal year", type=argparse_fiscal_year, help='Fiscal year to be "enhanced".')


def add_argument_nuclear_option(parser):
    parser.add_argument(
        "--nuke-destination-period-before-copy",
        action="store_true",
        help=(
            "This script will burst into flames if you attempt to copy to a fiscal period that "
            "has already been copied to.  This switch will empty the destination period  before "
            "attempting to copy."
        ),
    )


def add_argument_rds(parser):
    parser.add_argument(
        "--yes-i-know-its-rds",
        action="store_true",
        help="Just a safety precaution.  This switch is required to run against an RDS instance.",
    )


def add_argument_source_fiscal_period(parser):
    parser.add_argument(
        "source fiscal period", type=argparse_fiscal_period, help="Fiscal period to copy submissions from."
    )


def add_argument_source_fiscal_year(parser):
    parser.add_argument("source fiscal year", type=argparse_fiscal_year, help="Fiscal year to copy submissions from.")


def add_argument_vacuum(parser):
    parser.add_argument(
        "--vacuum", action="store_true", help="If you'd like the script to perform a little housekeeping for you."
    )


def add_argument_warning_epilog(parser):
    parser.epilog = (
        "WARNING!  THIS SCRIPT WILL IRREPARABLY MODIFY SUBMISSIONS AND FILE A/B/C DATA IN THE TARGET "
        "DATABASE!  DO NOT RUN ON PRODUCTION OR ANY DATABASE YOU CANNOT EASILY RESTORE!"
    )


def argparse_fiscal_period(input_string):
    msg = "Fiscal period must be a numeric value from 2 to 12.  Period 2 encompasses periods 1 and 2."
    if not re.fullmatch("[0-9]{1,2}", input_string):
        raise ArgumentTypeError(msg)
    fiscal_period = int(input_string)
    if fiscal_period < 2 or fiscal_period > 12:
        raise ArgumentTypeError(msg)
    return fiscal_period


def argparse_fiscal_quarter(input_string):
    msg = "Fiscal quarter must be a single numeric digit from 1 to 4."
    if not re.fullmatch("[1-4]", input_string):
        raise ArgumentTypeError(msg)
    return int(input_string)


def argparse_fiscal_year(input_string):
    if not re.fullmatch("[0-9]{4}", input_string):
        raise ArgumentTypeError("Fiscal year must be a number in the form of YYYY.")
    return int(input_string)


def get_all_period_start_end_dates(fiscal_year):
    period_starts = {
        p + 1: (date(fiscal_year - 1, 10 + p, 1) if p < 3 else date(fiscal_year, p - 2, 1)) for p in range(13)
    }
    period_ends = {p + 1: period_starts[p + 2] - timedelta(days=1) for p in range(12)}

    # There is no fiscal period 1 and fiscal period 2 starts where fiscal period 1 would have.
    period_starts[2] = period_starts[1]
    del period_starts[1]
    del period_ends[1]

    return period_starts, period_ends


def get_clone_periods(fiscal_quarter):
    """These are all of the periods in a quarter that are not the quarter end period."""
    return {1: [2], 2: [4, 5], 3: [7, 8], 4: [10, 11]}[fiscal_quarter]


def get_fiscal_quarter_final_period(fiscal_quarter):
    return fiscal_quarter * 3


def get_fiscal_quarter_from_period(fiscal_period):
    return (fiscal_period + 2) // 3


def get_period_start_end_dates(fiscal_year, fiscal_period):
    period_starts, period_ends = get_all_period_start_end_dates(fiscal_year)
    return period_starts[fiscal_period], period_ends[fiscal_period]


def get_quarter_start_date_from_period(fiscal_year, fiscal_period):
    """Quirky little function to find the quarter start date for the quarter the period falls in."""
    fiscal_quarter = get_fiscal_quarter_from_period(fiscal_period)
    first_period = get_clone_periods(fiscal_quarter)[0]
    period_starts, _ = get_all_period_start_end_dates(fiscal_year)
    return period_starts[first_period]


def is_quarter_final_period(fiscal_period):
    return fiscal_period in (3, 6, 9, 12)


def is_rds():
    """Not foolproof, but will hopefully prevent a few accidents between now and the end of CARES Act."""
    return "rdsdbdata" in execute_sql_return_single_value("show data_directory")


def period_has_submissions(fiscal_year, fiscal_period):
    return (
        SubmissionAttributes.objects.filter(
            reporting_fiscal_year=fiscal_year, reporting_fiscal_period=fiscal_period
        ).count()
        > 0
    )


def quarter_has_monthly_data(fiscal_year, fiscal_quarter):
    return (
        SubmissionAttributes.objects.filter(
            reporting_fiscal_year=fiscal_year, reporting_fiscal_quarter=fiscal_quarter, quarter_format_flag=False
        ).count()
        > 0
    )


def quarter_has_submissions(fiscal_year, fiscal_quarter):
    return (
        SubmissionAttributes.objects.filter(
            reporting_fiscal_year=fiscal_year, reporting_fiscal_quarter=fiscal_quarter
        ).count()
        > 0
    )


def read_sql_file(file_name):
    return (Path(__file__).resolve().parent / "generate_cares_act_test_data_sqls" / file_name).read_text()


def record_base_submission_ids():
    run_sqls(
        split_sql(
            """
                -- LOG: Record base submission ids
                alter table submission_attributes add column if not exists _base_submission_id int;

                update  submission_attributes
                set     _base_submission_id = submission_id
                where   _base_submission_id is null;
            """
        )
    )


def run_sqls(sqls):
    for s in sqls:
        with OneLineTimer(s.log) as t:
            count = execute_dml_sql(s.sql)
        logger.info(t.success_message + (f"... {count:,} rows affected" if count is not None else ""))


def shift_id(fiscal_year, fiscal_period):
    """
    To prevent ID collisions, we're going to prefix IDs with YYPP where YY is the two digit
    year and PP is the fiscal period.  We'll then shift all that by 1 million.  The current
    max submission id is 27,639 and we only get about 3,600 submissions a year so this should
    easily carry us for a while.  So, for example, submission id 27,655 copied to FY2021P7
    will receive an id of 2,107,027,655.
    """
    return (fiscal_year % 100 * 100 + fiscal_period) * 1000000


def split_sql(sql):
    """
    We use "-- SPLIT --" to be explicit... plus some of our SQLs include more than one
    statement (multiple semi-colons).  The "-- LOG:" thing was just convenient.  A "comments
    with the code" kind of thing.
    """
    SQL = namedtuple("SQL", ["sql", "log"])
    sqls = sql.split("-- SPLIT --")
    return [SQL(s, re.search("-- LOG: (.+)$", s, re.MULTILINE)[1]) for s in sqls]


def vacuum_tables():
    table_names = [
        "submission_attributes",
        "appropriation_account_balances",
        "financial_accounts_by_program_activity_object_class",
        "financial_accounts_by_awards",
    ]
    for table_name in table_names:
        with OneLineTimer(f"Vacuum {table_name}") as t:
            execute_dml_sql(f'vacuum (full, analyze) "{table_name}"')
        logger.info(t.success_message)


def validate_destination_has_no_data(nuclear_option, destination_fiscal_year, destination_fiscal_period):
    """Really just a warning, not a hard validation."""
    if not nuclear_option and period_has_submissions(destination_fiscal_year, destination_fiscal_period):
        logger.warning(
            "WARNING:  The destination period has submissions and the "
            "--nuke-destination-period-before-copy switch has not been provided.  We will attempt to "
            "run anyway, but if you see a duplicate primary key error, this is likely the cause."
        )


def validate_disaster_emergency_fund_code_table_has_data():
    if DisasterEmergencyFundCode.objects.count() == 0:
        raise RuntimeError(
            f"The {DisasterEmergencyFundCode._meta.db_table} table is empty.  This is a new "
            f"table and is required for this script.  The load_disaster_emergency_fund_codes "
            f"loader probably just hasn't been run against this database yet.  Why don't you "
            f"scurry along and deal with that right quick.  Thank you!"
        )


def validate_not_monthly_to_quarterly(source_fiscal_period, destination_fiscal_period):
    if not is_quarter_final_period(source_fiscal_period) and is_quarter_final_period(destination_fiscal_period):
        raise RuntimeError(
            "Unfortunately, copying from a monthly period to a quarterly period is not supported.  "
            "This is because we cannot currently fabricate quarterly records from monthly data."
        )


def validate_not_quarterly_to_monthly(source_fiscal_period, destination_fiscal_period):
    """Really just a warning, not a hard validation."""
    if is_quarter_final_period(source_fiscal_period) and not is_quarter_final_period(destination_fiscal_period):
        logger.warning(
            "WARNING:  You are attempting to copy quarterly data into a monthly period.  This is "
            "fine, however, you should know that only about 2/3 of the records will be copied.  "
            "This is because we use modulus math to identify which records should be converted "
            "to monthly in order to be consistent with the other tools in this package.  Those with "
            "ids evenly divisible by 3 will remain quarterly and will not be copied."
        )


def validate_not_rds(allow_rds):
    if not allow_rds and is_rds():
        raise RuntimeError(
            "You appear to be running against an RDS instance.  Consider using the --yes-i-know-its-rds "
            "switch if this was intended."
        )


def validate_not_same_period(
    source_fiscal_year, source_fiscal_period, destination_fiscal_year, destination_fiscal_period
):
    if source_fiscal_year == destination_fiscal_year and source_fiscal_period == destination_fiscal_period:
        raise RuntimeError("Source and destination periods may not be the same.")


def validate_period_has_submissions(fiscal_year, fiscal_period):
    if not period_has_submissions(fiscal_year, fiscal_period):
        raise RuntimeError(
            f"Well congratulations.  You've managed to choose a fiscal period with no "
            f"submissions.  Give 'select reporting_fiscal_year, reporting_fiscal_period, "
            f"count(*) from submission_attributes where group by reporting_fiscal_year, "
            f"reporting_fiscal_period order by reporting_fiscal_year, "
            f"reporting_fiscal_period;' a whirl and try again."
        )


def validate_quarter_has_submissions(fiscal_year, fiscal_quarter):
    if not quarter_has_submissions(fiscal_year, fiscal_quarter):
        raise RuntimeError(
            f"Well congratulations.  You've managed to choose a fiscal quarter with no "
            f"submissions.  Give 'select reporting_fiscal_year, reporting_fiscal_quarter, "
            f"count(*) from submission_attributes group by reporting_fiscal_year, "
            f"reporting_fiscal_quarter order by reporting_fiscal_year,"
            f"reporting_fiscal_quarter;' a whirl and try again."
        )
