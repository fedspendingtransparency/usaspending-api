"""
Jira Ticket Number(s): DEV-5343

    Generate test CARES Act monthly submissions and DEFC Files B and C data.

Expected CLI:

    $ ./manage.py generate_cares_act_test_data 2020 2  # which are FY and FQ - use any FY and FQ you want

Purpose:

    This script deterministically generates sample CARES Act data for testing and development
    purposes.  It generates this data from existing data by duplicating and modifying existing
    submissions and File A/B/C records.  Data points are adjusted in an attempt to make them
    seem realistic and true to their actual source submissions.

    These data will not be perfect, obviously, but they should be sufficient for testing.

Life expectancy:

    This file should live until CARES Act features have gone live.  Also delete the
    generate_cares_act_test_data_sqls directory and supporting SQL files.

"""
import logging
import re

from argparse import ArgumentTypeError
from collections import namedtuple
from datetime import timedelta, date
from django.core.management.base import BaseCommand
from django.db import transaction
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


class Command(BaseCommand):
    help = (
        "This script generates test CARES Act submissions and File A/B/C data from existing "
        "pre-CARES Act submissions."
    )

    fiscal_year = None
    fiscal_quarter = None
    allow_rds = False
    vacuum = False

    fiscal_period = None
    clone_periods = None
    period_ratios = None
    period_starts = None
    period_ends = None

    def add_arguments(self, parser):

        parser.add_argument(
            "fiscal year", type=self.validate_fiscal_year, help='Fiscal year to be "enhanced".',
        )

        parser.add_argument(
            "fiscal quarter", type=self.validate_fiscal_quarter, help='Fiscal quarter to be "enhanced".',
        )

        parser.add_argument(
            "--yes-i-know-its-rds",
            action="store_true",
            help="Just a safety precaution.  This switch is required to run against an RDS instance.",
        )

        parser.add_argument(
            "--vacuum", action="store_true", help="If you'd like the script to perform a little housekeeping for you.",
        )

        parser.epilog = (
            "WARNING!  THIS SCRIPT WILL IRREPARABLY MODIFY SUBMISSIONS AND FILE A/B/C DATA IN THE TARGET "
            "DATABASE!  DO NOT RUN ON PRODUCTION OR ANY DATABASE YOU CANNOT EASILY RESTORE!"
        )

    def handle(self, *args, **options):
        with ScriptTimer("CARES Act test data generation"):

            self.set_state(options)
            self.perform_validations()

            with transaction.atomic():
                self.record_base_submission_ids()
                self.clone_submissions()
                self.update_submissions()
                self.clone_for_defc()

                t = ScriptTimer("Commit transaction")
            t.log_success_message()

            if self.vacuum:
                self.vacuum_tables()

    def set_state(self, options):
        self.fiscal_year = options["fiscal year"]
        self.fiscal_quarter = options["fiscal quarter"]
        self.allow_rds = options["yes_i_know_its_rds"]
        self.vacuum = options["vacuum"]

        # Some other nice-to-haves we'll be using in loops.
        self.fiscal_period = {1: 3, 2: 6, 3: 9, 4: 12}[self.fiscal_quarter]
        self.clone_periods = {1: [2], 2: [4, 5], 3: [7, 8], 4: [10, 11]}[self.fiscal_quarter]

        # This adds some variability to clones.  Ratios for periods in the same quarter should add up to 1.
        # If they don't, no big deal really, but it keeps the dollar figures in line across Files A, B, and C.
        self.period_ratios = {2: 0.3, 3: 0.7, 4: 0.1, 5: 0.2, 6: 0.7, 7: 0.1, 8: 0.2, 9: 0.7, 10: 0.1, 11: 0.2, 12: 0.7}

        self.period_starts = {
            p + 1: (date(self.fiscal_year - 1, 10 + p, 1) if p < 3 else date(self.fiscal_year, p - 2, 1))
            for p in range(13)
        }
        self.period_ends = {p + 1: self.period_starts[p + 2] - timedelta(days=1) for p in range(12)}

        # There is no fiscal period 1 and fiscal period 2 starts where fiscal period 1 would have.
        self.period_starts[2] = self.period_starts[1]
        del self.period_starts[1]
        del self.period_ends[1]

    def perform_validations(self):
        if self.allow_rds is False and self.is_rds():
            raise RuntimeError(
                "You appear to be running against an RDS instance.  Consider using the --yes-i-know-its-rds "
                "switch if you no longer value your career."
            )

        if self.quarter_has_monthly_data():
            raise RuntimeError(
                f"There appears to be monthly data in FY{self.fiscal_year}Q{self.fiscal_quarter} which suggests "
                f"that CARES Act test data for this quarter have already been generated.  Multiple generations "
                f"for the same quarter is not currently supported.  Please choose another quarter."
            )

        if self.there_are_no_submissions():
            raise RuntimeError(
                f"Well congratulations.  You've managed to choose a fiscal quarter with no "
                f"submissions.  Give 'select reporting_fiscal_year, reporting_fiscal_quarter, count(*) from "
                f"submission_attributes where quarter_format_flag is true group by reporting_fiscal_year, "
                f"reporting_fiscal_quarter order by reporting_fiscal_year, reporting_fiscal_quarter;' a whirl "
                f"and try again."
            )

        if DisasterEmergencyFundCode.objects.count() == 0:
            raise RuntimeError(
                f"The {DisasterEmergencyFundCode._meta.db_table} table is empty.  This is a new table and is "
                f"required for this script.  The loader probably just hasn't been run against this database "
                f"yet.  Why don't you scurry along and deal with that right quick.  Thank you!"
            )

    @staticmethod
    def read_sql_file(file_name):
        return (Path(__file__).resolve().parent / "generate_cares_act_test_data_sqls" / file_name).read_text()

    @staticmethod
    def split_sql(sql):
        SQL = namedtuple("SQL", ["sql", "log"])
        sqls = sql.split("-- SPLIT --")
        return [SQL(s, re.search("-- LOG: (.+)$", s, re.MULTILINE)[1]) for s in sqls]

    @staticmethod
    def run_sqls(sqls):
        for s in sqls:
            with OneLineTimer(s.log) as t:
                count = execute_dml_sql(s.sql)
            logger.info(t.success_message + (f"... {count:,} rows affected" if count is not None else ""))

    def record_base_submission_ids(self):
        self.run_sqls(
            self.split_sql(
                """
                    -- LOG: Record base submission ids
                    alter table submission_attributes add column if not exists _base_submission_id int;

                    update  submission_attributes
                    set     _base_submission_id = submission_id
                    where   _base_submission_id is null;
                """
            )
        )

    def clone_submissions(self):
        sql = self.read_sql_file("clone_submissions.sql")
        for fiscal_period in self.clone_periods:
            sqls = self.split_sql(
                sql.format(
                    submission_id_shift=fiscal_period * 100000000,  # to prevent id collisions,
                    reporting_period_start=self.period_starts[fiscal_period],
                    reporting_period_end=self.period_ends[fiscal_period],
                    reporting_fiscal_period=fiscal_period,
                    filter_fiscal_year=self.fiscal_year,
                    filter_fiscal_period=self.fiscal_period,
                    adjustment_ratio=self.period_ratios[fiscal_period],
                )
            )
            self.run_sqls(sqls)

    def update_submissions(self):
        sql = self.read_sql_file("update_submissions.sql")
        sqls = self.split_sql(
            sql.format(
                reporting_period_start=self.period_starts[self.fiscal_period],
                reporting_period_end=self.period_ends[self.fiscal_period],
                filter_fiscal_year=self.fiscal_year,
                filter_fiscal_period=self.fiscal_period,
                adjustment_ratio=self.period_ratios[self.fiscal_period],
            )
        )
        self.run_sqls(sqls)

    def clone_for_defc(self):
        sql = self.read_sql_file("clone_for_defc.sql")
        # These are just carefully selected "random" values used to clone File B and C records.
        for p in [("I", 0.2, 13), ("F", 0.1, 11), ("L", 0.3, 10), ("M", 0.25, 8), ("N", 0.4, 7), ("O", 0.15, 5)]:
            sqls = self.split_sql(
                sql.format(
                    disaster_emergency_fund_code=p[0],
                    filter_fiscal_year=self.fiscal_year,
                    filter_fiscal_quarter=self.fiscal_quarter,
                    adjustment_ratio=p[1],
                    divisor=p[2],
                )
            )
            self.run_sqls(sqls)

    def there_are_no_submissions(self):
        return (
            SubmissionAttributes.objects.filter(
                reporting_fiscal_year=self.fiscal_year, reporting_fiscal_quarter=self.fiscal_quarter,
            ).count()
            == 0
        )

    def quarter_has_monthly_data(self):
        return (
            SubmissionAttributes.objects.filter(
                reporting_fiscal_year=self.fiscal_year,
                reporting_fiscal_quarter=self.fiscal_quarter,
                quarter_format_flag=False,
            ).count()
            > 0
        )

    @staticmethod
    def is_rds():
        """ Not foolproof, but will hopefully prevent a few accidents between now and the end of CARES Act. """
        return "rdsdbdata" in execute_sql_return_single_value("show data_directory")

    @staticmethod
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

    @staticmethod
    def validate_fiscal_year(input_string):
        if not re.fullmatch("[0-9]{4}", input_string):
            raise ArgumentTypeError("fiscal_year must be in the form of YYYY")
        fiscal_year = int(input_string)
        if fiscal_year < 2017:
            raise ArgumentTypeError("USASpending did not have account data prior to 2017")
        if fiscal_year > 2021:
            raise ArgumentTypeError("Oh man, if we're still working on CARES Act after 2021...")
        return fiscal_year

    @staticmethod
    def validate_fiscal_quarter(input_string):
        if not re.fullmatch("[0-9]", input_string):
            raise ArgumentTypeError("fiscal_quarter must be a single numeric digit between 1 and 4")
        fiscal_quarter = int(input_string)
        if fiscal_quarter < 1:
            raise ArgumentTypeError("fiscal_quarter must be a single numeric digit between 1 and 4")
        if fiscal_quarter > 4:
            raise ArgumentTypeError("This script does not support quantum quarters")
        return fiscal_quarter
