"""
Jira Ticket Number(s): DEV-5343

    This management command deterministically copies submissions from a source fiscal period into
    a destination fiscal period, including matching File A, B, and C records.  Records will be
    updated to reflect their new period.

Expected CLI:

    $ ./manage.py generate_cares_act_test_copy_submissions 2020 2 2020 3

    where 2020 2 is the source fiscal year and period and 2020 3 is the destination fiscal year and period

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

from django.core.management.base import BaseCommand
from django.db import transaction
from usaspending_api.common.helpers.timing_helpers import ScriptTimer
from usaspending_api.database_scripts.job_archive.management.commands import generate_cares_act_test_helpers as helper


class Command(BaseCommand):
    help = (
        "Deterministically copy submissions from one fiscal period to another fiscal period.  There "
        "are definitely some nuances with how this works.  In keeping with other tools in this suite, "
        "only about 2/3 of submissions will be treated as monthly.  The other 1/3 will be treated as "
        "quarterly.  This is controlled via modulus math with the primary key.  Note that you cannot "
        "copy from a non-quarter period to a quarter period because we are currently unable to fabricate "
        "quarterly data from monthly."
    )

    source_fiscal_year = None
    source_fiscal_period = None
    destination_fiscal_year = None
    destination_fiscal_period = None
    nuke = False
    allow_rds = False
    vacuum = False

    source_fyp = None
    destination_fyp = None

    def add_arguments(self, parser):
        helper.add_argument_source_fiscal_year(parser)
        helper.add_argument_source_fiscal_period(parser)
        helper.add_argument_destination_fiscal_year(parser)
        helper.add_argument_destination_fiscal_period(parser)
        helper.add_argument_nuclear_option(parser)
        helper.add_argument_rds(parser)
        helper.add_argument_vacuum(parser)
        helper.add_argument_warning_epilog(parser)

    def handle(self, *args, **options):
        self.set_state(options)
        self.perform_validations()

        with ScriptTimer(f"Copy submissions from {self.source_fyp} to {self.destination_fyp}"):

            with transaction.atomic():
                helper.record_base_submission_ids()
                if self.nuke:
                    self.delete_destination_period()
                self.copy_submissions()

                t = ScriptTimer("Commit transaction")
            t.log_success_message()

            if self.vacuum:
                helper.vacuum_tables()

    def set_state(self, options):
        self.source_fiscal_year = options["source fiscal year"]
        self.source_fiscal_period = options["source fiscal period"]
        self.destination_fiscal_year = options["destination fiscal year"]
        self.destination_fiscal_period = options["destination fiscal period"]
        self.nuke = options["nuke_destination_period_before_copy"]
        self.allow_rds = options["yes_i_know_its_rds"]
        self.vacuum = options["vacuum"]

        self.source_fyp = f"{self.source_fiscal_year}P{self.source_fiscal_period}"
        self.destination_fyp = f"{self.destination_fiscal_year}P{self.destination_fiscal_period}"

    def perform_validations(self):
        helper.validate_not_rds(self.allow_rds)
        helper.validate_not_same_period(
            self.source_fiscal_year,
            self.source_fiscal_period,
            self.destination_fiscal_year,
            self.destination_fiscal_period,
        )
        helper.validate_not_monthly_to_quarterly(self.source_fiscal_period, self.destination_fiscal_period)
        helper.validate_period_has_submissions(self.source_fiscal_year, self.source_fiscal_period)
        helper.validate_not_quarterly_to_monthly(self.source_fiscal_period, self.destination_fiscal_period)
        helper.validate_destination_has_no_data(self.nuke, self.destination_fiscal_year, self.destination_fiscal_period)

    def delete_destination_period(self):
        sql = helper.read_sql_file("delete_destination_period.sql")
        helper.run_sqls(
            helper.split_sql(
                sql.format(
                    filter_fiscal_year=self.destination_fiscal_year, filter_fiscal_period=self.destination_fiscal_period
                )
            )
        )

    def copy_submissions(self):
        period_start, period_end = helper.get_period_start_end_dates(
            self.destination_fiscal_year, self.destination_fiscal_period
        )
        sql = helper.read_sql_file("copy_submissions.sql")
        helper.run_sqls(
            helper.split_sql(
                sql.format(
                    source_fiscal_year=self.source_fiscal_year,
                    source_fiscal_period=self.source_fiscal_period,
                    destination_fiscal_year=self.destination_fiscal_year,
                    destination_fiscal_period=self.destination_fiscal_period,
                    destination_fiscal_quarter=helper.get_fiscal_quarter_from_period(self.destination_fiscal_period),
                    submission_id_shift=helper.shift_id(self.destination_fiscal_year, self.destination_fiscal_period),
                    reporting_period_start=period_start,
                    reporting_period_end=period_end,
                    quarter_reporting_period_start=helper.get_quarter_start_date_from_period(
                        self.destination_fiscal_year, self.destination_fiscal_period
                    ),
                )
            )
        )
