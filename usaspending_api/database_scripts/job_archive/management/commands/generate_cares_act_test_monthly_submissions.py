"""
Jira Ticket Number(s): DEV-5343

    This management command deterministically converts about 2/3 of submissions in the fiscal quarter
    provided into monthly submissions.  Monthly submissions are new to USAspending and are not yet
    being provided by Broker.

Expected CLI:

    $ ./manage.py generate_cares_act_test_monthly_submissions 2020 2

    where 2020 is the fiscal year and 2 is the fiscal quarter

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
        "Deterministically convert about 2/3 of submissions in the fiscal year and quarter "
        "provided into monthly submissions."
    )

    fiscal_year = None
    fiscal_quarter = None
    allow_rds = False
    vacuum = False

    fiscal_period = None
    period_ratios = None
    period_start = None
    period_end = None

    def add_arguments(self, parser):
        helper.add_argument_fiscal_year(parser)
        helper.add_argument_fiscal_quarter(parser)
        helper.add_argument_rds(parser)
        helper.add_argument_vacuum(parser)
        helper.add_argument_warning_epilog(parser)

    def handle(self, *args, **options):
        self.set_state(options)
        self.perform_validations()

        with ScriptTimer(f"Convert FY{self.fiscal_year}Q{self.fiscal_quarter} submissions into monthly submissions"):

            with transaction.atomic():
                helper.record_base_submission_ids()
                self.clone_submissions()
                self.update_base_submissions()

                t = ScriptTimer("Commit transaction")
            t.log_success_message()

            if self.vacuum:
                helper.vacuum_tables()

    def set_state(self, options):
        self.fiscal_year = options["fiscal year"]
        self.fiscal_quarter = options["fiscal quarter"]
        self.allow_rds = options["yes_i_know_its_rds"]
        self.vacuum = options["vacuum"]

        self.fiscal_period = helper.get_fiscal_quarter_final_period(self.fiscal_quarter)
        self.period_ratios = {2: 0.3, 3: 0.7, 4: 0.1, 5: 0.2, 6: 0.7, 7: 0.1, 8: 0.2, 9: 0.7, 10: 0.1, 11: 0.2, 12: 0.7}
        self.period_start, self.period_end = helper.get_period_start_end_dates(self.fiscal_year, self.fiscal_period)

    def perform_validations(self):
        helper.validate_not_rds(self.allow_rds)
        helper.validate_quarter_has_submissions(self.fiscal_year, self.fiscal_quarter)

    def clone_submissions(self):
        sql = helper.read_sql_file("clone_submissions.sql")
        for fiscal_period in helper.get_clone_periods(self.fiscal_quarter):
            helper.run_sqls(
                helper.split_sql(
                    sql.format(
                        submission_id_shift=helper.shift_id(self.fiscal_year, fiscal_period),
                        reporting_period_start=self.period_start,
                        reporting_period_end=self.period_end,
                        reporting_fiscal_period=fiscal_period,
                        filter_fiscal_year=self.fiscal_year,
                        filter_fiscal_period=self.fiscal_period,
                        adjustment_ratio=self.period_ratios[fiscal_period],
                    )
                )
            )

    def update_base_submissions(self):
        sql = helper.read_sql_file("update_submissions.sql")
        helper.run_sqls(
            helper.split_sql(
                sql.format(
                    reporting_period_start=self.period_start,
                    reporting_period_end=self.period_end,
                    filter_fiscal_year=self.fiscal_year,
                    filter_fiscal_period=self.fiscal_period,
                    adjustment_ratio=self.period_ratios[self.fiscal_period],
                )
            )
        )
