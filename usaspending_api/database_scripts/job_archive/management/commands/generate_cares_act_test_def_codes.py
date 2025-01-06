"""
Jira Ticket Number(s): DEV-5343

    This management command deterministically generates DEF code File B and C records for the
    fiscal period provided.  DEF codes are new to USAspending and are not yet being provided by
    Broker.

Expected CLI:

    $ ./manage.py generate_cares_act_test_def_codes 2020 6

    where 2020 is the fiscal year and 6 is the fiscal period

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
    help = "Deterministically generate File B and C DEF code records for the fiscal year and period provided."

    fiscal_year = None
    fiscal_period = None
    allow_rds = False
    vacuum = False

    # These are just carefully selected values used to clone and "randomize" File B and C records.
    # (
    #   DEF code,
    #   ratio of $ to be applied to this clone,
    #   denominator of modulus (13 means one in 13 records will be cloned - ish)
    # )
    clone_factors = [("I", 0.2, 13), ("F", 0.1, 11), ("L", 0.3, 10), ("M", 0.25, 8), ("N", 0.4, 7), ("O", 0.15, 5)]

    def add_arguments(self, parser):
        helper.add_argument_fiscal_year(parser)
        helper.add_argument_fiscal_period(parser)
        helper.add_argument_rds(parser)
        helper.add_argument_vacuum(parser)
        helper.add_argument_warning_epilog(parser)

    def handle(self, *args, **options):
        self.set_state(options)
        self.perform_validations()

        with ScriptTimer(f"Generate DEF code File B and C records for FY{self.fiscal_year}P{self.fiscal_period}"):

            with transaction.atomic():
                helper.record_base_submission_ids()
                self.clone_for_defc()

                t = ScriptTimer("Commit transaction")
            t.log_success_message()

            if self.vacuum:
                helper.vacuum_tables()

    def set_state(self, options):
        self.fiscal_year = options["fiscal year"]
        self.fiscal_period = options["fiscal period"]
        self.allow_rds = options["yes_i_know_its_rds"]
        self.vacuum = options["vacuum"]

    def perform_validations(self):
        helper.validate_not_rds(self.allow_rds)
        helper.validate_period_has_submissions(self.fiscal_year, self.fiscal_period)
        helper.validate_disaster_emergency_fund_code_table_has_data()

    def clone_for_defc(self):
        sql = helper.read_sql_file("clone_for_defc.sql")
        for p in self.clone_factors:
            helper.run_sqls(
                helper.split_sql(
                    sql.format(
                        disaster_emergency_fund_code=p[0],
                        filter_fiscal_year=self.fiscal_year,
                        filter_fiscal_period=self.fiscal_period,
                        adjustment_ratio=p[1],
                        divisor=p[2],
                    )
                )
            )
