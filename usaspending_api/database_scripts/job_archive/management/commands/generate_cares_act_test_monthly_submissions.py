"""
Jira Ticket Number(s): DEV-5343

    This management command deterministically converts about 2/3 of submissions in the FYQ provided
    into monthly submissions.  Monthly submissions are new to USAspending and are not yet being
    provided by Broker.

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
        - job_archive/management/commands/generate_cares_act_test_data_helpers.py
        - job_archive/management/commands/generate_cares_act_test_def_codes.py
        - job_archive/management/commands/generate_cares_act_test_monthly_submissions.py
        - job_archive/management/commands/generate_cares_act_test_data_sqls

"""
from django.core.management.base import BaseCommand
from django.db import transaction
from usaspending_api.common.helpers.timing_helpers import ScriptTimer
from usaspending_api.database_scripts.job_archive.management.commands.generate_cares_act_test_data_helpers import (
    DEV5343Mixin,
)


class Command(DEV5343Mixin, BaseCommand):
    help = (
        "Deterministically convert about 2/3 of submissions in the fiscal year and quarter "
        "provided into monthly submissions."
    )

    fiscal_period = None
    clone_periods = None
    period_ratios = None
    period_starts = None
    period_ends = None

    def add_arguments(self, parser):

        super().add_arguments(parser)

        parser.add_argument(
            "fiscal quarter", type=self.validate_fiscal_quarter, help='Fiscal quarter to be "enhanced".',
        )

    def handle(self, *args, **options):
        self.set_state(options)
        self.perform_validations()

        with ScriptTimer(f"Convert FY{self.fiscal_year}Q{self.fiscal_quarter} submissions into monthly submissions"):

            with transaction.atomic():
                self.record_base_submission_ids()
                self.clone_submissions()
                self.update_base_submissions()

                t = ScriptTimer("Commit transaction")
            t.log_success_message()

            if self.vacuum:
                self.vacuum_tables()

    def perform_validations(self):
        super().perform_validations()

        if not self.quarter_has_submissions():
            raise RuntimeError(
                f"Well congratulations.  You've managed to choose a fiscal quarter with no "
                f"submissions.  Give 'select reporting_fiscal_year, reporting_fiscal_quarter, "
                f"count(*) from submission_attributes group by reporting_fiscal_year, "
                f"reporting_fiscal_quarter order by reporting_fiscal_year,"
                f"reporting_fiscal_quarter;' a whirl and try again."
            )
