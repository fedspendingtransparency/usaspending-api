"""
Jira Ticket Number(s): DEV-5343

    This management command deterministically generates DEF code File B and C record in the FYP
    provided.  DEF codes are new to USAspending and are not yet being provided by Broker.

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
from usaspending_api.references.models import DisasterEmergencyFundCode


class Command(DEV5343Mixin, BaseCommand):
    help = "Generate File B and C DEF code records for the fiscal year and period provided."

    def add_arguments(self, parser):

        super().add_arguments(parser)

        parser.add_argument(
            "fiscal period", type=self.validate_fiscal_period, help='Fiscal period to be "enhanced".',
        )

    def handle(self, *args, **options):
        self.set_state(options)
        self.perform_validations()

        with ScriptTimer(f"Generate DEF code File B and C records for FY{self.fiscal_year}P{self.fiscal_period}"):

            with transaction.atomic():
                self.record_base_submission_ids()
                self.clone_for_defc()

                t = ScriptTimer("Commit transaction")
            t.log_success_message()

            if self.vacuum:
                self.vacuum_tables()

    def perform_validations(self):
        super().perform_validations()

        if not self.period_has_submissions():
            raise RuntimeError(
                f"Well congratulations.  You've managed to choose a fiscal period with no "
                f"submissions.  Give 'select reporting_fiscal_year, reporting_fiscal_period, "
                f"count(*) from submission_attributes where group by reporting_fiscal_year, "
                f"reporting_fiscal_period order by reporting_fiscal_year, "
                f"reporting_fiscal_period;' a whirl and try again."
            )

        if DisasterEmergencyFundCode.objects.count() == 0:
            raise RuntimeError(
                f"The {DisasterEmergencyFundCode._meta.db_table} table is empty.  This is a new "
                f"table and is required for this script.  The load_disaster_emergency_fund_codes "
                f"loader probably just hasn't been run against this database yet.  Why don't you "
                f"scurry along and deal with that right quick.  Thank you!"
            )
