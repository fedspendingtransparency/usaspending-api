import logging

from django.core.management.base import BaseCommand
from django.db import transaction
from pathlib import Path
from usaspending_api.common.etl.postgres import mixins
from usaspending_api.common.helpers.timing_helpers import ScriptTimer as Timer


logger = logging.getLogger("script")


class Command(mixins.ETLMixin, BaseCommand):

    help = "Update recipient_lookup in USAspending."

    full_reload = False

    etl_logger_function = logger.info
    etl_timer = Timer
    etl_dml_sql_directory = Path(__file__).resolve().parent.parent / "sql" / "recipient_lookup"

    def handle(self, *args, **options):
        with Timer("SQL Files"):
            try:
                with transaction.atomic():
                    self._perform_load()
                    t = Timer("Commit transaction")
                    t.log_starting_message()
                t.log_success_message()
            except Exception:
                logger.error("ALL CHANGES ROLLED BACK DUE TO EXCEPTION")
                raise

    def _perform_load(self):
        """Loop through each SQL file in order (000... to 1XX...) and execute"""
        for sqlfile in sorted(list(self.etl_dml_sql_directory.glob("*.sql"))):
            self._execute_etl_dml_sql_directory_file(sqlfile)
