import logging
import pandas

from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import transaction
from functools import partial
from psycopg2.sql import Literal, SQL
from usaspending_api.common.helpers.sql_helpers import execute_fetchall, execute_update_sql
from usaspending_api.common.helpers.timing_helpers import Timer


logger = logging.getLogger("console")


# All of our timers log to logger.info and logger.error.
Timer = partial(Timer, success_logger=logger.info, failure_logger=logger.error)


MAX_DIFF = 10


class Command(BaseCommand):

    help = (
        "Load CGAC codes/agencies from {} into USAspending.  "
        "Every load is a full restock since this is a tiny table.".format(settings.AGENCY_FILE_URL)
    )

    def add_arguments(self, parser):

        parser.add_argument(
            "--force",
            action="store_true",
            help=(
                "By default, load_cgacs performs a sanity check on the "
                "incoming data to prevent database corruption in the face of "
                "source data corruption or dirty reads.  If it detects more "
                "than {} CGAC differences, it will INTENTIONALLY fail.  To "
                "force it to run anyhow, supply this switch.".format(MAX_DIFF)
            ),
        )

    def handle(self, *args, **options):

        # Pick out just the last bit of the module name (which should match the
        # file name minus the extension).
        module_name = __name__.split(".")[-1]
        with Timer(module_name):

            with Timer("Retrieve new CGACs"):
                new_cgacs = self._get_new_cgacs()
                logger.info("{:,} CGACs read from '{}'.".format(len(new_cgacs), settings.AGENCY_FILE_URL))

            if len(new_cgacs) < 1:
                raise RuntimeError("No CGAC value were found in '{}'.".format(settings.AGENCY_FILE_URL))

            if options["force"]:

                logger.info("--force switch provided.  Performing a full load.")

            else:

                with Timer("Retrieve old CGACs"):
                    old_cgacs = self._get_old_cgacs()
                    logger.info("{:,} CGACs read from cgac table".format(len(old_cgacs)))

                if not old_cgacs:
                    logger.info("No CGACs in cgac table.  Performing a full load.")
                else:
                    diff = self._diff_cgacs(new_cgacs, old_cgacs)
                    if diff > 0:
                        logger.info("Found {:,} differences.  Performing a full load.".format(diff))
                    else:
                        logger.info("No differences found.  No load will be performed.")
                        return

            with Timer("Load new CGACs"):
                self._load_cgacs(new_cgacs)
                logger.info("{:,} CGACs loaded.".format(len(new_cgacs)))

    @staticmethod
    def _get_new_cgacs():

        df = pandas.read_csv(
            settings.AGENCY_FILE_URL,
            header=0,
            usecols=["CGAC AGENCY CODE", "AGENCY NAME", "AGENCY ABBREVIATION"],
            dtype={"CGAC AGENCY CODE": str, "AGENCY NAME": str, "AGENCY ABBREVIATION": str},
            engine="python",
            skipinitialspace=True,
            keep_default_na=False,
        )
        df = df.drop_duplicates()
        if not pandas.Series(df["CGAC AGENCY CODE"]).is_unique:
            raise ValueError(
                "Found CGAC AGENCY CODE values with more than one AGENCY NAME/AGENCY "
                "ABBREVIATION in the source file '{}'.".format(settings.AGENCY_FILE_URL)
            )
        df = df.sort_values(["CGAC AGENCY CODE"])
        df = df.replace({"": None})

        return [tuple(v) for v in df.values]

    @staticmethod
    def _get_old_cgacs():

        return execute_fetchall("select cgac_code, agency_name, agency_abbreviation from cgac")

    @staticmethod
    def _diff_cgacs(new_cgacs, old_cgacs):

        # The way we're calculating difference is rather trivial.  It's the max
        # list length minus the number of similarities.
        diff = max(len(new_cgacs), len(old_cgacs)) - len(set(new_cgacs) & set(old_cgacs))

        if diff > MAX_DIFF:
            raise RuntimeError(
                "There are {:,} differences between '{}' and the cgac table.   "
                "As a failsafe, we throw an exception if there are more than {:,} "
                "differences.  If this is expected, rerun this script with the "
                "--force switch to accept these changes.".format(diff, settings.AGENCY_FILE_URL, MAX_DIFF)
            )

        return diff

    @staticmethod
    def _load_cgacs(new_cgacs):

        sql = SQL("insert into cgac (cgac_code, agency_name, agency_abbreviation) values {}").format(
            SQL(", ").join([Literal(cgac) for cgac in new_cgacs])
        )

        with transaction.atomic():
            execute_update_sql("delete from cgac")
            execute_update_sql(sql)

        # Because this is a tiny table and it takes less than a second to
        # vacuum, this should not be a problem.
        execute_update_sql("vacuum full analyze cgac")
