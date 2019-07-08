import logging
import pandas

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
        "Load CGAC codes/agencies from the indicated file into USAspending.  "
        "Every load is a full restock since this is a tiny table."
    )

    def add_arguments(self, parser):

        parser.add_argument(
            "--agency-file-uri",
            required=True,
            help=(
                "URI for the file to be loaded.  As of this writing, only files " 
                "obtained via https have been tested."
            ),
        )

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
        """
        Sooooooo, return 0 if there were changes... 3 if
        there were no changes... and anything other than
        0 or 3 if there was an issue.  ¯\_(ツ)_/¯
        """
        try:

            if self._handle_load(**options):
                exit(0)  # Changes
            exit(3)  # No changes

        except Exception as e:

            logger.exception("Exception encountered during load.  Details should follow.")
            exit(1)  # No bueno

    def _handle_load(self, **options):

        # Pick out just the last bit of the module name (which should match the
        # file name minus the extension).
        module_name = __name__.split(".")[-1]
        with Timer(module_name):

            agency_file_uri = options["agency_file_uri"]

            with Timer("Retrieve new CGACs"):
                new_cgacs = self._get_new_cgacs(agency_file_uri)
                logger.info("{:,} CGACs read from '{}'.".format(len(new_cgacs), agency_file_uri))

            if len(new_cgacs) < 1:
                raise RuntimeError("No CGAC value were found in '{}'.".format(agency_file_uri))

            if options["force"]:

                logger.info("--force switch provided.  Performing a full load.")

            else:

                with Timer("Retrieve old CGACs"):
                    old_cgacs = self._get_old_cgacs()
                    logger.info("{:,} CGACs read from cgac table".format(len(old_cgacs)))

                if not old_cgacs:
                    logger.info("No CGACs in cgac table.  Performing a full load.")
                else:
                    diff = self._diff_cgacs(new_cgacs, old_cgacs, agency_file_uri)
                    if diff > 0:
                        logger.info("Found {:,} differences.  Performing a full load.".format(diff))
                    else:
                        logger.info("No differences found.  No load will be performed.")
                        return 0

            with Timer("Load new CGACs"):
                self._load_cgacs(new_cgacs)
                logger.info("{:,} CGACs loaded.".format(len(new_cgacs)))
                return len(new_cgacs)

    @staticmethod
    def _get_new_cgacs(agency_file_uri):

        df = pandas.read_csv(
            agency_file_uri,
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
                "ABBREVIATION in the source file '{}'.".format(agency_file_uri)
            )
        df = df.sort_values(["CGAC AGENCY CODE"])
        df = df.replace({"": None})

        return [tuple(v) for v in df.values]

    @staticmethod
    def _get_old_cgacs():

        return execute_fetchall("select cgac_code, agency_name, agency_abbreviation from cgac")

    @staticmethod
    def _diff_cgacs(new_cgacs, old_cgacs, agency_file_uri):

        # The way we're calculating difference is rather trivial.  It's the max
        # list length minus the number of similarities.
        diff = max(len(new_cgacs), len(old_cgacs)) - len(set(new_cgacs) & set(old_cgacs))

        if diff > MAX_DIFF:
            raise RuntimeError(
                "There are {:,} differences between '{}' and the cgac table.   "
                "As a failsafe, we throw an exception if there are more than {:,} "
                "differences.  If this is expected, rerun this script with the "
                "--force switch to accept these changes.".format(diff, agency_file_uri, MAX_DIFF)
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
