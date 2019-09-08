import asyncio
import logging
import psycopg2
import subprocess

from django.core.management.base import BaseCommand
from pathlib import Path

from usaspending_api.common.data_connectors.async_sql_query import async_run_creates
from usaspending_api.common.helpers.timing_helpers import Timer
from usaspending_api.common.matview_manager import (
    DEFAULT_MATIVEW_DIR,
    DROP_OLD_MATVIEWS,
    MATERIALIZED_VIEWS,
    MATVIEW_GENERATOR_FILE,
    OVERLAY_VIEWS,
)
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string

logger = logging.getLogger("console")


class Command(BaseCommand):

    help = "Create, Run, Verify Materialized View SQL"

    def faux_init(self, args):
        self.matviews = MATERIALIZED_VIEWS
        if args["only"]:
            self.matviews = {args["only"]: MATERIALIZED_VIEWS[args["only"]]}
        self.matview_dir = args["temp_dir"]
        self.no_cleanup = args["leave_sql"]

    def add_arguments(self, parser):
        parser.add_argument("--only", choices=list(MATERIALIZED_VIEWS.keys()))
        parser.add_argument(
            "--leave-sql",
            action="store_true",
            help="Leave the generated SQL files instead of cleaning them after script completion.",
        )
        parser.add_argument(
            "--temp-dir",
            type=Path,
            help="Choose a non-default directory to store materialized view SQL files.",
            default=DEFAULT_MATIVEW_DIR,
        )

    def handle(self, *args, **options):
        """Overloaded Command Entrypoint"""
        with Timer(__name__):
            self.faux_init(options)
            self.generate_matview_sql()
            self.create_views()
            if not self.no_cleanup:
                self.cleanup()

    def generate_matview_sql(self):
        """Convert JSON definition files to SQL"""
        if self.matview_dir.exists():
            logger.warn("Clearing dir {}".format(self.matview_dir))
            recursive_delete(self.matview_dir)
        self.matview_dir.mkdir()

        # NOTE TO SELF!!!!!!!
        # DO NOT LEAVE hardcoded `python` in the command!!!!!!!
        exec_str = "python {} --quiet --dest={}/ --batch_indexes=3".format(MATVIEW_GENERATOR_FILE, self.matview_dir)
        subprocess.call(exec_str, shell=True)

    def cleanup(self):
        """Cleanup files after run"""
        recursive_delete(self.matview_dir)

    def create_views(self):
        loop = asyncio.new_event_loop()
        tasks = []
        for matview, config in self.matviews.items():
            logger.info("Creating Future for {}".format(matview))
            sql = open(str(self.matview_dir / config["sql_filename"]), "r").read()
            tasks.append(asyncio.ensure_future(async_run_creates(sql, wrapper=Timer(matview)), loop=loop))

        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()

        for view in OVERLAY_VIEWS:
            sql = open(str(view), "r").read()
            run_sql(sql, "Creating Views")

        drop_sql = open(str(DROP_OLD_MATVIEWS), "r").read()
        run_sql(drop_sql, "Drop Old Materialized Views")


def run_sql(sql, name):
    with psycopg2.connect(dsn=get_database_dsn_string()) as connection:
        with connection.cursor() as cursor:
            with Timer(name):
                cursor.execute(sql)


def recursive_delete(path):
    """Remove file or directory (clear entire dir structure)"""
    path = Path(str(path)).resolve()  # ensure it is an absolute path
    if not path.exists() and len(str(path)) < 6:  # don't delete the entire dir
        return
    if path.is_dir():
        for f in path.glob("*"):
            recursive_delete(f)
        path.rmdir()
    elif not path.is_dir():
        path.unlink()
    else:
        logger.info("Nothing to delete")
