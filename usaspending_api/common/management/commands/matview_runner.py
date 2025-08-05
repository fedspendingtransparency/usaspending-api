import asyncio
import logging
import psycopg2
import subprocess

from django.core.management import call_command
from django.core.management.base import BaseCommand
from pathlib import Path

from usaspending_api.common.data_connectors.async_sql_query import async_run_creates
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer
from usaspending_api.common.matview_manager import (
    CHUNKED_MATERIALIZED_VIEWS,
    DEFAULT_MATIVEW_DIR,
    DEFAULT_CHUNKED_MATIVEW_DIR,
    DEPENDENCY_FILEPATH,
    DROP_OLD_MATVIEWS,
    MATERIALIZED_VIEWS,
    MATVIEW_GENERATOR_FILE,
    CHUNKED_MATVIEW_GENERATOR_FILE,
)
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string

logger = logging.getLogger("script")


class Command(BaseCommand):
    help = "Create, Run, Verify Materialized View SQL"

    def faux_init(self, args):
        self.matviews = MATERIALIZED_VIEWS
        self.chunked_matviews = CHUNKED_MATERIALIZED_VIEWS
        if args["only"]:
            if args["only"] == "none":
                self.matviews = {}
            else:
                self.matviews = {args["only"]: MATERIALIZED_VIEWS[args["only"]]}
        self.matview_dir = args["temp_dir"]
        self.matview_chunked_dir = args["temp_chunked_dir"]
        self.no_cleanup = args["leave_sql"]
        self.remove_matviews = not args["leave_old"]
        self.run_dependencies = args["dependencies"]
        self.chunk_count = args["chunk_count"]
        self.include_chunked_matviews = args["include_chunked_matviews"]
        self.index_concurrency = args["index_concurrency"]

    def add_arguments(self, parser):
        parser.add_argument(
            "--only",
            choices=list(MATERIALIZED_VIEWS.keys()) + ["none"],
            help="If matviews are listed with this option, only those matviews will be run. 'none' will result in no matviews being run",
        )
        parser.add_argument(
            "--leave-sql",
            action="store_true",
            help="Leave the generated SQL files instead of cleaning them after script completion.",
        )
        parser.add_argument(
            "--leave-old",
            action="store_true",
            help="Leave the old materialized views instead of dropping them from the DB.",
        )
        parser.add_argument(
            "--temp-dir",
            type=Path,
            help="Choose a non-default directory to store materialized view SQL files.",
            default=DEFAULT_MATIVEW_DIR,
        )
        parser.add_argument(
            "--temp-chunked-dir",
            type=Path,
            help="Choose a non-default directory to store materialized view SQL files.",
            default=DEFAULT_CHUNKED_MATIVEW_DIR,
        )
        parser.add_argument(
            "--dependencies", action="store_true", help="Run the SQL dependencies before the materialized view SQL."
        )
        parser.add_argument(
            "--chunk-count", default=10, help="Number of chunks to split chunked matviews into", type=int
        )
        parser.add_argument(
            "--include-chunked-matviews",
            action="store_true",
            help="Chunked Transaction Search matviews will be refreshed and inserted into table",
        )
        parser.add_argument("--index-concurrency", default=20, help="Number of indexes to be created at once", type=int)

    def handle(self, *args, **options):
        """Overloaded Command Entrypoint"""
        with Timer(__name__):
            self.faux_init(options)
            self.generate_matview_sql()
            if self.run_dependencies:
                create_dependencies()
            self.create_views()
            if not self.no_cleanup:
                self.cleanup()

    @staticmethod
    def clean_or_create_dir(dir_path):
        if dir_path.exists():
            logger.warning(f"Clearing dir {dir_path}")
            recursive_delete(dir_path)
        dir_path.mkdir()

    def generate_matview_sql(self):
        """Convert JSON definition files to SQL"""
        self.clean_or_create_dir(self.matview_dir)
        self.clean_or_create_dir(self.matview_chunked_dir)

        # IF using this for operations, DO NOT LEAVE hardcoded `python3` in the command
        # Create main list of Matview SQL files
        exec_str = f"python3 {MATVIEW_GENERATOR_FILE} --quiet --dest={self.matview_dir}/ --batch_indexes=3"
        subprocess.call(exec_str, shell=True)

        if self.include_chunked_matviews:
            # Create SQL files for Chunked Universal Transaction Matviews
            for matview, config in self.chunked_matviews.items():
                exec_str = (
                    f"python3 {CHUNKED_MATVIEW_GENERATOR_FILE} --quiet"
                    f" --file {config['json_filepath']}"
                    f" --chunk-count {self.chunk_count}"
                    f" --dest={self.matview_chunked_dir}"
                )
                subprocess.call(exec_str, shell=True)

    def cleanup(self):
        """Cleanup files after run"""
        recursive_delete(self.matview_dir)
        recursive_delete(self.matview_chunked_dir)

    def create_views(self):
        loop = asyncio.new_event_loop()
        tasks = []

        # Create Matviews
        for matview, config in self.matviews.items():
            logger.info(f"Creating Future for matview {matview}")
            sql = (self.matview_dir / config["sql_filename"]).read_text()
            tasks.append(asyncio.ensure_future(async_run_creates(sql, wrapper=Timer(matview)), loop=loop))

        # Create Chunked Matviews
        if self.include_chunked_matviews:
            for matview, config in self.chunked_matviews.items():
                for current_chunk in range(self.chunk_count):
                    chunked_matview = f"{matview}_{current_chunk}"
                    logger.info(f"Creating Future for chunked matview {chunked_matview}")
                    sql = (self.matview_chunked_dir / f"{chunked_matview}.sql").read_text()
                    tasks.append(
                        asyncio.ensure_future(async_run_creates(sql, wrapper=Timer(chunked_matview)), loop=loop)
                    )

        if len(tasks) > 0:
            loop.run_until_complete(asyncio.gather(*tasks))

        loop.close()

        if "transaction_search" in self.chunked_matviews and self.include_chunked_matviews:
            logger.info("Inserting data from transaction_search chunks into transaction_search table.")
            call_command(
                "combine_transaction_search_chunks",
                chunk_count=self.chunk_count,
                index_concurrency=self.index_concurrency,
                matview_dir=self.matview_chunked_dir,
            )

        if self.remove_matviews:
            run_sql(DROP_OLD_MATVIEWS.read_text(), "Drop Old Materialized Views")


def create_dependencies():
    run_sql(DEPENDENCY_FILEPATH.read_text(), "dependencies")


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
