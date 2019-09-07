import asyncio

from pathlib import Path
from django.core.management.base import BaseCommand

from usaspending_api.common.data_connectors.async_sql_query import async_run_select
from usaspending_api.common.matview_manager import MATERIALIZED_VIEWS


class Command(BaseCommand):

    help = "Create, Run, Verify Materialized View SQL"

    def faux_init(self, args):
        self.matviews = MATERIALIZED_VIEWS
        if args["only"]:
            self.matviews = MATERIALIZED_VIEWS[args["only"]]
        self.dry_run = args["dry-run"]
        self.no_database = args["no-database"]

    def add_arguments(self, parser):
        parser.add_argument("--only", choices=list(MATERIALIZED_VIEWS.keys()))
        parser.add_argument("--dry-run", action="store_true", help="Test all the steps, rollback the transactions.")
        parser.add_argument(
            "--parallel",
            action="store_true",
            help="As possible, run parallelized components of the materialized view creation.",
        )
        parser.add_argument(
            "--no-database", action="store_true", help="Create SQL files from JSON, don't run any database operations."
        )
        parser.add_argument(
            "--temp-dir", type=Path, help="Choose a non-default directory to store materialized view SQL files."
        )

    def handle(self, *args, **options):
        """Overloaded Command Entrypoint"""

    def dry_run(self):
        """Method for testing Materialized View JSON and SQL"""

    def generate_matview_sql(self):
        """Convert JSON definition files to SQL"""

    def create_views(self):
        """Create any views built on any materialized views"""

    def matview_runner(self):
        """Manager controlling the SQL run"""

        loop = asyncio.new_event_loop()
        results = {}
        for k, v in self.matviews.items():
            sql = ""

            # Django refuses to provide a viable option to exclude "GROUP BY ..." so it is stripped before running the SQL
            remove_groupby_string_index = sql.find("GROUP BY")
            results[k] = asyncio.ensure_future(async_run_select(sql[:remove_groupby_string_index]), loop=loop)

        all_statements = asyncio.gather(*[value for value in results.values()])
        loop.run_until_complete(all_statements)
        loop.close()
