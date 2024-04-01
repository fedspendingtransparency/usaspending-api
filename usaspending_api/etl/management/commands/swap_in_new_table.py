from typing import List, OrderedDict
from django.core.management import BaseCommand
from usaspending_api.broker.lookups import LoadTrackerLoadTypeEnum, LoadTrackerStepEnum
from usaspending_api.common.load_tracker import LoadTracker
from usaspending_api.etl.swap_in_new_table_strategies import (
    FullLoadSwapInTableStrategy,
    IncrementalLoadSwapInTableStrategy,
)

LOAD_STEP = "swap_in_new_table-{table}"


class Command(BaseCommand):
    help = """
    This command is used to load data to our live tables in Postgres. We support two different types of loads
    currently; incremental and full load. Full load is the default load type of this command. If you'd like
    to use the incremental load, use the --incremental option.
    """

    # Values are set in the beginning of "handle()"
    curr_schema_name: str
    curr_table_name: str
    dep_views: List[OrderedDict]

    # Query values are populated as they are run during validation and saved for re-use
    query_result_lookup = {
        "temp_table_constraints": [],
        "curr_table_constraints": [],
        "temp_table_indexes": [],
        "curr_table_indexes": [],
    }

    def add_arguments(self, parser):
        parser.add_argument(
            "--table",
            type=str,
            help="The active Postgres table to swap with another containing the same name with '_temp' appended",
        )
        parser.add_argument(
            "--keep-old-data",
            action="store_true",
            default=False,
            help="Indicates whether or not to drop old table at the end of the command",
        )
        parser.add_argument(
            "--allow-foreign-key",
            action="store_true",
            default=False,
            help="A guard is enabled / disabled depending on the value of this flag. When 'FALSE' Foreign Keys are not"
            " allowed and both the active and new table are searched for any Foreign Keys before proceeding."
            " It is advised to not allow Foreign Key constraints since they can cause deadlock.",
        )
        parser.add_argument(
            "--source-suffix",
            type=str,
            required=False,
            nargs="?",
            const="",  # value if flag provided but no arg values given
            default="temp",
            help="The assumed suffix on the name of the source table to be swapped in, and all its objects (like "
            "indexes, constraints).",
        )
        parser.add_argument(
            "--dest-suffix",
            type=str,
            required=False,
            nargs="?",
            const="",  # value if flag provided but no arg values given
            default="",
            help="The assumed suffix on the name of the table to be replaced, and all its objects (like "
            "indexes, constraints).",
        )
        parser.add_argument(
            "--undo",
            action="store_true",
            default=False,
            help="Reverse the most recent swap (that it can find) on the given --table. Only works if some old data "
            "was saved using --keep-old-data when the swap or a prior swap was run. If you know specifically "
            "which old table you want to swap, just run a regular command instead, supplying --source-suffix and "
            "possibly --dest-suffix",
        )
        parser.add_argument(
            "--incremental",
            action="store_true",
            default=False,
            help="Instead of writing the full table to live tables in Postgres, use the incremental tables created "
            "earlier in the pipeline to load the live tables.",
        )

    def handle(self, *args, **options):
        incremental_load = options["incremental"]
        table = options["table"]

        load_step = LoadTrackerStepEnum(LOAD_STEP.format(table=table))
        if incremental_load:
            load_type = LoadTrackerLoadTypeEnum.INCREMENTAL_LOAD
            load_strategy = IncrementalLoadSwapInTableStrategy(django_command=self, **options)
        else:
            load_type = LoadTrackerLoadTypeEnum.FULL_LOAD
            load_strategy = FullLoadSwapInTableStrategy(django_command=self, **options)

        # Instantiate a load tracker for the current job
        load_tracker = LoadTracker(load_type, load_step)
        load_strategy.load_tracker = load_tracker
        load_tracker.start()
        load_strategy.swap_in_new_table()
        load_tracker.end()
