import logging
import re

from collections import namedtuple
from django.core.management.base import BaseCommand
from django.db import transaction
from psycopg2.extras import execute_values
from psycopg2.sql import SQL
from usaspending_api.common.csv_helpers import read_csv_file_as_list_of_dictionaries
from usaspending_api.common.etl.postgres import ETLTable, ETLTemporaryTable
from usaspending_api.common.etl.postgres import mixins
from usaspending_api.common.etl.postgres.operations import insert_missing_rows, update_changed_rows
from usaspending_api.common.helpers.sql_helpers import get_connection
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer
from usaspending_api.references.models import ObjectClass


OBJECT_CLASS_PATTERN = re.compile("^[0-9]{3}0?$")

CREATE_TEMP_TABLE = """
    drop table if exists temp_load_object_classes;

    create temporary table temp_load_object_classes (
        row_number int,
        major_object_class text,
        major_object_class_name text,
        object_class text,
        object_class_name text,
        direct_reimbursable text,
        direct_reimbursable_name text
    );
"""

logger = logging.getLogger("script")

RawObjectClass = namedtuple("RawObjectClass", ["row_number", "object_class", "object_class_name"])

FullObjectClass = namedtuple(
    "FullObjectClass",
    [
        "row_number",
        "major_object_class",
        "major_object_class_name",
        "object_class",
        "object_class_name",
        "direct_reimbursable",
        "direct_reimbursable_name",
    ],
)


class Command(mixins.ETLMixin, BaseCommand):

    help = "Load object class CSV file.  If anything fails, nothing gets saved.  DOES NOT DELETE RECORDS."
    object_class_file = None
    etl_logger_function = logger.info

    def add_arguments(self, parser):

        parser.add_argument(
            "--object_class_file",
            metavar="FILE",
            help="Path or URI of the raw object class CSV file to be loaded.",
            default="https://files.usaspending.gov/reference_data/object_class.csv",
        )

    def handle(self, *args, **options):

        self.object_class_file = options["object_class_file"]

        logger.info("OBJECT CLASS FILE: {}".format(self.object_class_file))

        with Timer("Load object classes"):

            try:
                with transaction.atomic():
                    self._perform_load()
                    t = Timer("Commit transaction")
                    t.log_starting_message()
                t.log_success_message()
            except Exception:
                logger.error("ALL CHANGES ROLLED BACK DUE TO EXCEPTION")
                raise

            try:
                self._vacuum_tables()
            except Exception:
                logger.error("CHANGES WERE SUCCESSFULLY COMMITTED EVEN THOUGH VACUUMS FAILED")
                raise

    @staticmethod
    def _prep(text):
        """
        A semi-common problem with CSV files that have been edited by third party tools is the
        introduction of leading and/or trailing spaces.  Strip them.
        """
        if text and type(text) is str:
            return text.strip()
        return text

    def _read_raw_object_classes_csv(self):

        object_classes = read_csv_file_as_list_of_dictionaries(self.object_class_file)
        if len(object_classes) < 1:
            raise RuntimeError("Object class file '{}' appears to be empty".format(self.object_class_file))

        # only importing the 3-digit versions of the codes to prevent unnecessary dups
        self.raw_object_classes = [
            RawObjectClass(
                row_number=row_number,
                object_class=self._prep(object_class["MAX OC Code"]),
                object_class_name=self._prep(object_class["MAX Object Class name"]),
            )
            for row_number, object_class in enumerate(object_classes, start=1)
            if self._prep(object_class["MAX OC Code"])
        ]

        return len(self.raw_object_classes)

    @staticmethod
    def _validate_raw_object_class(raw_object_class):

        messages = []

        if not OBJECT_CLASS_PATTERN.fullmatch(raw_object_class.object_class):
            messages.append(
                f"Invalid object class code '{raw_object_class.object_class}' in row "
                f"{raw_object_class.row_number:,}.  Object class codes must be three or four numeric "
                f"digits and, if four digits, must end with '0'."
            )

        if not raw_object_class.object_class_name:
            messages.append(f"Object class name is required in row {raw_object_class.row_number:,}.")

        return messages

    def _validate_raw_object_classes(self):

        messages = []

        for raw_object_class in self.raw_object_classes:
            messages += self._validate_raw_object_class(raw_object_class)

        if messages:
            for message in messages:
                logger.error(message)
            raise RuntimeError(
                f"{len(messages):,} problem(s) have been found with the raw object class file.  See log for details."
            )

    def _keep_only_3_digit_object_classes(self):
        """While the file and users can provide both versions, this loader only needs the 3-digits when processing"""
        self.raw_object_classes = [
            raw_object_class for raw_object_class in self.raw_object_classes if len(raw_object_class.object_class) == 3
        ]

    def _add_unknown_object_classes(self):
        """These are not officially sanctioned object classes but we use them on the website."""

        unknown = ObjectClass.MAJOR_OBJECT_CLASS.UNKNOWN_NAME
        self.raw_object_classes = [
            RawObjectClass(row_number=None, object_class="000", object_class_name=unknown),
        ] + self.raw_object_classes

    def _derive_remaining_fields(self):
        """The remaining object class data are derived."""

        # Alias to cut down on line lengths below.
        ocdr = ObjectClass.DIRECT_REIMBURSABLE

        def derive_remaining_fields(raw_object_class: RawObjectClass, direct_reimbursable: str) -> FullObjectClass:
            object_class = raw_object_class.object_class
            major_object_class = object_class[0] + "0"
            object_class = f"{object_class[:2]}.{object_class[2:]}"

            return FullObjectClass(
                row_number=raw_object_class.row_number,
                major_object_class=major_object_class,
                major_object_class_name=ObjectClass.MAJOR_OBJECT_CLASS.LOOKUP[major_object_class],
                object_class=object_class,
                object_class_name=raw_object_class.object_class_name,
                direct_reimbursable=direct_reimbursable,
                direct_reimbursable_name=ocdr.LOOKUP[direct_reimbursable],
            )

        self.full_object_classes = []
        for roc in self.raw_object_classes:
            # for each object class, we're including the three possible versions
            for dr in [ocdr.UNKNOWN, ocdr.DIRECT, ocdr.REIMBURSABLE]:
                self.full_object_classes.append(derive_remaining_fields(roc, dr))

    def _import_object_classes(self):

        with get_connection(read_only=False).cursor() as cursor:
            execute_values(
                cursor.cursor,
                """
                    insert into temp_load_object_classes (
                        row_number,
                        major_object_class,
                        major_object_class_name,
                        object_class,
                        object_class_name,
                        direct_reimbursable,
                        direct_reimbursable_name
                    ) values %s
                """,
                self.full_object_classes,
                page_size=len(self.full_object_classes),
            )
            return cursor.rowcount

    def _perform_load(self):

        overrides = {
            "insert_overrides": {"create_date": SQL("now()"), "update_date": SQL("now()")},
            "update_overrides": {"update_date": SQL("now()")},
            "key_overrides": ["object_class", "direct_reimbursable"],
        }

        object_class_table = ETLTable("object_class", **overrides)
        temp_object_class_table = ETLTemporaryTable("temp_load_object_classes")

        self._execute_dml_sql(CREATE_TEMP_TABLE, "Create object_class temp table")
        self._execute_function_and_log(self._read_raw_object_classes_csv, "Read raw object class csv")
        self._execute_function(self._validate_raw_object_classes, "Validate raw object classes")
        self._execute_function(self._keep_only_3_digit_object_classes, "Keep 3-digit object classes")
        self._execute_function(self._add_unknown_object_classes, 'Add "unknown" object classes')
        self._execute_function(self._derive_remaining_fields, "Derive remaining fields")
        self._execute_function_and_log(self._import_object_classes, "Import object classes")

        # NOT deleting object classes is intentional for historical reasons.
        self._execute_function_and_log(
            update_changed_rows, "Update changed object classes", temp_object_class_table, object_class_table
        )
        self._execute_function_and_log(
            insert_missing_rows, "Insert missing object classes", temp_object_class_table, object_class_table
        )

    def _vacuum_tables(self):
        self._execute_dml_sql("vacuum (full, analyze) object_class", "Vacuum object_class table")
