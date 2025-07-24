import logging
from typing import Iterator

from django.core.management import BaseCommand
from django.db import connections

from usaspending_api.common.helpers.timing_helpers import Timer
from usaspending_api.settings import DATA_BROKER_DB_ALIAS, DEFAULT_DB_ALIAS

logger = logging.getLogger(__name__)


class Command(BaseCommand):

    help = "Command used for incrementally updating a field in a USAspending DB table based on its Broker counterpart"

    CHUNK_SIZE = 50_000

    load_field_type: str

    usas_table_name: str
    usas_load_field: str
    usas_match_field: str

    broker_table_name: str
    broker_load_field: str
    broker_match_field: str

    def add_arguments(self, parser):
        parser.add_argument(
            "--load-field-type",
            type=str,
            required=False,
            default="text",
            help="Postgres data type of the field that will be copied from Broker",
        )

        usaspending_group = parser.add_argument_group(
            title="usaspending", description="Fields that reference components of the USAspending DB"
        )
        usaspending_group.add_argument(
            "--table-name",
            type=str,
            required=True,
            help="Table name in the USAspending DB to update",
        )
        usaspending_group.add_argument(
            "--load-field", type=str, required=True, help="Field that will be updated on the USAspending DB"
        )
        usaspending_group.add_argument(
            "--match-field",
            type=str,
            required=False,
            help="Field that will be matched against on USAspending DB for making updates",
        )

        broker_group = parser.add_argument_group(
            title="broker",
            description=(
                "Optional fields that reference components of the Broker DB; each defaults to the USAspending "
                "counterpart"
            ),
        )
        broker_group.add_argument(
            "--broker-table-name",
            type=str,
            required=False,
            help="Table name in the Broker DB to update",
        )
        broker_group.add_argument(
            "--broker-load-field",
            type=str,
            required=False,
            help="Field that will be read from Broker DB when updating the USAspending field",
        )
        broker_group.add_argument(
            "--broker-match-field",
            type=str,
            required=False,
            help="Field that will be matched against on Broker DB for making updates",
        )

    def id_ranges(self, min_id: int, max_id: int) -> Iterator[tuple[int, int]]:
        for n in range(min_id, max_id + 1, self.CHUNK_SIZE):
            yield n, min(n + self.CHUNK_SIZE, max_id)

    def get_match_field_range(self) -> tuple[int, int]:
        with connections[DATA_BROKER_DB_ALIAS].cursor() as cursor:
            cursor.execute(
                f"""
                SELECT min({self.broker_match_field}), max({self.broker_match_field})
                FROM {self.broker_table_name}
            """
            )
            min_val, max_val = cursor.fetchone()
        if min_val is None or max_val is None:
            msg = (
                f"Range of {min_val} to {max_val} is invalid for Broker table "
                f'"{self.broker_table_name}"."{self.broker_match_field}"'
            )
            logger.error(msg)
            raise ValueError(msg)
        logger.info(
            f"Found range of {min_val} to {max_val} for Broker table "
            f'"{self.broker_table_name}"."{self.broker_match_field}"'
        )
        return min_val, max_val

    def run_update(self, min_id: int, max_id: int) -> None:
        total_row_count = 0
        estimated_id_count = max_id - min_id + 1
        with Timer(
            "Performing update from Broker --> USAspending", success_logger=logger.info, failure_logger=logger.error
        ) as timer:
            for chunk_min_id, chunk_max_id in self.id_ranges(min_id, max_id):
                with connections[DEFAULT_DB_ALIAS].cursor() as cursor:
                    cursor.execute(
                        f"""
                            UPDATE {self.usas_table_name} AS usas_table
                            SET {self.usas_load_field} = broker_table.load_field
                            FROM dblink(
                                'broker_server','(
                                    SELECT {self.broker_match_field}, {self.broker_load_field}
                                    FROM {self.broker_table_name}
                                    WHERE
                                        {self.broker_match_field} >= {chunk_min_id}
                                        AND {self.broker_match_field} <= {chunk_max_id}
                                )') AS broker_table
                                     (
                                          lookup_id bigint,
                                          load_field {self.load_field_type}
                                     )
                            WHERE
                                usas_table.{self.usas_match_field} = broker_table.lookup_id
                        ;
                        """
                    )

                row_count = cursor.rowcount
                total_row_count += row_count
                ratio = (chunk_max_id - min_id + 1) / estimated_id_count
                logger.info(
                    f'Updated {row_count:,d} rows with "{self.usas_match_field}" between {chunk_min_id:,d} and {chunk_max_id:,d}.'
                    f" Estimated time remaining: {timer.estimated_remaining_runtime(ratio)}"
                )
        logger.info(
            f'Finished updating {total_row_count:,d} rows for "{self.usas_table_name}"."{self.usas_load_field}" '
            f"in {timer}"
        )

    def handle(self, *args, **options):
        self.load_field_type = options["load_field_type"]

        self.usas_table_name = options["table_name"]
        self.usas_load_field = options["load_field"]
        self.usas_match_field = options["match_field"]

        self.broker_table_name = options["broker_table_name"] or self.usas_table_name
        self.broker_load_field = options["broker_load_field"] or self.usas_load_field
        self.broker_match_field = options["broker_match_field"] or self.usas_match_field

        logger.info(
            f'Copying "{self.broker_table_name}"."{self.broker_load_field}" from Broker to '
            f'"{self.usas_table_name}"."{self.usas_load_field}" in USAspending.'
        )
        logger.info(
            f'Matching "{self.broker_table_name}"."{self.broker_match_field}" from Broker to '
            f'"{self.usas_table_name}"."{self.usas_match_field}" in USAspending.'
        )

        min_val, max_val = self.get_match_field_range()
        self.run_update(min_val, max_val)
