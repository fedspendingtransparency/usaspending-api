"""
Jira Ticket Number(s): DEV-4109

Expected CLI:

    $ python3 manage.py dev_4109_repair_awards

Purpose:

    Correct Faulty FKs and missing award records
"""

import logging

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.db import connection
from time import perf_counter

from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer
from usaspending_api.common.operations_reporter import OpsReporter


logger = logging.getLogger("console")

CHECK_MISSING_AWARDS = """
    SELECT tf.{field}
    FROM {table} AS tf
    LEFT OUTER JOIN vw_awards a on a.generated_unique_award_id = tf.unique_award_key
    WHERE a.id IS NULL;
"""


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument(
            "--inspect",
            action="store_true",
            help=(
                "Don't execute DML commands. "
                "Print results of SQL statements to review database "
                " records to track data state over time"
            ),
        )
        parser.add_argument(
            "--metrics",
            type=str,
            help="If provided, a JSON file will be created at the path containing metrics from the run",
        )

    def handle(self, *args, **options):
        self.warning_status = False
        self.metrics = OpsReporter(job_name="dev_4109_repair_awards.py")
        start = perf_counter()

        with Timer("dev_4109_repair_awards.py"):
            self.main(options)

        self.metrics["end_status"] = 3 if self.warning_status else 0
        self.metrics["duration"] = perf_counter() - start
        if options["metrics"]:
            self.metrics.dump_to_file(options["metrics"])

        logger.info("Complete")

    def main(self, options):

        if options["inspect"]:
            self.inspect()
            return

        with Timer(f"{__name__}"):
            self.part_one()
            self.part_two()
            self.part_three()

    def part_one(self):
        with Timer("Part 1: Addressing Missing Award Records"):
            with connection.cursor() as cursor:
                cursor.execute(CHECK_MISSING_AWARDS.format(table="transaction_fabs", field="afa_generated_unique"))
                fabs_to_load = [str(row[0]) for row in cursor.fetchall()]
                self.metrics["count_of_fabs_to_reload"] = len(fabs_to_load)

                cursor.execute(
                    CHECK_MISSING_AWARDS.format(table="transaction_fpds", field="detached_award_procurement_id")
                )
                fpds_to_load = [str(row[0]) for row in cursor.fetchall()]
                self.metrics["count_of_fpds_to_reload"] = len(fpds_to_load)

            if fabs_to_load:
                logger.warning(f"NEED TO LOAD {len(fabs_to_load)} transactions from Broker!")
                logger.info(
                    "This script will not handle this reload. "
                    "Store this IDs in a file for fabs_nightly_loader --afa-id-file:"
                    f"\n{' '.join(fabs_to_load)}"
                    "\n (note, check case of -none- vs -NONE-"
                )
            else:
                logger.info("No FABS transactions need to be reloaded")

            if fpds_to_load:
                logger.info(f"Reloading {len(fpds_to_load)} FPDS transactions from Broker\n{' '.join(fpds_to_load)}")
                call_command("load_fpds_transactions", "--ids", *fpds_to_load)
            else:
                logger.info("No FPDS transactions need to be reloaded")

    def part_two(self):
        fix_txn_fk_to_awards = """
            UPDATE vw_transaction_normalized AS tn
            SET award_id = a.id
            FROM vw_awards a
            WHERE
                tn.unique_award_key = a.generated_unique_award_id
                AND tn.award_id IS DISTINCT FROM a.id
            RETURNING tn.id
        """
        with Timer("Running Part 2: Fixing Transaction FKs"):
            with connection.cursor() as cursor:
                cursor.execute(fix_txn_fk_to_awards)
                transactions = [str(row[0]) for row in cursor.fetchall()]

            self.metrics["updated_transaction_normalized_fks_to_awards"] = len(transactions)
            if transactions:
                logger.info(f"Updated transactions:\n{', '.join(transactions)}")
            else:
                logger.info("No transactions needed to be updated")

    def part_three(self):
        logger.info("Part 3: Re-Computing Awards")
        logger.warning("Run usaspending_api/database_scripts/job_archive/recompute_all_awards.py")

    def inspect(self):
        logger.info("Running SQL to inspect data")
        faulty_transaction_to_awards_fks = """
            SELECT
                tn.id,
                tn.unique_award_key as uak,
                a.generated_unique_award_id AS guai
            FROM transaction_normalized tn
            LEFT OUTER JOIN vw_awards a ON tn.award_id = a.id
            WHERE
                a.generated_unique_award_id IS DISTINCT FROM tn.unique_award_key
        """

        faulty_awards_to_transactions_fks = """
            SELECT
                a.id,
                a.generated_unique_award_id AS unique_award_key_in_award
            FROM vw_awards a
            LEFT JOIN transaction_normalized e ON a.earliest_transaction_id = e.id
            LEFT JOIN transaction_normalized l ON a.latest_transaction_id = l.id
            WHERE
                a.generated_unique_award_id != e.unique_award_key
                OR a.generated_unique_award_id != l.unique_award_key
                OR a.id != e.award_id
                OR a.id != l.award_id;
        """

        blank_award_fks = """
            SELECT
                id,
                generated_unique_award_id
            FROM vw_awards
            WHERE
                   earliest_transaction_id IS NULL
                OR latest_transaction_id IS NULL
            """

        with connection.cursor() as cursor:
            with Timer("Checking FPDS"):
                cursor.execute(CHECK_MISSING_AWARDS.format(table="transaction_fpds", field="unique_award_key"))
                fpds_missing_ids = set([str(row[0]) for row in cursor.fetchall()])
                self.metrics["count_missing_fpds_awards"] = len(fpds_missing_ids)

            with Timer("Checking FABS"):
                cursor.execute(CHECK_MISSING_AWARDS.format(table="transaction_fabs", field="unique_award_key"))
                fabs_missing_ids = set([str(row[0]) for row in cursor.fetchall()])
                self.metrics["count_missing_fabs_awards"] = len(fabs_missing_ids)

            with Timer("Checking bad FKs in transaction_normalized"):
                cursor.execute(faulty_transaction_to_awards_fks)
                faulty_txn_fks = [row for row in cursor.fetchall()]
                self.metrics["count_faulty_award_fks_in_transaction_normalized"] = len(faulty_txn_fks)

            with Timer("Checking bad FKs in awards"):
                cursor.execute(faulty_awards_to_transactions_fks)
                faulty_award_fks = [row for row in cursor.fetchall()]
                self.metrics["count_faulty_transaction_fks_in_awards"] = len(faulty_award_fks)

            with Timer("Checking missing FKs in Awards"):
                cursor.execute(blank_award_fks)
                blank_fks = [row for row in cursor.fetchall()]
                self.metrics["count_blank_fks_in_awards"] = len(blank_fks)

        if fpds_missing_ids:
            self.warning_status = True
            logger.warning(f"{', '.join(fpds_missing_ids)}\n\n{len(fpds_missing_ids)} missing Procurement awards")
        else:
            logger.info("No Procurement awards missing based on `unique_award_key`")

        if fabs_missing_ids:
            self.warning_status = True
            logger.warning(f"{', '.join(fabs_missing_ids)}\n\n{len(fabs_missing_ids)} missing Assistance awards")
        else:
            logger.info("No Assistance awards missing based on `unique_award_key`")

        if faulty_txn_fks:
            self.warning_status = True
            records = "\n\t".join([f"TXN ID {f[0]} | stored key {f[1]} | award record {f[2]}" for f in faulty_txn_fks])
            message = f"{len(faulty_txn_fks)} faulty FKs `transaction_normalized.award_id` -> `awards.id`\n\t{records}"
            logger.warning(message)
        else:
            logger.info("No `transaction_normalized.award_id` -> `awards.id` mismatches")

        if faulty_award_fks:
            self.warning_status = True
            records = "\n\t".join([f"Award ID {f[0]} | stored key {f[1]}" for f in faulty_award_fks])
            msg = (
                f"{len(faulty_award_fks)}  faulty FKs `awards.latest_transaction_id|awards.earliest_transaction_id` "
                " -> `transaction_normalized.award_id`"
            )
            logger.warning(f"{msg}\n\t{records}")
        else:
            logger.info("No `transaction_normalized.award_id` -> `awards.id` mismatches")

        if blank_fks:
            self.warning_status = True
            records = "\n\t".join([f"Award ID {f[0]} | stored key {f[1]}" for f in blank_fks])
            logger.warning(f"{len(blank_fks)} Awards missing transaction FKs\n\t{records}")
        else:
            logger.info("All awards have populated FKs to transaction_normalized")
