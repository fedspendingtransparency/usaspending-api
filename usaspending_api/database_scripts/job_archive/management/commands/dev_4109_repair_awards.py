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

from usaspending_api.common.helpers.timing_helpers import Timer

logger = logging.getLogger("console")

CHECK_MISSING_AWARDS = """
    SELECT tf.{field}
    FROM {table} AS tf
    LEFT OUTER JOIN awards a on a.generated_unique_award_id = tf.unique_award_key
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

    def handle(self, *args, **options):

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
                cursor.execute(
                    CHECK_MISSING_AWARDS.format(table="transaction_fpds", field="detached_award_procurement_id")
                )
                ids_to_load = [str(row[0]) for row in cursor.fetchall()]

            if ids_to_load:
                logger.info(f"Reloading {len(ids_to_load)} transactions from Broker")
                call_command("load_fpds_transactions", "--ids", " ".join(ids_to_load))
            else:
                logger.info("No transactions to load")

    def part_two(self):
        fix_txn_fk_to_awards = """
            UPDATE transaction_normalized AS tn
            SET award_id = a.id
            FROM awards a
            WHERE
                tn.unique_award_key = a.generated_unique_award_id
                AND tn.award_id IS DISTINCT FROM a.id
            RETURNING tn.id
        """
        with Timer("Running Part 2: Fixing Transaction FKs"):
            with connection.cursor() as cursor:
                cursor.execute(fix_txn_fk_to_awards)
                transactions = [str(row[0]) for row in cursor.fetchall()]
            if transactions:
                logger.info(f"Updated transactions:\n{', '.join(transactions)}")

    def part_three(self):
        logger.info("Part 3: Re-Computing Awards")
        logger.warning("Run usaspending-api/usaspending_api/scripts/archive/recompute_all_awards.py")

    def inspect(self):
        logger.info("Running SQL to inspect data")
        faulty_transaction_to_awards_fks = """
            SELECT
                tn.id,
                tn.unique_award_key as uak,
                a.generated_unique_award_id AS guai
            FROM transaction_normalized tn
            LEFT OUTER JOIN awards a ON tn.award_id = a.id
            WHERE
                a.generated_unique_award_id IS DISTINCT FROM tn.unique_award_key
        """

        faulty_awards_to_transactions_fks = """
            SELECT
                a.id,
                a.generated_unique_award_id AS unique_award_key_in_award,
            FROM awards a
            LEFT JOIN transaction_normalized e ON a.earliest_transaction_id = e.id
            LEFT JOIN transaction_normalized l ON a.latest_transaction_id = l.id
            WHERE
                a.generated_unique_award_id != e.unique_award_key
                OR a.generated_unique_award_id != l.unique_award_key
                OR a.id != e.award_id
                OR a.id != l.award_id;
        """

        blank_award_fks = "SELECT * FROM awards where earliest_transaction_id is null or latest_transaction_id is null"

        with connection.cursor() as cursor:
            with Timer("Checking FPDS"):
                cursor.execute(CHECK_MISSING_AWARDS.format(table="transaction_fpds", field="unique_award_key"))
                fpds_missing_ids = set([str(row[0]) for row in cursor.fetchall()])

            with Timer("Checking FABS"):
                cursor.execute(CHECK_MISSING_AWARDS.format(table="transaction_fabs", field="unique_award_key"))
                fabs_missing_ids = set([str(row[0]) for row in cursor.fetchall()])

            with Timer("Checking bad FKs in transaction_normalized"):
                cursor.execute(faulty_transaction_to_awards_fks)
                faulty_txn_fks = [row for row in cursor.fetchall()]

            with Timer("Checking bad FKs in awards"):
                cursor.execute(faulty_awards_to_transactions_fks)
                faulty_award_fks = [row for row in cursor.fetchall()]

            with Timer("Checking missing FKs in Awards"):
                cursor.execute(blank_award_fks)
                blank_fks = [row for row in cursor.fetchall()]

        if fpds_missing_ids:
            logger.warn(f"{', '.join(fpds_missing_ids)}\n\n{len(fpds_missing_ids)} missing Procurement award count")
        else:
            logger.info("No Procurement awards missing based on `unique_award_key`")

        if fabs_missing_ids:
            logger.warn(f"{', '.join(fabs_missing_ids)}\n\n{len(fabs_missing_ids)} missing Assistance award count")
        else:
            logger.info("No Asssitance awards missing based on `unique_award_key`")

        if faulty_txn_fks:
            records = "\n\t".join([f"TXN ID {f[0]} | stored key {f[1]} | award record {f[2]}" for f in faulty_txn_fks])
            message = f"Faulty FKs found `transaction_normalized.award_id` -> `awards.id`\n{records}"
            logger.warn(message)
        else:
            logger.info("No `transaction_normalized.award_id` -> `awards.id` mismatches")

        if faulty_award_fks:
            records = "\n\t".join([f"Award ID {f[0]} | stored key {f[1]}" for f in faulty_award_fks])
            msg = (
                "Faulty FKs found `awards.latest_transaction_id|awards.earliest_transaction_id` "
                " -> `transaction_normalized.award_id`"
            )
            logger.warn(f"{msg}\n{records}")
        else:
            logger.info("No `transaction_normalized.award_id` -> `awards.id` mismatches")

        if blank_fks:
            logger.warn(f"{', '.join(blank_fks)}\n\n{len(blank_fks)} Awards missing transaction FKs")
        else:
            logger.info("All awards have populated FKs to transaction_normalized")
