import logging
import time

from django.core.management.base import BaseCommand
from django.db import connection


class Command(BaseCommand):
    help = "One-time fixer to fill missing location.zip5 from zip4"

    # Estimated runtime, based on simple linear extrap: 10 hours

    logger = logging.getLogger("console")

    LIMIT = 10000
    regexp = r"^(\d{5})\-?(\d{4})?$"

    fixable_rows = """
              SELECT location_id
          FROM   references_location
          WHERE  zip5 IS NULL
          AND    zip4 IS NOT NULL
          AND    zip4 ~* '{}'
    """.format(
        regexp
    )

    update_qry = """
       WITH target_ids AS (
          {fixable_rows}
          LIMIT {limit}
       )
       UPDATE references_location rl
       SET    zip5 = SUBSTRING(zip4 FROM '{regexp}')
       FROM   target_ids
       WHERE  rl.location_id = target_ids.location_id;
    """.format(
        fixable_rows=fixable_rows, limit=LIMIT, regexp=regexp
    )

    unfixed_qry = """
        SELECT EXISTS (
            {fixable_rows}
        )""".format(
        fixable_rows=fixable_rows
    )

    def handle(self, *args, **options):
        overall_start_time = time.time()
        with connection.cursor() as cursor:
            unfixed = True
            while unfixed:
                start_time = time.time()
                cursor.execute(self.update_qry)
                cursor.execute(self.unfixed_qry)
                unfixed = cursor.fetchone()[0]
                elapsed = time.time() - start_time
                self.logger.info("Batch of <= {} fixed in {} seconds".format(self.LIMIT, elapsed))
        overall_elapsed = time.time() - overall_start_time
        self.logger.info("Finished in {} seconds".format(overall_elapsed))
