"""
Second step in process to fix locations from transaction_fpds / fabs tables

1. populate_transaction_location_data
2. match_translations_to_locations - probably in parallel (it is the slow step)
3. create_locations - AFTER all parallel instances of match_translations_to_locations have finished

At the end, transaction_location_data is populated with transaction_id and location_id
"""

import logging
import string
import time

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

BATCH_DOWNLOAD_SIZE = 10000


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument('--batch', type=int, default=BATCH_DOWNLOAD_SIZE, help="ID range to update per query")
        parser.add_argument('--min_location_id', type=int, default=1, help="Begin at transaction ID")
        parser.add_argument('--max_location_id', type=int, default=0, help="End at transaction ID")

    def handle(self, *args, **options):

        start = time.time()
        with connection.cursor() as curs:
            batches = list(self.find_batches(curs, options))
            for base_qry in QUERIES.split('\n\n'):
                base_qry = base_qry.strip()
                if not base_qry:
                    continue
                first_line = base_qry.splitlines()[0]
                logger.info(first_line)
                if "${floor}" in base_qry:
                    for (floor, ceiling) in batches:
                        qry = string.Template(base_qry).safe_substitute(floor=floor, ceiling=ceiling)
                        curs.execute(qry)
                        elapsed = time.time() - start
                        logger.info('ID {} to {}, {} s'.format(floor, ceiling, elapsed))
                else:  # simple query, no iterating over batch
                    curs.execute(base_qry)
                    elapsed = time.time() - start
                    logger.info('{} s'.format(elapsed))

    def find_batches(self, curs, options):
        batch = options['batch']
        curs.execute(self.BOUNDARY_FINDER)
        (lowest, highest) = curs.fetchone()
        lowest = max(lowest, options['min_location_id'])
        if options['max_location_id']:
            highest = min(highest, options['max_location_id'])
        floor = (lowest // batch) * batch
        while floor <= highest:
            yield (floor, floor + batch)
            floor += batch

    BOUNDARY_FINDER = """
        SELECT MIN(id) AS floor, MAX(id) AS ceiling
        FROM   transaction_normalized"""


QUERIES = """
-- match existing locations
UPDATE transaction_location_data
SET    location_id = l.location_id
FROM   references_location l
WHERE
  transaction_location_data.data_source IS NOT DISTINCT FROM l.data_source AND
  transaction_location_data.country_name IS NOT DISTINCT FROM l.country_name AND
  transaction_location_data.state_code IS NOT DISTINCT FROM l.state_code AND
  transaction_location_data.state_name IS NOT DISTINCT FROM l.state_name AND
  transaction_location_data.state_description IS NOT DISTINCT FROM l.state_description AND
  transaction_location_data.city_name IS NOT DISTINCT FROM l.city_name AND
  transaction_location_data.city_code IS NOT DISTINCT FROM l.city_code AND
  transaction_location_data.county_name IS NOT DISTINCT FROM l.county_name AND
  transaction_location_data.county_code IS NOT DISTINCT FROM l.county_code AND
  transaction_location_data.address_line1 IS NOT DISTINCT FROM l.address_line1 AND
  transaction_location_data.address_line2 IS NOT DISTINCT FROM l.address_line2 AND
  transaction_location_data.address_line3 IS NOT DISTINCT FROM l.address_line3 AND
  transaction_location_data.foreign_location_description IS NOT DISTINCT FROM l.foreign_location_description AND
  transaction_location_data.zip4 IS NOT DISTINCT FROM l.zip4 AND
  transaction_location_data.zip_4a IS NOT DISTINCT FROM l.zip_4a AND
  transaction_location_data.congressional_code IS NOT DISTINCT FROM l.congressional_code AND
  transaction_location_data.performance_code IS NOT DISTINCT FROM l.performance_code AND
  transaction_location_data.zip_last4 IS NOT DISTINCT FROM l.zip_last4 AND
  transaction_location_data.zip5 IS NOT DISTINCT FROM l.zip5 AND
  transaction_location_data.foreign_postal_code IS NOT DISTINCT FROM l.foreign_postal_code AND
  transaction_location_data.foreign_province IS NOT DISTINCT FROM l.foreign_province AND
  transaction_location_data.foreign_city_name IS NOT DISTINCT FROM l.foreign_city_name AND
  transaction_location_data.place_of_performance_flag IS NOT DISTINCT FROM l.place_of_performance_flag AND
  transaction_location_data.recipient_flag IS NOT DISTINCT FROM l.recipient_flag AND
  transaction_location_data.location_country_code IS NOT DISTINCT FROM l.location_country_code
AND transaction_location_data.transaction_id >= ${floor}
AND transaction_location_data.transaction_id < ${ceiling};
  ;
"""
