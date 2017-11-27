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
CREATE INDEX
IF NOT EXISTS
references_location_composite__idx1
ON references_location
( MD5(
    COALESCE(data_source, '') ||
    COALESCE(country_name, '') ||
    COALESCE(state_code, '') ||
    COALESCE(state_name, '') ||
    COALESCE(state_description, '') ||
    COALESCE(city_name, '') ||
    COALESCE(city_code, '') ||
    COALESCE(county_name, '') ||
    COALESCE(county_code, '') ||
    COALESCE(address_line1, '') ||
    COALESCE(address_line2, '') ||
    COALESCE(address_line3, '') ||
    COALESCE(foreign_location_description, '') ||
    COALESCE(zip4, '') ||
    COALESCE(zip_4a, '') ||
    COALESCE(congressional_code, '') ||
    COALESCE(performance_code, '') ||
    COALESCE(zip_last4, '') ||
    COALESCE(zip5, '') ||
    COALESCE(foreign_postal_code, '') ||
    COALESCE(foreign_province, '') ||
    COALESCE(foreign_city_name, '') ||
    COALESCE(place_of_performance_flag::TEXT, '') ||
    COALESCE(recipient_flag::TEXT, '') ||
    COALESCE(location_country_code, '')
));


UPDATE transaction_location_data
SET    location_id = l.location_id
FROM   references_location l
WHERE
  MD5(
    COALESCE(transaction_location_data.data_source, '') ||
    COALESCE(transaction_location_data.country_name, '') ||
    COALESCE(transaction_location_data.state_code, '') ||
    COALESCE(transaction_location_data.state_name, '') ||
    COALESCE(transaction_location_data.state_description, '') ||
    COALESCE(transaction_location_data.city_name, '') ||
    COALESCE(transaction_location_data.city_code, '') ||
    COALESCE(transaction_location_data.county_name, '') ||
    COALESCE(transaction_location_data.county_code, '') ||
    COALESCE(transaction_location_data.address_line1, '') ||
    COALESCE(transaction_location_data.address_line2, '') ||
    COALESCE(transaction_location_data.address_line3, '') ||
    COALESCE(transaction_location_data.foreign_location_description, '') ||
    COALESCE(transaction_location_data.zip4, '') ||
    COALESCE(transaction_location_data.zip_4a, '') ||
    COALESCE(transaction_location_data.congressional_code, '') ||
    COALESCE(transaction_location_data.performance_code, '') ||
    COALESCE(transaction_location_data.zip_last4, '') ||
    COALESCE(transaction_location_data.zip5, '') ||
    COALESCE(transaction_location_data.foreign_postal_code, '') ||
    COALESCE(transaction_location_data.foreign_province, '') ||
    COALESCE(transaction_location_data.foreign_city_name, '') ||
    COALESCE(transaction_location_data.place_of_performance_flag::TEXT, '') ||
    COALESCE(transaction_location_data.recipient_flag::TEXT, '') ||
    COALESCE(transaction_location_data.location_country_code, '')
) =
MD5(
    COALESCE(l.data_source, '') ||
    COALESCE(l.country_name, '') ||
    COALESCE(l.state_code, '') ||
    COALESCE(l.state_name, '') ||
    COALESCE(l.state_description, '') ||
    COALESCE(l.city_name, '') ||
    COALESCE(l.city_code, '') ||
    COALESCE(l.county_name, '') ||
    COALESCE(l.county_code, '') ||
    COALESCE(l.address_line1, '') ||
    COALESCE(l.address_line2, '') ||
    COALESCE(l.address_line3, '') ||
    COALESCE(l.foreign_location_description, '') ||
    COALESCE(l.zip4, '') ||
    COALESCE(l.zip_4a, '') ||
    COALESCE(l.congressional_code, '') ||
    COALESCE(l.performance_code, '') ||
    COALESCE(l.zip_last4, '') ||
    COALESCE(l.zip5, '') ||
    COALESCE(l.foreign_postal_code, '') ||
    COALESCE(l.foreign_province, '') ||
    COALESCE(l.foreign_city_name, '') ||
    COALESCE(l.place_of_performance_flag::TEXT, '') ||
    COALESCE(l.recipient_flag::TEXT, '') ||
    COALESCE(l.location_country_code, '')
)
AND transaction_location_data.transaction_id >= ${floor}
AND transaction_location_data.transaction_id < ${ceiling};
"""
