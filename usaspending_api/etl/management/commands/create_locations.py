"""
Last step in process to fix locations from transaction_fpds / fabs tables

1. populate_transaction_location_data
2. match_translations_to_locations - probably in parallel (it is the slow step)
3. this script - AFTER all parallel instances of match_translations_to_locations have finished

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
ALTER TABLE references_location
ADD COLUMN IF NOT EXISTS transaction_ids INTEGER[];


  INSERT INTO references_location (
    transaction_ids,
    data_source,
    country_name,
    state_code,
    state_name,
    state_description,
    city_name,
    city_code,
    county_name,
    county_code,
    address_line1,
    address_line2,
    address_line3,
    foreign_location_description,
    zip4,
    zip_4a,
    congressional_code,
    performance_code,
    zip_last4,
    zip5,
    foreign_postal_code,
    foreign_province,
    foreign_city_name,
    place_of_performance_flag,
    recipient_flag,
    location_country_code
  )
  SELECT
    ARRAY_AGG(transaction_id),  -- ==> transaction_ids
    data_source,  -- ==> data_source
    country_name,  -- ==> country_name
    state_code,  -- ==> state_code
    state_name,  -- ==> state_name
    state_description,  -- ==> state_description
    city_name,  -- ==> city_name
    city_code,  -- ==> city_code
    county_name,  -- ==> county_name
    county_code,  -- ==> county_code
    address_line1,  -- ==> address_line1
    address_line2,  -- ==> address_line2
    address_line3,  -- ==> address_line3
    foreign_location_description,  -- ==> foreign_location_description
    zip4,  -- ==> zip4
    zip_4a,  -- ==> zip_4a
    congressional_code,  -- ==> congressional_code
    performance_code,  -- ==> performance_code
    zip_last4,  -- ==> zip_last4
    zip5,  -- ==> zip5
    foreign_postal_code,  -- ==> foreign_postal_code
    foreign_province,  -- ==> foreign_province
    foreign_city_name,  -- ==> foreign_city_name
    place_of_performance_flag,  -- ==> place_of_performance_flag
    recipient_flag,  -- ==> recipient_flag
    location_country_code  -- ==> location_country_code
  FROM transaction_location_data
  WHERE transaction_location_data.location_id IS NULL
  GROUP BY 2, 3, 4, 5, 6, 7, 8, 9, 10,
           11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
           21, 22, 23, 24, 25, 26
  ;


UPDATE transaction_location_data
SET    location_id = l.location_id
FROM   references_location l
WHERE  transaction_location_data.transaction_id = ANY(l.transaction_ids)
AND    transaction_location_data.location_id IS NULL
AND    transaction_location_data.transaction_id >= ${floor}
AND    transaction_location_data.transaction_id < ${ceiling};


ALTER TABLE references_location DROP COLUMN transaction_ids;
"""
