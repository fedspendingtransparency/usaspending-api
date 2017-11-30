"""
Remove duplicate Locations, reassigning their dependent objects

The first step is to generate location_duplicate_ids, mapping a location_id to keep
to the location_ids that can be condensed down into it.  This step is SLOW and
unfortunately runs as a single query that cannot log its intermediate progress!
The remaining steps run in batches and log their progress.

Idempotent, although by default, it leaves the location_duplicate_ids table behind.
Running with `--drop` removes that table (then quits).
Otherwise, once location_duplicate_ids has been generated, the script can be
run again without regenerating it, saving time.
"""

import logging
import string
import time

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

BATCHES = 0
BATCH_SIZE = 10000


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument('--batch', type=int, default=BATCH_SIZE, help="Number of locations to fix per batch")
        parser.add_argument('--batches', type=int, default=BATCHES, help="Stop after N batches")
        parser.add_argument('--min_location_id', type=int, default=0, help="Begin at this location ID")
        parser.add_argument('--max_location_id', type=int, default=0, help="Stop at this location ID")
        parser.add_argument('--drop', action='store_true', help="Drop location_duplicate_ids and exit.")

    def execute_sql(self, qry, curs, start_time):
        qry = qry.strip()
        if qry:
            logger.info(qry.splitlines()[0])
            curs.execute(qry)
            logger.info('Elapsed: %d s' % int(time.time() - start_time))

    def handle(self, *args, **options):

        start_time = time.time()
        with connection.cursor() as curs:
            if options['drop']:
                self.execute_sql(qry=DROP_TABLE, curs=curs, start_time=start_time)
                return

            self.execute_sql(qry=CREATE_DUPE_TABLE, curs=curs, start_time=start_time)
            batches = self.find_batches(curs, options)
            for (floor, ceiling) in batches:
                for base_qry in REMOVE_DUPES.split('\n\n'):
                    logger.info('location_id %d to %d' % (floor, ceiling))
                    qry = string.Template(base_qry).safe_substitute(floor=floor, ceiling=ceiling)
                    self.execute_sql(qry=qry, curs=curs, start_time=start_time)

            for qry in FINALIZE.split('\n\n'):
                self.execute_sql(qry=qry, curs=curs, start_time=start_time)

    def find_batches(self, curs, options):
        batch = options['batch']
        n_batches = 0
        curs.execute(self.BOUNDARY_FINDER)
        (lowest, highest) = curs.fetchone()
        logger.info('location ID range in database %d to %d' % (lowest, highest))
        lowest = max(lowest, options['min_location_id'])
        if options['max_location_id']:
            highest = min(highest, options['max_location_id'])
        floor = (lowest // batch) * batch
        logger.info('location ID range to update %d to %d' % (lowest, highest))
        while (floor <= highest):
            if (options['batches'] and n_batches >= options['batches']):
                logger.info('Maximum # of batches, %d, reached' % options['batches'])
                break
            yield (floor, floor + batch)
            floor += batch
            n_batches += 1

    BOUNDARY_FINDER = """
        SELECT MIN(location_id) AS floor, MAX(location_id) AS ceiling
        FROM   references_location"""


CREATE_DUPE_TABLE = """
CREATE TABLE IF NOT EXISTS location_duplicate_ids AS
SELECT MIN(location_id) AS location_id,
       ARRAY_AGG(location_id) AS location_ids,
       MIN(reporting_period_start) AS reporting_period_start,
       MAX(reporting_period_end) AS reporting_period_end,
       MAX(last_modified_date) AS last_modified_date,
       MIN(create_date) AS create_date,
       MAX(update_date) AS update_date
FROM   references_location
GROUP BY
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
   certified_date,
   place_of_performance_flag,
   recipient_flag,
   location_country_code
HAVING count(*) > 1
;
"""

REMOVE_DUPES = """
UPDATE references_location
SET    reporting_period_start = d.reporting_period_start,
       reporting_period_end = d.reporting_period_end,
       last_modified_date = d.last_modified_date,
       create_date = d.create_date,
       update_date = d.update_date
FROM   location_duplicate_ids d
WHERE  references_location.location_id = d.location_id
AND    d.location_id >= ${floor}
AND    d.location_id < ${ceiling};


UPDATE awards
SET    place_of_performance_id = d.location_id
FROM   location_duplicate_ids d
WHERE  awards.place_of_performance_id = ANY(d.location_ids)
AND    d.location_id >= ${floor}
AND    d.location_id < ${ceiling};



UPDATE awards_subaward
SET    place_of_performance_id = d.location_id
FROM   location_duplicate_ids d
WHERE  awards_subaward.place_of_performance_id = ANY(d.location_ids)
AND    d.location_id >= ${floor}
AND    d.location_id < ${ceiling};



UPDATE legal_entity
SET    location_id = d.location_id
FROM   location_duplicate_ids d
WHERE  legal_entity.location_id = ANY(d.location_ids)
AND    d.location_id >= ${floor}
AND    d.location_id < ${ceiling};



UPDATE transaction_normalized
SET    place_of_performance_id = d.location_id
FROM   location_duplicate_ids d
WHERE  transaction_normalized.place_of_performance_id = ANY(d.location_ids)
AND    d.location_id >= ${floor}
AND    d.location_id < ${ceiling};



DELETE
FROM   references_location l
USING  location_duplicate_ids d
WHERE  l.location_id = ANY(d.location_ids)
AND    l.location_id != d.location_id
AND    d.location_id >= ${floor}
AND    d.location_id < ${ceiling};
"""

FINALIZE = """
VACUUM references_location;


VACUUM awards;


VACUUM awards_subaward;


VACUUM legal_entity;


VACUUM transaction_normalized;
"""

DROP_TABLE = "DROP TABLE IF EXISTS location_duplicate_ids;"
