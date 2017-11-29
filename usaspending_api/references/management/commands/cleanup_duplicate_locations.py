"""
Remove duplicate Locations, reassigning their dependent objects

Idempotent.
"""

import logging
import time

from django.core.management.base import BaseCommand, CommandError
from django.db import connection

logger = logging.getLogger('console')
exception_logger = logging.getLogger("exceptions")

BATCHES = 0
BATCH_SIZE = 1000


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument('--batch', type=int, default=BATCH_SIZE, help="Number of locations to fix per batch")
        parser.add_argument('--batches', type=int, default=BATCHES, help="Stop after N batches")

    def handle(self, *args, **options):

        start_time = time.time()
        batches_done = 0
        batches_to_do = options['batches']
        with connection.cursor() as curs:
            while (not batches_to_do) or (batches_done < batches_to_do):
                curs.execute(CREATE_DUPE_TABLE.format(batch=options['batch']))
                curs.execute(DUPE_TABLE_ROWS)
                rows = curs.fetchone()[0]
                if not rows:
                    logger.info('No duplicate locations remaining')
                    break
                curs.execute(REMOVE_DUPES)
                logger.info('Batch of {} finished.  Elapsed: {} s'.format(
                    options['batch'], time.time() - start_time))
                batches_done += 1

            for sql in FINALIZE.split('\n\n'):
                curs.execute(sql)
        logger.info('Completed in {} s'.format(time.time() - start_time))


CREATE_DUPE_TABLE = """
DROP TABLE IF EXISTS location_duplicate_ids;


CREATE TABLE location_duplicate_ids AS
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
LIMIT {batch}
;
"""

DUPE_TABLE_ROWS = "SELECT COUNT(*) FROM location_duplicate_ids"

REMOVE_DUPES = """
UPDATE references_location
SET    reporting_period_start = d.reporting_period_start,
       reporting_period_end = d.reporting_period_end,
       last_modified_date = d.last_modified_date,
       create_date = d.create_date,
       update_date = d.update_date
FROM   location_duplicate_ids d
WHERE  references_location.location_id = d.location_id;


UPDATE awards
SET    place_of_performance_id = d.location_id
FROM   location_duplicate_ids d
WHERE  awards.place_of_performance_id = ANY(d.location_ids);


UPDATE awards_subaward
SET    place_of_performance_id = d.location_id
FROM   location_duplicate_ids d
WHERE  awards_subaward.place_of_performance_id = ANY(d.location_ids);


UPDATE legal_entity
SET    location_id = d.location_id
FROM   location_duplicate_ids d
WHERE  legal_entity.location_id = ANY(d.location_ids);


UPDATE transaction_normalized
SET    place_of_performance_id = d.location_id
FROM   location_duplicate_ids d
WHERE  transaction_normalized.place_of_performance_id = ANY(d.location_ids);


DELETE
FROM   references_location l
USING  location_duplicate_ids d
WHERE  l.location_id = ANY(d.location_ids)
AND    l.location_id != d.location_id;


DROP TABLE IF EXISTS location_duplicate_ids;
"""

FINALIZE = """
VACUUM references_location;


VACUUM awards;


VACUUM awards_subaward;


VACUUM legal_entity;


VACUUM transaction_normalized;
"""
