"""
First step in process to fix locations from transaction_fpds / fabs tables

At the end, transaction_to_location is populated with transaction_id and location_id
"""

import logging
import string
import time

from django.core.management.base import BaseCommand
from django.db import connection

logger = logging.getLogger("console")
exception_logger = logging.getLogger("exceptions")

BATCH_DOWNLOAD_SIZE = 100000


class Command(BaseCommand):
    def add_arguments(self, parser):

        parser.add_argument("--batch", type=int, default=BATCH_DOWNLOAD_SIZE, help="ID range to update per query")
        parser.add_argument("--min_transaction_id", type=int, default=1, help="Begin at transaction ID")
        parser.add_argument("--max_transaction_id", type=int, default=0, help="End at transaction ID")

    def handle(self, *args, **options):

        start = time.time()
        with connection.cursor() as curs:
            batches = list(self.find_batches(curs, options))
            for base_qry in QUERIES.split("\n\n"):
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
                        logger.info("ID {} to {}, {} s".format(floor, ceiling, elapsed))
                else:  # simple query, no iterating over batch
                    curs.execute(base_qry)
                    elapsed = time.time() - start
                    logger.info("{} s".format(elapsed))

    def find_batches(self, curs, options):
        batch = options["batch"]
        curs.execute(self.BOUNDARY_FINDER)
        (lowest, highest) = curs.fetchone()
        lowest = max(lowest, options["min_transaction_id"])
        if options["max_transaction_id"]:
            highest = min(highest, options["max_transaction_id"])
        floor = (lowest // batch) * batch
        while floor <= highest:
            yield (floor, floor + batch)
            floor += batch

    BOUNDARY_FINDER = """
        SELECT MIN(id) AS floor, MAX(id) AS ceiling
        FROM   transaction_normalized"""


QUERIES = r"""
DROP TABLE IF EXISTS transaction_location_data CASCADE;


CREATE TABLE transaction_location_data
AS
SELECT t.transaction_id,
  l.data_source,
  l.location_id,
  l.country_name,
  l.state_code,
  l.state_name,
  l.state_description,
  l.city_name,
  l.city_code,
  l.county_name,
  l.county_code,
  l.address_line1,
  l.address_line2,
  l.address_line3,
  l.foreign_location_description,
  l.zip4,
  l.zip_4a,
  l.congressional_code,
  l.performance_code,
  l.zip_last4,
  l.zip5,
  l.foreign_postal_code,
  l.foreign_province,
  l.foreign_city_name,
  l.place_of_performance_flag,
  l.recipient_flag,
  l.location_country_code
FROM   references_location l,
       transaction_fabs t
WHERE  false;


DROP TABLE IF EXISTS state_abbrevs CASCADE;


CREATE TEMPORARY TABLE state_abbrevs (abbrev TEXT PRIMARY KEY, name TEXT NOT NULL);


INSERT INTO state_abbrevs (abbrev, name) VALUES
('AK', 'ALASKA'),
('AL', 'ALABAMA'),
('AR', 'ARKANSAS'),
('AS', 'AMERICAN SAMOA'),
('AZ', 'ARIZONA'),
('CA', 'CALIFORNIA'),
('CO', 'COLORADO'),
('CT', 'CONNECTICUT'),
('DC', 'DISTRICT OF COLUMBIA'),
('DE', 'DELAWARE'),
('FL', 'FLORIDA'),
('FM', 'FEDERATED STATES OF MICRONESIA'),
('GA', 'GEORGIA'),
('GU', 'GUAM'),
('HI', 'HAWAII'),
('IA', 'IOWA'),
('ID', 'IDAHO'),
('IL', 'ILLINOIS'),
('IN', 'INDIANA'),
('KS', 'KANSAS'),
('KY', 'KENTUCKY'),
('LA', 'LOUISIANA'),
('MA', 'MASSACHUSETTS'),
('MD', 'MARYLAND'),
('ME', 'MAINE'),
('MH', 'MARSHALL ISLANDS'),
('MI', 'MICHIGAN'),
('MN', 'MINNESOTA'),
('MO', 'MISSOURI'),
('MP', 'NORTHERN MARIANA ISLANDS'),
('MS', 'MISSISSIPPI'),
('MT', 'MONTANA'),
('NC', 'NORTH CAROLINA'),
('ND', 'NORTH DAKOTA'),
('NE', 'NEBRASKA'),
('NH', 'NEW HAMPSHIRE'),
('NJ', 'NEW JERSEY'),
('NM', 'NEW MEXICO'),
('NV', 'NEVADA'),
('NY', 'NEW YORK'),
('OH', 'OHIO'),
('OK', 'OKLAHOMA'),
('OR', 'OREGON'),
('PA', 'PENNSYLVANIA'),
('PR', 'PUERTO RICO'),
('PW', 'PALAU'),
('RI', 'RHODE ISLAND'),
('SC', 'SOUTH CAROLINA'),
('SD', 'SOUTH DAKOTA'),
('TN', 'TENNESSEE'),
('TX', 'TEXAS'),
('UT', 'UTAH'),
('UM', 'U.S. MINOR OUTLYING ISLANDS'),
('VA', 'VIRGINIA'),
('VI', 'VIRGIN ISLANDS'),
('VT', 'VERMONT'),
('WA', 'WASHINGTON'),
('WI', 'WISCONSIN'),
('WV', 'WEST VIRGINIA'),
('WY', 'WYOMING');


CREATE INDEX ON state_abbrevs (name);


-- transaction_location_data: recipient, FABS
INSERT INTO transaction_location_data (
  transaction_id,
  data_source,
  location_id,
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
  transaction_id,  -- ==> transaction_id
  'DBR',  -- ==> data_source
  NULL::INTEGER,  -- ==> location_id
  ref_country_code.country_name,  -- ==> country_name
  state_abbrevs.abbrev,  -- ==> state_code
  state_abbrevs.name,  -- ==> state_name
  NULL,  -- ==> state_description
  UPPER(TRIM(REGEXP_REPLACE(legal_entity_city_name, '\s+', ' '))),  -- ==> city_name
  NULL,  -- ==> city_code
  UPPER(TRIM(REGEXP_REPLACE(legal_entity_county_name, '\s+', ' '))),  -- ==> county_name
  legal_entity_county_code,  -- ==> county_code
  UPPER(TRIM(REGEXP_REPLACE(legal_entity_address_line1, '\s+', ' '))),  -- ==> address_line1
  UPPER(TRIM(REGEXP_REPLACE(legal_entity_address_line2, '\s+', ' '))),  -- ==> address_line2
  UPPER(TRIM(REGEXP_REPLACE(legal_entity_address_line3, '\s+', ' '))),  -- ==> address_line3
  NULL,  -- ==> foreign_location_description
  NULL,  -- ==> zip4
  NULL,  -- ==> zip_4a
  legal_entity_congressional,  -- ==> congressional_code
  NULL,  -- ==> performance_code
  legal_entity_zip_last4,  -- ==> zip_last4
  legal_entity_zip5,  -- ==> zip5
  legal_entity_foreign_posta,  -- ==> foreign_postal_code
  UPPER(TRIM(REGEXP_REPLACE(legal_entity_foreign_provi, '\s+', ' '))),  -- ==> foreign_province
  UPPER(TRIM(REGEXP_REPLACE(legal_entity_foreign_city, '\s+', ' '))),  -- ==> foreign_city_name
  false,  -- ==> place_of_performance_flag
  true,  -- ==> recipient_flag
    -- different for place of performance!
  ref_country_code.country_code  -- ==> location_country_code
FROM transaction_fabs
LEFT OUTER JOIN ref_country_code ON (
    CASE
      WHEN transaction_fabs.legal_entity_country_code = 'UNITED STATES' THEN 'USA'
      WHEN transaction_fabs.legal_entity_country_code IS NULL THEN 'USA'
      WHEN transaction_fabs.legal_entity_country_code IN (
        'ASM', 'GUM', 'MNP', 'PRI', 'VIR', 'UMI', 'FSM', 'MHL', 'PLW') THEN 'USA'
    ELSE legal_entity_country_code END
      = ref_country_code.country_code
)
LEFT OUTER JOIN state_abbrevs ON (
    ref_country_code.country_code = 'USA'
    AND (    REPLACE(transaction_fabs.legal_entity_state_code, '.', '') = state_abbrevs.abbrev
         OR  UPPER(TRIM(REGEXP_REPLACE(transaction_fabs.legal_entity_state_name, '\s+', ' '))) = state_abbrevs.name))
WHERE transaction_id >= ${floor}
AND   transaction_id < ${ceiling}
AND awardee_or_recipient_uniqu is NULL;




-- transaction_location_data: recipient, FPDS
INSERT INTO transaction_location_data (
  transaction_id,
  data_source,
  location_id,
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
  transaction_id,  -- ==> transaction_id
  'DBR',  -- ==> data_source
  NULL::INTEGER,  -- ==> location_id
  ref_country_code.country_name,  -- ==> country_name
  state_abbrevs.abbrev,  -- ==> state_code
  state_abbrevs.name,  -- ==> state_name
  NULL,  -- ==> state_description
  UPPER(TRIM(REGEXP_REPLACE(legal_entity_city_name, '\s+', ' '))),  -- ==> city_name
  NULL,  -- ==> city_code
  NULL,  -- ==> county_name
  NULL,  -- ==> county_code
  UPPER(TRIM(REGEXP_REPLACE(legal_entity_address_line1, '\s+', ' '))),  -- ==> address_line1
  UPPER(TRIM(REGEXP_REPLACE(legal_entity_address_line2, '\s+', ' '))),  -- ==> address_line2
  UPPER(TRIM(REGEXP_REPLACE(legal_entity_address_line3, '\s+', ' '))),  -- ==> address_line3
  NULL,  -- ==> foreign_location_description
  legal_entity_zip4,  -- ==> zip4
  NULL,  -- ==> zip_4a
  legal_entity_congressional,  -- ==> congressional_code
  NULL,  -- ==> performance_code
  NULL,  -- ==> zip_last4
  SUBSTRING(legal_entity_zip4 FROM '^(\d{5})\-?(\d{4})?$'),  -- ==> zip5
  NULL,  -- ==> foreign_postal_code
  NULL,  -- ==> foreign_province
  NULL,  -- ==> foreign_city_name
  false,  -- ==> place_of_performance_flag
  true,  -- ==> recipient_flag
  ref_country_code.country_code  -- ==> location_country_code
FROM transaction_fpds
LEFT OUTER JOIN ref_country_code ON (
    CASE
      WHEN transaction_fpds.legal_entity_country_code = 'UNITED STATES' THEN 'USA'
      WHEN transaction_fpds.legal_entity_country_code IS NULL THEN 'USA'
      WHEN transaction_fpds.legal_entity_country_code IN (
        'ASM', 'GUM', 'MNP', 'PRI', 'VIR', 'UMI', 'FSM', 'MHL', 'PLW') THEN 'USA'
    ELSE legal_entity_country_code END
      = ref_country_code.country_code
)
LEFT OUTER JOIN state_abbrevs ON (
        ref_country_code.country_code = 'USA'
    AND REPLACE(transaction_fpds.legal_entity_state_code, '.', '') = state_abbrevs.abbrev
         )
WHERE transaction_id >= ${floor}
AND   transaction_id < ${ceiling}
AND   awardee_or_recipient_uniqu is NULL;


-- transaction_location_data: place of performance, FABS
INSERT INTO transaction_location_data (
  transaction_id,
  data_source,
  location_id,
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
  transaction_id,  -- ==> transaction_id
  'DBR',  -- ==> data_source
  NULL::INTEGER,  -- ==> location_id
  ref_country_code.country_name,  -- ==> country_name
  state_abbrevs.abbrev,  -- ==> state_code
  state_abbrevs.name,  -- ==> state_name
  NULL,  -- ==> state_description
  UPPER(TRIM(REGEXP_REPLACE(place_of_performance_city, '\s+', ' '))),  -- ==> city_name
  NULL,  -- ==> city_code
  UPPER(TRIM(REGEXP_REPLACE(place_of_perform_county_na, '\s+', ' '))),  -- ==> county_name
  NULL,  -- ==> county_code
  NULL,  -- ==> address_line1
  NULL,  -- ==> address_line2
  NULL,  -- ==> address_line3
  NULL,  -- ==> foreign_location_description
  NULL,  -- ==> zip4
  NULL,  -- ==> zip_4a
  place_of_performance_congr,  -- ==> congressional_code
  NULL,  -- ==> performance_code
  place_of_performance_zip4a,  -- ==> zip_last4
  SUBSTRING(place_of_performance_zip4a FROM '^(\d{5})\-?(\d{4})?$'),  -- ==> zip5
  NULL,  -- ==> foreign_postal_code
  NULL,  -- ==> foreign_province
  NULL,  -- ==> foreign_city_name
  true,  -- ==> place_of_performance_flag
  false,  -- ==> recipient_flag
  ref_country_code.country_code  -- ==> location_country_code
FROM transaction_fabs
LEFT OUTER JOIN ref_country_code ON (
    CASE
      WHEN transaction_fabs.place_of_performance_code != '00FORGN' THEN 'USA'
      WHEN transaction_fabs.place_of_perform_country_c IN (
        'ASM', 'GUM', 'MNP', 'PRI', 'VIR', 'UMI', 'FSM', 'MHL', 'PLW') THEN 'USA'
      ELSE transaction_fabs.place_of_perform_country_c END
      = ref_country_code.country_code
)
LEFT OUTER JOIN state_abbrevs ON (
    ref_country_code.country_code = 'USA'
    AND UPPER(TRIM(REGEXP_REPLACE(transaction_fabs.place_of_perform_state_nam, '\s+', ' '))) = state_abbrevs.name)
WHERE transaction_id >= ${floor}
AND   transaction_id < ${ceiling}
AND awardee_or_recipient_uniqu is NULL;


-- transaction_location_data: place of performance, FPDS
INSERT INTO transaction_location_data (
  transaction_id,
  data_source,
  location_id,
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
  transaction_id,  -- ==> transaction_id
  'DBR',  -- ==> data_source
  NULL::INTEGER,  -- ==> location_id
  ref_country_code.country_name,  -- ==> country_name
  state_abbrevs.abbrev,  -- ==> state_code
  state_abbrevs.name,  -- ==> state_name
  NULL,  -- ==> state_description
  UPPER(TRIM(REGEXP_REPLACE(place_of_performance_locat, '\s+', ' '))),  -- ==> city_name
  NULL,  -- ==> city_code
  NULL,  -- ==> county_name
  NULL,  -- ==> county_code
  NULL,  -- ==> address_line1
  NULL,  -- ==> address_line2
  NULL,  -- ==> address_line3
  NULL,  -- ==> foreign_location_description
  NULL,  -- ==> zip4
  NULL,  -- ==> zip_4a
  place_of_performance_congr,  -- ==> congressional_code
  NULL,  -- ==> performance_code
  place_of_performance_zip4a,  -- ==> zip_last4
  SUBSTRING(place_of_performance_zip4a FROM '^(\d{5})\-?(\d{4})?$'),  -- ==> zip5
  NULL,  -- ==> foreign_postal_code
  NULL,  -- ==> foreign_province
  NULL,  -- ==> foreign_city_name
  true,  -- ==> place_of_performance_flag
  false,  -- ==> recipient_flag
  ref_country_code.country_code  -- ==> location_country_code
FROM transaction_fpds
LEFT OUTER JOIN ref_country_code ON (
    CASE
      WHEN transaction_fpds.place_of_perform_country_c = 'UNITED STATES' THEN 'USA'
      WHEN transaction_fpds.place_of_perform_country_c IS NULL THEN 'USA'
      WHEN transaction_fpds.place_of_perform_country_c IN (
        'ASM', 'GUM', 'MNP', 'PRI', 'VIR', 'UMI', 'FSM', 'MHL', 'PLW') THEN 'USA'
    ELSE place_of_perform_country_c END
      = ref_country_code.country_code
)
LEFT OUTER JOIN state_abbrevs ON (
    ref_country_code.country_code = 'USA'
    AND UPPER(TRIM(REGEXP_REPLACE(transaction_fpds.place_of_performance_state, '\s+', ' '))) = state_abbrevs.name)
WHERE transaction_id >= ${floor}
AND   transaction_id < ${ceiling}
AND awardee_or_recipient_uniqu is NULL;


CREATE INDEX ON transaction_location_data (transaction_id);


ALTER TABLE references_location ADD COLUMN IF NOT EXISTS transaction_ids INTEGER[];


DROP TABLE IF EXISTS transaction_to_location;


CREATE TABLE transaction_to_location AS
SELECT t.transaction_id,
       l.location_id
FROM   transaction_location_data t,
       references_location l
WHERE  false;


WITH new_locs AS (
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
  SELECT ARRAY_AGG(transaction_id),
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
  WHERE transaction_location_data.transaction_id >= ${floor}
  AND   transaction_location_data.transaction_id < ${ceiling}
  GROUP BY
    2, 3, 4, 5, 6, 7, 8, 9, 10,
    11, 12, 13, 14, 15, 16, 17, 18, 19, 20,
    21, 22, 23, 24, 25, 26
  RETURNING transaction_ids, location_id
)
INSERT INTO transaction_to_location (transaction_id, location_id)
SELECT UNNEST(transaction_ids), location_id
FROM   new_locs
;


ALTER TABLE references_location DROP COLUMN transaction_ids;
"""
