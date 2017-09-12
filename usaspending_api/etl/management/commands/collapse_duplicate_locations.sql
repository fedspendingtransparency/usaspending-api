-- One-time fix to enable SQL-based loading; duplicate locations
-- must be collapsed.

ALTER TABLE references_location ADD COLUMN old_location_ids INTEGER[];


INSERT INTO references_location (
location_country_code,
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
congressional_code,
performance_code,
zip_last4,
zip5,
foreign_postal_code,
foreign_province,
foreign_city_name,
reporting_period_start,
reporting_period_end,
recipient_flag,
place_of_performance_flag,
zip_4a,
data_source,
last_modified_date,
certified_date,
create_date,
update_date,
old_location_ids
)
SELECT
  location_country_code,
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
  congressional_code,
  performance_code,
  zip_last4,
  zip5,
  foreign_postal_code,
  foreign_province,
  foreign_city_name,
  reporting_period_start,
  reporting_period_end,
  recipient_flag,
  place_of_performance_flag,
  MAX(zip_4a),
  MAX(data_source),
  MAX(last_modified_date),
  MAX(certified_date),
  MIN(create_date),
  MAX(update_date),
  ARRAY_AGG(location_id) AS ids
  from references_location
  group by 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
    20, 21, 22, 23, 24, 25
  HAVING count(*) > 1;

UPDATE legal_entity le
SET    location_id = l.location_id
FROM   references_location l
WHERE  le.location_id = ANY(l.old_location_ids);

UPDATE awards_subaward a
SET    place_of_performance_id = l.location_id
FROM   references_location l
WHERE  a.place_of_performance_id = ANY(l.old_location_ids);

UPDATE awards a
SET    place_of_performance_id = l.location_id
FROM   references_location l
WHERE  a.place_of_performance_id = ANY(l.old_location_ids);

UPDATE transaction t
SET    place_of_performance_id = l.location_id
FROM   references_location l
WHERE  t.place_of_performance_id = ANY(l.old_location_ids);

-- Remove the location rows replaced by aggregated rows

DELETE
FROM   references_location l
WHERE  l.location_id IN
  ( SELECT UNNEST(old_location_ids) FROM references_location);

ALTER TABLE references_location DROP COLUMN old_location_ids;
