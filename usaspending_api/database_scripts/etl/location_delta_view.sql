DROP VIEW IF EXISTS location_delta_view;

CREATE VIEW location_delta_view AS
WITH locations_cte AS (
	SELECT
		CASE
			WHEN
				pop_country_name = 'UNITED STATES OF AMERICA'
			THEN
				'UNITED STATES'
			ELSE
				pop_country_name
		END AS country_name,
		CASE
			WHEN
				pop_country_name = 'UNITED STATES'
				AND
				pop_state_name IN (SELECT UPPER(name) FROM state_data)
			THEN
				pop_state_name
			ELSE
				NULL
		END AS state_name,
		CASE
			WHEN
				pop_country_name = 'UNITED STATES'
				AND
				pop_state_name IS NOT NULL
				AND
				pop_city_name ~ '^[a-zA-Z]'
			THEN
				pop_city_name
			ELSE
				NULL
		END AS city_name,
		CASE
			WHEN
				pop_country_name = 'UNITED STATES'
				AND
				pop_state_name IS NOT NULL
				AND
				pop_county_name ~ '^[a-zA-Z]'
			THEN
				pop_county_name
			ELSE
				NULL
		END AS county_name,
		CASE
			WHEN
				pop_country_name = 'UNITED STATES'
				AND
				pop_state_name IS NOT NULL
				AND
				pop_state_fips ~ '^[0-9]{2}$'
			THEN
				pop_state_fips
			ELSE
				NULL
		END AS state_fips,
		CASE
			WHEN
				pop_country_name = 'UNITED STATES'
				AND
				pop_state_name IS NOT NULL
				AND
				pop_county_code ~ '^[0-9]{3}$'
			THEN
				pop_county_code
			ELSE
				NULL
		END AS county_fips,
		CASE
			WHEN
				pop_country_name = 'UNITED STATES'
				AND
				pop_state_name IS NOT NULL
			THEN
				pop_zip5
			ELSE
				NULL
		END AS zip_code,
		CASE
			WHEN
				pop_country_name = 'UNITED STATES'
				AND
				(
					sd.code IS NOT NULL
					AND
					pop_congressional_code_current IS NOT NULL
				)
			THEN
				CONCAT(sd.code, pop_congressional_code_current)
			ELSE
				NULL
		END AS current_congressional_district,
		CASE
			WHEN
				pop_country_name = 'UNITED STATES'
				AND
				(
					sd.code IS NOT NULL
					AND
					pop_congressional_code IS NOT NULL
				)
			THEN
				CONCAT(sd.code, pop_congressional_code)
			ELSE
				NULL
		END AS original_congressional_district
	FROM	
		rpt.transaction_search
	LEFT JOIN
		state_data sd ON pop_state_name = UPPER(sd.name)
	WHERE
		pop_country_name IS NOT NULL
	UNION
	SELECT
		CASE
			WHEN
				recipient_location_country_name = 'UNITED STATES OF AMERICA'
			THEN
				'UNITED STATES'
			ELSE
				recipient_location_country_name 
		END AS country_name,
		CASE
			WHEN
				recipient_location_country_name = 'UNITED STATES'
				AND
				recipient_location_state_name IN (SELECT UPPER(name) FROM state_data)
			THEN
				recipient_location_state_name
			ELSE
				NULL 
		END AS state_name,
		CASE
			WHEN
				recipient_location_country_name = 'UNITED STATES'
				AND
				recipient_location_state_name IS NOT NULL
				AND
				recipient_location_city_name ~ '^[a-zA-Z]'
			THEN
				recipient_location_city_name
			ELSE
				NULL
		END AS city_name,
		CASE
			WHEN
				recipient_location_country_name = 'UNITED STATES'
				AND
				recipient_location_state_name IS NOT NULL
				AND
				recipient_location_county_name ~ '^[a-zA-Z]'
			THEN
				recipient_location_county_name
			ELSE
				NULL
		END AS county_name,
		CASE
			WHEN
				recipient_location_country_name = 'UNITED STATES'
				AND
				recipient_location_state_name IS NOT NULL
				AND
				recipient_location_state_fips ~ '^[0-9]{2}$'
			THEN
				recipient_location_state_fips
			ELSE
				NULL
		END AS state_fips,
		CASE
			WHEN
				recipient_location_country_name = 'UNITED STATES'
				AND
				recipient_location_state_name IS NOT NULL
				AND
				recipient_location_county_code ~ '^[0-9]{3}$'
			THEN
				recipient_location_county_code
			ELSE
				NULL
		END AS county_fips,
		CASE
			WHEN
				recipient_location_country_name = 'UNITED STATES'
				AND
				recipient_location_state_name IS NOT NULL
			THEN
				recipient_location_zip5
			ELSE
				NULL
		END AS zip_code,
		CASE
			WHEN
				recipient_location_country_name = 'UNITED STATES'
				AND
				(
					sd.code IS NOT NULL
					AND
					recipient_location_congressional_code_current IS NOT NULL
				)
			THEN
				CONCAT(sd.code, recipient_location_congressional_code_current)
			ELSE
				NULL
		END AS current_congressional_district,
		CASE
			WHEN
				recipient_location_country_name = 'UNITED STATES'
				AND
				(
					sd.code IS NOT NULL
					AND
					recipient_location_congressional_code IS NOT NULL
				)
			THEN
				CONCAT(sd.code, recipient_location_congressional_code)
			ELSE
				NULL
		END AS original_congressional_district
	FROM
		rpt.transaction_search
	LEFT JOIN
		state_data sd ON recipient_location_state_name = UPPER(sd.name)
	WHERE
		recipient_location_country_name IS NOT NULL
)
SELECT
    ROW_NUMBER() OVER (ORDER BY country_name, state_name) AS id,
	country_name,
	state_name,
	array_agg(DISTINCT(city_name)) FILTER (WHERE city_name IS NOT NULL) AS cities,
	to_json(array_agg(DISTINCT(jsonb_build_object(
		'name', county_name,
		'fips', CONCAT(state_fips, county_fips)
	)))
	FILTER (
		WHERE (
			county_name IS NOT NULL
			AND
			state_fips IS NOT NULL
			AND
			county_fips IS NOT NULL
		)
	))
	AS counties,
	array_agg(DISTINCT(zip_code)) FILTER (WHERE zip_code IS NOT NULL) AS zip_codes,
	array_agg(DISTINCT(current_congressional_district)) FILTER (WHERE current_congressional_district IS NOT NULL) AS current_congressional_districts,
	array_agg(DISTINCT(original_congressional_district)) FILTER (WHERE original_congressional_district IS NOT NULL) AS original_congressional_districts
FROM
	locations_cte
WHERE
	-- require state name for UNITED STATES
	(
		country_name = 'UNITED STATES'
		AND
		state_name IS NOT NULL
	)
    OR
	-- only need country name for foreign countries since we don't support foreign "states"
	(
		country_name != 'UNITED STATES'
		AND
		state_name is NULL
	)
GROUP BY
	country_name,
	state_name
