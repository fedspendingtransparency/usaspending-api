DROP VIEW IF EXISTS location_delta_view;

CREATE VIEW location_delta_view AS
WITH country_cte AS (
	SELECT
		CASE
			WHEN
				pop_country_name = 'UNITED STATES OF AMERICA'
			THEN
				'UNITED STATES'
			ELSE
				pop_country_name
		END AS country_name
	FROM
		rpt.transaction_search
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
		END AS country_name
	FROM
		rpt.transaction_search
	WHERE
		recipient_location_country_name IS NOT NULL
),
state_cte AS (
	SELECT
		UPPER(sd.name) AS state_name,
		'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd ON recipient_location_state_name = UPPER(sd.name)
	UNION
	SELECT
		UPPER(sd.name) AS state_name,
		'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd ON pop_state_name = UPPER(sd.name)
),
county_cte AS (
	SELECT
		CONCAT(SPLIT_PART(pop_county_name, ', ', 1), ' COUNTY') AS county_name,
		CONCAT(pop_state_fips, pop_county_code) AS county_fips,
		UPPER(sd.name) AS state_name,
		'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd
			ON pop_state_name = UPPER(sd.name) AND pop_state_fips = sd.fips
	WHERE
		pop_country_name IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
		AND
		(
			pop_county_name IS NOT NULL
			AND
			pop_county_name ~ '^[a-zA-Z]'
		)
		AND
		CONCAT(pop_state_fips, pop_county_code) ~ '^[0-9]{5}$'
	UNION
	SELECT
		CONCAT(SPLIT_PART(recipient_location_county_name, ', ', 1), ' COUNTY') AS county_name,
		CONCAT(recipient_location_state_fips, recipient_location_county_code) AS county_fips,
		UPPER(sd.name) AS state_name,
		'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd
			ON recipient_location_state_name = UPPER(sd.name) AND recipient_location_state_fips = sd.fips
	WHERE
		recipient_location_country_name IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
		AND
		(
			recipient_location_county_name IS NOT NULL
			AND
			recipient_location_county_name ~ '^[a-zA-Z]'
		)
		AND
		CONCAT(recipient_location_state_fips, recipient_location_county_code) ~ '^[0-9]{5}$'
),
zip_cte AS (
	SELECT
		pop_zip5 AS zip_code,
		UPPER(sd.name) AS state_name,
		'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd ON pop_state_name = UPPER(sd.name)
	WHERE
		pop_country_name IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
		AND
		pop_zip5 IS NOT NULL
	UNION
	SELECT
		recipient_location_zip5 AS zip_code,
		UPPER(sd.name) AS state_name,
		'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd ON recipient_location_state_name = UPPER(sd.name)
	WHERE
		recipient_location_country_name IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
		AND
		recipient_location_zip5 IS NOT NULL
),
current_cd_cte AS (
	SELECT
		CONCAT(sd.code, '-', recipient_location_congressional_code_current) AS current_cd,
		UPPER(sd.name) AS state_name,
		'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd ON recipient_location_state_name = UPPER(sd.name)
	WHERE
		recipient_location_country_name IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
		AND
		recipient_location_congressional_code_current IS NOT NULL
	UNION
	SELECT
		CONCAT(sd.code, '-', pop_congressional_code_current) AS current_cd,
		UPPER(sd.name) AS state_name,
		'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd ON pop_state_name = UPPER(sd.name)
	WHERE
		pop_country_name IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
		AND
		pop_congressional_code_current IS NOT NULL
),
original_cd_cte AS (
	SELECT
		CONCAT(sd.code, '-', recipient_location_congressional_code) AS original_cd,
		UPPER(sd.name) AS state_name,
		'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd ON recipient_location_state_name = UPPER(sd.name)
	WHERE
		recipient_location_country_name IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
		AND
		recipient_location_congressional_code IS NOT NULL
	UNION
	SELECT
		CONCAT(sd.code, '-', pop_congressional_code) AS original_cd,
		UPPER(sd.name) AS state_name,
		'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd ON pop_state_name = UPPER(sd.name)
	WHERE
		pop_country_name IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
		AND
		pop_congressional_code IS NOT NULL
),
domestic_city_cte AS (
	SELECT
		recipient_location_city_name AS city_name,
		UPPER(sd.name) AS state_name,
		'UNITED STATES' AS country_name
	FROM rpt.transaction_search
	INNER JOIN
		state_data sd ON recipient_location_state_name = UPPER(sd.name)
	WHERE
		recipient_location_country_name IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
		AND
		(
			recipient_location_city_name IS NOT NULL
			AND
			recipient_location_city_name NOT LIKE '%,%'
		)
	UNION
	SELECT
		pop_city_name AS city_name,
		UPPER(sd.name) AS state_name,
		'UNITED STATES' AS country_name
	FROM rpt.transaction_search
	INNER JOIN
		state_data sd ON pop_state_name = UPPER(sd.name)
	WHERE
		pop_country_name IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
		AND
		(
			pop_city_name IS NOT NULL
			AND
			pop_city_name NOT LIKE '%,%'
		)
),
foreign_city_cte AS (
	SELECT
		recipient_location_city_name AS city_name,
		recipient_location_country_name AS country_name
	FROM rpt.transaction_search
	WHERE
		recipient_location_city_name IS NOT NULL
		AND
		(
			recipient_location_country_name IS NOT NULL
			AND
			recipient_location_country_name NOT IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
		)
	UNION
	SELECT
		pop_city_name AS city_name,
		pop_country_name AS country_name
	FROM rpt.transaction_search
	WHERE
		pop_city_name IS NOT NULL
		AND
		(
			pop_country_name IS NOT NULL
			AND
			pop_country_name NOT IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
		)
),
select_cte AS (
	SELECT
		country_name AS location,
		TO_JSONB(
			JSONB_BUILD_OBJECT(
				'country_name', country_name
			)
		) AS location_json,
		'country' AS location_type
	FROM
		country_cte
	UNION
	SELECT
		CONCAT(state_name, ', ', country_name) AS location,
		TO_JSONB(
			JSONB_BUILD_OBJECT(
				'state_name', state_name,
				'country_name', country_name
			)
		) AS location_json,
		'state' AS location_type
	FROM
		state_cte
	UNION
	SELECT
		CONCAT(county_name, ', ', state_name, ', ', country_name) AS location,
		TO_JSONB(
			JSONB_BUILD_OBJECT(
				'county_name', county_name,
				'county_fips', county_fips,
				'state_name', state_name,
				'country_name', country_name
			)
		) AS location_json,
		'county' AS location_type
	FROM
		county_cte
	UNION
	SELECT
		CONCAT(zip_code, ', ', state_name, ', ', country_name) AS location,
		TO_JSONB(
			JSONB_BUILD_OBJECT(
				'zip_code', zip_code,
				'state_name', state_name,
				'country_name', country_name
			)
		) AS location_json,
		'zip_code' AS location_type
	FROM
		zip_cte
	UNION
	SELECT
		CONCAT(REPLACE(current_cd, '-', ''), ', ', state_name, ', ', country_name) AS location,
		TO_JSONB(
			JSONB_BUILD_OBJECT(
				'current_cd', current_cd,
				'state_name', state_name,
				'country_name', country_name
			)
		) AS location_json,
		'current_cd' AS location_type
	FROM
		current_cd_cte
	UNION
	SELECT
		CONCAT(REPLACE(original_cd, '-', ''), ', ', state_name, ', ', country_name) AS location,
		TO_JSONB(
			JSONB_BUILD_OBJECT(
				'original_cd', original_cd,
				'state_name', state_name,
				'country_name', country_name
			)
		) AS location_json,
		'original_cd' AS location_type
	FROM
		original_cd_cte
	UNION
	SELECT
		CONCAT(city_name, ', ', state_name, ', ', country_name) AS location,
		TO_JSONB(
			JSONB_BUILD_OBJECT(
				'city_name', city_name,
				'state_name', state_name,
				'country_name', country_name
			)
		) AS location_json,
		'city' AS location_type
	FROM
		domestic_city_cte
	UNION
	SELECT
		CONCAT(city_name, ', ', country_name) AS location,
		TO_JSONB(
			JSONB_BUILD_OBJECT(
				'city_name', city_name,
				'state_name', NULL,
				'country_name', country_name
			)
		) AS location_json,
		'city' AS location_type
	FROM
		foreign_city_cte
)
SELECT
	ROW_NUMBER() OVER (ORDER BY location, location_json, location_type) AS id,
	location,
	location_json,
	location_type
FROM
	select_cte
WHERE
	location IS NOT NULL
GROUP BY
	location,
	location_json,
	location_type