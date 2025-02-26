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
		CASE
			WHEN
				pop_country_name = 'UNITED STATES'
				AND
				pop_state_name IS NOT NULL
				AND
				pop_county_name ~ '^[a-zA-Z]'
			THEN
				CONCAT(pop_county_name, ' COUNTY')
		END AS county_name,
		CASE
			WHEN
				pop_country_name = 'UNITED STATES'
				AND
				pop_state_name IS NOT NULL
				AND
				CONCAT(pop_state_fips, pop_county_code) ~ '^[0-9]{5}$'
			THEN
				CONCAT(pop_state_fips, pop_county_code)
		END AS county_fips,
        UPPER(sd.name) AS state_name,
        'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd
            ON pop_state_name = UPPER(sd.name) AND pop_state_fips = sd.fips
	WHERE
		pop_country_name IS NOT NULL
        AND
        pop_county_name NOT LIKE '%,%'
	UNION
	SELECT
		CASE
			WHEN
				recipient_location_country_name = 'UNITED STATES'
				AND
				recipient_location_state_name IS NOT NULL
				AND
				recipient_location_county_name ~ '^[a-zA-Z]'
			THEN
				CONCAT(recipient_location_county_name, ' COUNTY', ', ', UPPER(sd.name))
		END AS county_name,
		CASE
			WHEN
				recipient_location_country_name = 'UNITED STATES'
				AND
				recipient_location_state_name IS NOT NULL
				AND
				CONCAT(recipient_location_state_fips, recipient_location_county_code) ~ '^[0-9]{5}$'
			THEN
				CONCAT(recipient_location_state_fips, recipient_location_county_code)
		END AS county_fips,
        UPPER(sd.name) AS state_name,
        'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd
            ON recipient_location_state_name = UPPER(sd.name) AND recipient_location_state_fips = sd.fips
	WHERE
		recipient_location_country_name IS NOT NULL
        AND
        recipient_location_county_name NOT LIKE '%,%'
),
zip_cte AS (
	SELECT
		CASE
			WHEN
				pop_country_name = 'UNITED STATES'
				AND
				sd.name IS NOT NULL
                AND
				pop_zip5 IS NOT NULL
			THEN
				pop_zip5
		END AS zip_code,
        UPPER(sd.name) AS state_name,
        'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd ON pop_state_name = UPPER(sd.name)
	WHERE
		pop_county_name IS NOT NULL
	UNION
	SELECT
		CASE
			WHEN
				recipient_location_country_name = 'UNITED STATES'
				AND
				sd.name IS NOT NULL
                AND
				recipient_location_zip5 IS NOT NULL
			THEN
				recipient_location_zip5
			ELSE
				NULL
		END AS zip_code,
        UPPER(sd.name) AS state_name,
        'UNITED STATES' AS country_name
	FROM
		rpt.transaction_search
	INNER JOIN
		state_data sd ON recipient_location_state_name = UPPER(sd.name)
	WHERE
		recipient_location_country_name IS NOT NULL
),
current_cd_cte AS (
    SELECT
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
		END AS current_cd,
        UPPER(sd.name) AS state_name,
        'UNITED STATES' AS country_name
    FROM
        rpt.transaction_search
    INNER JOIN
		state_data sd ON recipient_location_state_name = UPPER(sd.name)
	WHERE
		recipient_location_country_name IS NOT NULL
	UNION
	SELECT
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
		END AS current_cd,
        UPPER(sd.name) AS state_name,
        'UNITED STATES' AS country_name
    FROM
        rpt.transaction_search
    INNER JOIN
		state_data sd ON pop_state_name = UPPER(sd.name)
	WHERE
		pop_country_name IS NOT NULL
),
original_cd_cte AS (
    SELECT
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
				CONCAT(UPPER(sd.code), recipient_location_congressional_code)
		END AS original_cd,
        UPPER(sd.name) AS state_name,
        'UNITED STATES' AS country_name
    FROM
        rpt.transaction_search
    INNER JOIN
		state_data sd ON recipient_location_state_name = UPPER(sd.name)
	WHERE
		recipient_location_country_name IS NOT NULL
	UNION
	SELECT
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
				CONCAT(UPPER(sd.code), pop_congressional_code)
		END AS original_cd,
        UPPER(sd.name) AS state_name,
        'UNITED STATES' AS country_name
    FROM
        rpt.transaction_search
    INNER JOIN
		state_data sd ON pop_state_name = UPPER(sd.name)
	WHERE
		pop_country_name IS NOT NULL
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
        recipient_location_city_name NOT LIKE '%,%'
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
        pop_city_name NOT LIKE '%,%'
),
foreign_city_cte AS (
    SELECT
        recipient_location_city_name AS city_name,
        recipient_location_country_name AS country_name
    FROM rpt.transaction_search
    WHERE
        recipient_location_city_name IS NOT NULL
        AND
        recipient_location_country_name IS NOT NULL
        AND
        recipient_location_country_name NOT IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
    UNION
    SELECT
        pop_city_name AS city_name,
        pop_country_name AS country_name
    FROM rpt.transaction_search
    WHERE
        pop_city_name IS NOT NULL
        AND
        pop_country_name IS NOT NULL
        AND
        pop_country_name NOT IN ('UNITED STATES', 'UNITED STATES OF AMERICA')
),
select_cte AS (
    SELECT
        NULL AS city_name,
        NULL AS zip_code,
        NULL AS county_name,
        NULL AS county_fips,
        NULL AS current_cd,
        NULL AS original_cd,
        NULL AS state_name,
        country_name
    FROM
        country_cte
    UNION
    SELECT
        NULL AS city_name,
        NULL AS zip_code,
        NULL AS county_name,
        NULL AS county_fips,
        NULL AS current_cd,
        NULL AS original_cd,
        state_name,
        country_name
    FROM
        state_cte
    UNION
    SELECT
        NULL AS city_name,
        NULL AS zip_code,
        county_name,
        county_fips,
        NULL AS current_cd,
        NULL AS original_cd,
        state_name,
        country_name
    FROM
        county_cte
    UNION
    SELECT
        NULL AS city_name,
        zip_code,
        NULL AS county_name,
        NULL AS county_fips,
        NULL AS current_cd,
        NULL AS original_cd,
        state_name,
        country_name
    FROM
        zip_cte
    UNION
    SELECT
        NULL AS city_name,
        NULL AS zip_code,
        NULL AS county_name,
        NULL AS county_fips,
        current_cd,
        NULL AS original_cd,
        state_name,
        country_name
    FROM
        current_cd_cte
    UNION
    SELECT
        NULL AS city_name,
        NULL AS zip_code,
        NULL AS county_name,
        NULL AS county_fips,
        NULL AS current_cd,
        original_cd,
        state_name,
        country_name
    FROM
        original_cd_cte
    UNION
    SELECT
        city_name,
        NULL AS zip_code,
        NULL AS county_name,
        NULL AS county_fips,
        NULL AS current_cd,
        NULL AS original_cd,
        state_name,
        country_name
    FROM
        domestic_city_cte
    UNION
    SELECT
        city_name,
        NULL AS zip_code,
        NULL AS county_name,
        NULL AS county_fips,
        NULL AS current_cd,
        NULL AS original_cd,
        NULL AS state_name,
        country_name
    FROM
        foreign_city_cte
)
SELECT
    ROW_NUMBER() OVER (ORDER BY city_name, state_name, country_name) AS id,
    city_name,
    zip_code,
    county_name,
    county_fips,
    current_cd,
    original_cd,
    state_name,
    country_name
FROM
    select_cte
GROUP BY
    city_name,
    zip_code,
    county_name,
    county_fips,
    current_cd,
    original_cd,
    state_name,
    country_name