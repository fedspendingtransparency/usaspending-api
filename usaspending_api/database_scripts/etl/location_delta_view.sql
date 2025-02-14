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
		END AS location_string,
		'country' AS location_type
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
		END AS location_string,
		'country' AS location_type
	FROM
		rpt.transaction_search
	WHERE
		recipient_location_country_name IS NOT NULL
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
				CONCAT(pop_county_name, ' COUNTY', ', ', UPPER(sd.name))
		END AS location_string,
		'county' AS location_type,
		CASE
			WHEN
				pop_country_name = 'UNITED STATES'
				AND
				pop_state_name IS NOT NULL
				AND
				CONCAT(pop_state_fips, pop_county_code) ~ '^[0-9]{5}$'
			THEN
				CONCAT(pop_state_fips, pop_county_code)
		END AS county_fips
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
		END AS location_string,
		'county' AS location_type,
		CASE
			WHEN
				recipient_location_country_name = 'UNITED STATES'
				AND
				recipient_location_state_name IS NOT NULL
				AND
				CONCAT(recipient_location_state_fips, recipient_location_county_code) ~ '^[0-9]{5}$'
			THEN
				CONCAT(recipient_location_state_fips, recipient_location_county_code)
		END AS county_fips
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
			    and
				pop_zip5 IS NOT NULL
			THEN
				CONCAT(pop_zip5, ', ', UPPER(sd.name))
		END AS location_string,
		'zip_code' AS location_type
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
				CONCAT(recipient_location_zip5, ', ', UPPER(sd.name))
			ELSE
				NULL
		END AS location_string,
		'zip_code' AS location_type
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
				CONCAT(sd.code, recipient_location_congressional_code_current, ', ', UPPER(sd.name))
		END AS location_string,
        'current_congressional_district' AS location_type
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
				CONCAT(sd.code, pop_congressional_code_current, ', ', UPPER(sd.name))
		END AS location_string,
        'current_congressional_district' AS location_type
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
				CONCAT(UPPER(sd.code), recipient_location_congressional_code, ', ', UPPER(sd.name))
		END AS location_string,
        'original_congressional_district' AS location_type
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
				CONCAT(UPPER(sd.code), pop_congressional_code, ', ', UPPER(sd.name))
		END AS location_string,
        'original_congressional_district' AS location_type
    FROM
        rpt.transaction_search
    INNER JOIN
		state_data sd ON pop_state_name = UPPER(sd.name)
	WHERE
		pop_country_name IS NOT NULL
),
state_cte AS (
    SELECT
        UPPER(sd.name) AS location_string,
        'state' AS location_type
    FROM
        rpt.transaction_search
    INNER JOIN
        state_data sd ON recipient_location_state_name = UPPER(sd.name)
    UNION
    SELECT
        UPPER(sd.name) AS location_string,
        'state' AS location_type
    FROM
        rpt.transaction_search
    INNER JOIN
        state_data sd ON pop_state_name = UPPER(sd.name)
),
city_cte AS (
    SELECT
        CASE
            WHEN
                recipient_location_country_name = 'UNITED STATES'
                AND
                (
                    sd.code IS NOT NULL
                    AND
                    recipient_location_city_name IS NOT NULL
                )
            THEN
                CONCAT(UPPER(recipient_location_city_name), ', ', UPPER(sd.name))
        END AS location_string,
        'city' AS location_type
    FROM rpt.transaction_search
    INNER JOIN
        state_data sd ON recipient_location_state_name = UPPER(sd.name)
    WHERE
        recipient_location_country_name IS NOT NULL
        AND
        recipient_location_city_name NOT LIKE '%,%'
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
                CONCAT(UPPER(pop_city_name), ', ', UPPER(sd.name))
        END AS location_string,
        'city' AS location_type
    FROM rpt.transaction_search
    INNER JOIN
        state_data sd ON pop_state_name = UPPER(sd.name)
    WHERE
        pop_country_name IS NOT NULL
        AND
        pop_city_name NOT LIKE '%,%'
),
select_cte AS (
    SELECT
        location_string,
        location_type,
        NULL AS county_fips
    FROM
        country_cte
    GROUP BY
        location_string,
        location_type
    UNION
    SELECT
        location_string,
        location_type,
        county_fips
    FROM
        county_cte
    GROUP BY
        location_string,
        location_type,
        county_fips
    UNION
    SELECT
        location_string,
        location_type,
        NULL AS county_fips
    FROM
        zip_cte
    GROUP BY
        location_string,
        location_type
    UNION
    SELECT
        location_string,
        location_type,
        NULL AS county_fips
    FROM
        current_cd_cte
    GROUP BY
        location_string,
        location_type
    UNION
    SELECT
        location_string,
        location_type,
        NULL AS county_fips
    FROM
        original_cd_cte
    GROUP BY
        location_string,
        location_type
    UNION
    SELECT
        location_string,
        location_type,
        NULL AS county_fips
    FROM
        city_cte
    GROUP BY
        location_string,
        location_type
    UNION
    SELECT
        location_string,
        location_type,
        NULL AS county_fips
    FROM
        state_cte
    GROUP BY
        location_string,
        location_type
)
SELECT
    ROW_NUMBER() OVER (ORDER BY location_string, location_type) AS id,
    location_string,
    location_type,
    county_fips
FROM
    select_cte
GROUP BY
    location_string,
    location_type,
    county_fips