DROP VIEW IF EXISTS location_delta_view;

CREATE VIEW location_delta_view AS
WITH transaction_locations_cte AS (
    SELECT
	-- Country
    CASE
		WHEN pop_country_name = 'UNITED STATES OF AMERICA'
            THEN 'UNITED STATES'
		ELSE
            pop_country_name
	END AS pop_country_string,
	CASE
		WHEN pop_country_name = 'UNITED STATES OF AMERICA'
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'country_name', 'UNITED STATES'
                )
            )
		ELSE
            TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'country_name', pop_country_name,
                    'location_type', 'country'
                )
            )
	END AS pop_country_json,
	-- State
    CASE
		WHEN pop_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES') AND pop_state_name IS NOT NULL
            THEN CONCAT(UPPER(pop_state_name), ', ', 'UNITED STATES')
	END AS pop_state_string,
	CASE
		WHEN pop_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES') AND pop_state_name IS NOT NULL
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'state_name', UPPER(pop_state_name),
		            'country_name', 'UNITED STATES',
                    'location_type', 'state'
                )
            )
	END AS pop_state_json,
	-- City
    CASE
		WHEN pop_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES') AND pop_state_name IS NOT NULL AND pop_city_name IS NOT NULL
            THEN CONCAT(UPPER(pop_city_name), ', ',	UPPER(pop_state_name), ', ', 'UNITED STATES')
		WHEN pop_country_name NOT IN ('UNITED STATES OF AMERICA', 'UNITED STATES') AND pop_country_name IS NOT NULL	AND pop_city_name IS NOT NULL
            THEN concat(UPPER(pop_city_name), ', ',	UPPER(pop_country_name))
	END AS pop_city_string,
	CASE
		WHEN pop_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES') AND pop_state_name IS NOT NULL AND pop_city_name IS NOT NULL
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'city_name', UPPER(pop_city_name),
		            'state_name', UPPER(pop_state_name),
		            'country_name', 'UNITED STATES',
                    'location_type', 'city'
                )
            )
		WHEN pop_country_name NOT IN ('UNITED STATES OF AMERICA', 'UNITED STATES') AND pop_country_name IS NOT NULL AND pop_city_name IS NOT NULL
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'city_name', UPPER(pop_city_name),
		            'state_name', NULL,
		            'country_name', UPPER(pop_country_name),
                    'location_type', 'city'
                )
            )
	END AS pop_city_json,
	-- County
    CASE
		WHEN (
            pop_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND pop_state_name IS NOT NULL
		    AND pop_state_fips IS NOT NULL
		    AND pop_state_code IS NOT NULL
		    AND pop_county_name IS NOT NULL
        )
            THEN CONCAT(UPPER(pop_county_name), ' COUNTY, ', UPPER(pop_state_name), ', ', 'UNITED STATES')
	END AS pop_county_string,
	CASE
		WHEN (
            pop_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND pop_state_name IS NOT NULL
		    AND pop_state_fips IS NOT NULL
		    AND pop_state_code IS NOT NULL
		    AND pop_county_name IS NOT NULL
        )
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'county_name', UPPER(pop_county_name),
		            'county_fips', CONCAT(pop_state_fips, pop_county_code),
		            'state_name', UPPER(pop_state_name),
		            'country_name', 'UNITED STATES',
                    'location_type', 'county'
                )
            )
	END AS pop_county_json,
	-- Zip code
    CASE
		WHEN pop_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		AND pop_state_name IS NOT NULL
		AND pop_zip5 IS NOT NULL
            THEN pop_zip5
	END AS pop_zip_string,
	CASE
		WHEN (
            pop_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND pop_state_name IS NOT NULL
		    AND pop_zip5 IS NOT NULL
        )
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'zip_code', pop_zip5,
		            'state_name', UPPER(pop_state_name),
		            'country_name', 'UNITED STATES',
                    'location_type', 'zip_code'
                )
            )
	END AS pop_zip_json,
	-- Current Congressional district
    CASE
		WHEN (
            pop_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND pop_state_name IS NOT NULL
		    AND pop_congressional_code_current IS NOT NULL
		    AND pop_state_code IS NOT NULL
        )
            THEN CONCAT(UPPER(pop_state_code), pop_congressional_code_current)
	END AS pop_current_cd_string,
	CASE
		WHEN (
            pop_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND pop_state_name IS NOT NULL
		    AND pop_congressional_code_current IS NOT NULL
		    AND pop_state_code IS NOT NULL
        )
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'current_cd', CONCAT(UPPER(pop_state_code),	'-', pop_congressional_code_current),
		            'state_name', UPPER(pop_state_name),
		            'country_name', 'UNITED STATES',
                    'location_type', 'current_cd'
                )
            )
	END AS pop_current_cd_json,
	-- Original Congressional district
    CASE
		WHEN (
            pop_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
	    	AND pop_state_name IS NOT NULL
		    AND pop_congressional_code IS NOT NULL
		    AND pop_state_code IS NOT NULL
        )
            THEN CONCAT(UPPER(pop_state_code), pop_congressional_code)
	END AS pop_original_cd_string,
	CASE
		WHEN (
            pop_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND pop_state_name IS NOT NULL
		    AND pop_congressional_code IS NOT NULL
		    AND pop_state_code IS NOT NULL
        )
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'current_cd', CONCAT(UPPER(pop_state_code), '-', pop_congressional_code),
		            'state_name', UPPER(pop_state_name),
		            'country_name', 'UNITED STATES',
                    'location_type', 'original_cd'
                )
            )
	END AS pop_original_cd_json,
	CASE
		WHEN recipient_location_country_name = 'UNITED STATES OF AMERICA'
            THEN 'UNITED STATES'
		ELSE recipient_location_country_name
	END AS recipient_location_country_string,
	CASE
		WHEN recipient_location_country_name = 'UNITED STATES OF AMERICA'
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'country_name', 'UNITED STATES',
                    'location_type', 'country'
                )
            )
		ELSE
            TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'country_name', recipient_location_country_name,
                    'location_type', 'country'
                )
            )
	END AS recipient_location_country_json,
	-- State
    CASE
		WHEN recipient_location_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES') AND recipient_location_state_name IS NOT NULL
            THEN CONCAT(UPPER(recipient_location_state_name), ', ',	'UNITED STATES')
	END AS recipient_location_state_string,
	CASE
		WHEN recipient_location_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES') AND recipient_location_state_name IS NOT NULL
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'state_name', UPPER(recipient_location_state_name),
		            'country_name', 'UNITED STATES',
                    'location_type', 'state'
                )
            )
	END AS recipient_location_state_json,
	-- City
    CASE
		WHEN (
            recipient_location_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND recipient_location_state_name IS NOT NULL
		    AND recipient_location_city_name IS NOT NULL
        )
            THEN CONCAT(UPPER(recipient_location_city_name), ', ', UPPER(recipient_location_state_name), ', ', 'UNITED STATES')
		WHEN (
            recipient_location_country_name NOT IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND recipient_location_country_name IS NOT NULL
		    AND recipient_location_city_name IS NOT NULL
        )
            THEN concat(UPPER(recipient_location_city_name), ', ', UPPER(recipient_location_country_name))
	END AS recipient_location_city_string,
	CASE
		WHEN (
            recipient_location_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND recipient_location_state_name IS NOT NULL
		    AND recipient_location_city_name IS NOT NULL
        )
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'city_name', UPPER(recipient_location_city_name),
		            'state_name', UPPER(recipient_location_state_name),
	                'country_name', 'UNITED STATES',
                    'location_type', 'city'
                )
            )
		WHEN (
            recipient_location_country_name NOT IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND recipient_location_country_name IS NOT NULL
		    AND recipient_location_city_name IS NOT NULL
        )
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'city_name', UPPER(recipient_location_city_name),
		            'state_name', NULL,
		            'country_name', UPPER(recipient_location_country_name),
                    'location_type', 'city'
                )
            )
	END AS recipient_location_city_json,
	-- County
    CASE
		WHEN (
            recipient_location_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
	    	AND recipient_location_state_name IS NOT NULL
	    	AND recipient_location_state_fips IS NOT NULL
	    	AND recipient_location_state_code IS NOT NULL
	    	AND recipient_location_county_name IS NOT NULL
        )
            THEN CONCAT(UPPER(recipient_location_county_name), ' COUNTY, ', UPPER(recipient_location_state_name), ', ',	'UNITED STATES')
	END AS recipient_location_county_string,
	CASE
		WHEN (
            recipient_location_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND recipient_location_state_name IS NOT NULL
		    AND recipient_location_state_fips IS NOT NULL
		    AND recipient_location_state_code IS NOT NULL
		    AND recipient_location_county_name IS NOT NULL
        )
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'county_name', UPPER(recipient_location_county_name),
		            'county_fips', CONCAT(recipient_location_state_fips, recipient_location_county_code),
		            'state_name', UPPER(recipient_location_state_name),
		            'country_name', 'UNITED STATES',
                    'location_type', 'county'
                )
            )
	END AS recipient_location_county_json,
	-- Zip code
    CASE
		WHEN (
            recipient_location_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND recipient_location_state_name IS NOT NULL
		    AND recipient_location_zip5 IS NOT NULL
        )
            THEN recipient_location_zip5
	END AS recipient_location_zip_string,
	CASE
		WHEN (
            recipient_location_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND recipient_location_state_name IS NOT NULL
		    AND recipient_location_zip5 IS NOT NULL
        )
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'zip_code', recipient_location_zip5,
		            'state_name', UPPER(recipient_location_state_name),
		            'country_name', 'UNITED STATES',
                    'location_type', 'zip_code'
                )
            )
	END AS recipient_location_zip_json,
	-- Current Congressional district
    CASE
		WHEN (
            recipient_location_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND recipient_location_state_name IS NOT NULL
		    AND recipient_location_congressional_code_current IS NOT NULL
		    AND recipient_location_state_code IS NOT NULL
        )
            THEN CONCAT(UPPER(recipient_location_state_code), recipient_location_congressional_code_current)
	END AS recipient_location_current_cd_string,
	CASE
		WHEN (
            recipient_location_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND recipient_location_state_name IS NOT NULL
		    AND recipient_location_congressional_code_current IS NOT NULL
		    AND recipient_location_state_code IS NOT NULL
        )
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'current_cd', CONCAT(UPPER(recipient_location_state_code), '-',	recipient_location_congressional_code_current),
		            'state_name', UPPER(recipient_location_state_name),
		            'country_name', 'UNITED STATES',
                    'location_type', 'current_cd'
                )
            )
	END AS recipient_location_current_cd_json,
	-- Original Congressional district
    CASE
		WHEN (
            recipient_location_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND recipient_location_state_name IS NOT NULL
		    AND recipient_location_congressional_code IS NOT NULL
		    AND recipient_location_state_code IS NOT NULL
        )
            THEN CONCAT(UPPER(recipient_location_state_code), recipient_location_congressional_code)
	END AS recipient_location_original_cd_string,
	CASE
		WHEN (
            recipient_location_country_name IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
		    AND recipient_location_state_name IS NOT NULL
		    AND recipient_location_congressional_code IS NOT NULL
		    AND recipient_location_state_code IS NOT NULL
        )
            THEN TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'current_cd', CONCAT(UPPER(recipient_location_state_code), '-', recipient_location_congressional_code),
		            'state_name', UPPER(recipient_location_state_name),
		            'country_name', 'UNITED STATES',
                    'location_type', 'original_cd'
                )
            )
	END AS recipient_location_original_cd_json
FROM
	rpt.transaction_search
WHERE
	pop_country_name IS NOT NULL
	OR
    recipient_location_country_name IS NOT NULL
),
unnested_cte AS (
	SELECT
		/*
			!! THE ORDER OF THESE UNNEST STATEMENTS MATTER !!
			The column names in these arrays need to align so that the values appear in the correct rows.
		 */
		UNNEST(
			ARRAY[
				pop_country_string,
				pop_state_string,
				pop_city_string,
				pop_county_string,
				pop_zip_string,
				pop_current_cd_string,
				pop_original_cd_string,
				recipient_location_country_string,
				recipient_location_state_string,
				recipient_location_city_string,
				recipient_location_county_string,
				recipient_location_zip_string,
				recipient_location_current_cd_string,
				recipient_location_original_cd_string
			]
		) AS LOCATION,
		UNNEST(
			ARRAY[
				pop_country_json,
				pop_state_json,
				pop_city_json,
				pop_county_json,
				pop_zip_json,
				pop_current_cd_json,
				pop_original_cd_json,
				recipient_location_country_json,
				recipient_location_state_json,
				recipient_location_city_json,
				recipient_location_county_json,
				recipient_location_zip_json,
				recipient_location_current_cd_json,
				recipient_location_original_cd_json
			]
		) AS location_json
	FROM
		transaction_locations_cte
)
SELECT
    ROW_NUMBER() OVER(ORDER BY location_json) AS id,
	location,
	location_json
FROM
	unnested_cte
WHERE
    -- Only include locations that have at least two characters
    location ~ '[A-Z0-9].*[A-Z0-9]'
    AND
    location_json IS NOT NULL
GROUP BY
    location,
    location_json