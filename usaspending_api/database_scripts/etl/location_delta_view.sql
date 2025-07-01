DROP VIEW IF EXISTS location_delta_view;

CREATE VIEW location_delta_view AS
-- Country
WITH
    country_cte AS (
        SELECT
            UPPER(country_name) AS location,
            TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'country_name', country_name,
                    'location_type', 'country'
                )
            ) AS location_json
        FROM
            ref_country_code
    ),
    -- State
    state_cte AS (
        SELECT
            CONCAT(UPPER(name), ', ', 'UNITED STATES') AS location,
            TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'state_name', UPPER(name),
                    'country_name', 'UNITED STATES',
                    'location_type', 'state'
                )
            ) AS location_json
        FROM
            state_data
    ),
    -- City (domestic)
    city_domestic AS (
        SELECT
            CONCAT(UPPER(ref_city.feature_name), ', ', UPPER(ref_state.name), ', ', 'UNITED STATES') AS location,
            TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'city_name', UPPER(ref_city.feature_name),
                    'state_name', UPPER(ref_state.name),
                    'country_name', 'UNITED STATES',
                    'location_type', 'city'
                )
            ) AS location_json
        FROM
            ref_city_county_state_code AS ref_city
        JOIN
            state_data AS ref_state ON ref_state.code = ref_city.state_alpha
    ),
    -- City (foreign)
    city_foreign_pop_cte AS (
        SELECT
            CONCAT(UPPER(pop_city_name), ', ', rcc.country_name) AS location,
            TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'city_name', UPPER(pop_city_name),
                    'state_name', NULL,
                    'country_name', rcc.country_name,
                    'location_type', 'city'
                )
            ) AS location_json
        FROM
            rpt.transaction_search
        JOIN
            ref_country_code AS rcc ON pop_country_code = rcc.country_code
        WHERE
            rcc.country_name NOT IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
            AND
            rcc.country_name IS NOT NULL
            AND
            pop_city_name IS NOT NULL
    ),
    city_foreign_rl_cte AS (
        SELECT
            CONCAT(UPPER(recipient_location_city_name), ', ', rcc.country_name) AS location,
            TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'city_name', UPPER(recipient_location_city_name),
                    'state_name', NULL,
                    'country_name', rcc.country_name,
                    'location_type', 'city'
                )
            ) AS location_json
        FROM
            rpt.transaction_search
        JOIN ref_country_code AS rcc ON
            recipient_location_country_code = rcc.country_code
        WHERE
            rcc.country_name NOT IN ('UNITED STATES OF AMERICA', 'UNITED STATES')
            AND
            rcc.country_name IS NOT NULL
            AND
            recipient_location_city_name IS NOT NULL
    ),
    -- County
    county_cte AS (
        SELECT
            CONCAT(UPPER(ref_county.county_name), ' COUNTY, ', UPPER(sd.name), ', ', 'UNITED STATES') AS location,
            TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'county_name', UPPER(ref_county.county_name),
                    'state_name', UPPER(sd.name),
                    'country_name', 'UNITED STATES',
                    'location_type', 'county'
                )
            ) AS location_json
        FROM
            ref_city_county_state_code AS ref_county
        JOIN
            state_data AS sd ON sd.code = ref_county.state_alpha
    ),
    -- Zip code
    zip_cte AS (
        SELECT
            CONCAT(zips.zip5, ', ', UPPER(sd.name), ', ', 'UNITED STATES') AS location,
            TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'zip_code', zips.zip5,
                    'state_name', UPPER(sd.name),
                    'country_name', 'UNITED STATES',
                    'location_type', 'zip_code'
                )
            ) AS location_json
        FROM
            zips_grouped AS zips
        JOIN
            state_data AS sd ON sd.code = zips.state_abbreviation
    ),
    -- Current Congressional district
    current_cd_pop_cte AS (
        SELECT
            CONCAT(UPPER(pop_state_code), pop_congressional_code_current) AS location,
            TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'current_cd', CONCAT(UPPER(pop_state_code), '-', pop_congressional_code_current),
                    'state_name', UPPER(sd.name),
                    'country_name', 'UNITED STATES',
                    'location_type', 'current_cd'
                )
            ) AS location_json
        FROM
            rpt.transaction_search
        RIGHT JOIN
            state_data AS sd ON sd.code = pop_state_code
        WHERE
            pop_state_code IS NOT NULL
            AND
            pop_congressional_code_current ~ '^[0-9]{2}$'
    ),
    current_cd_rl_cte AS (
        SELECT
            CONCAT(UPPER(recipient_location_state_code), recipient_location_congressional_code_current) AS location,
            TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'current_cd',
                    CONCAT(UPPER(recipient_location_state_code), '-', recipient_location_congressional_code_current),
                    'state_name', UPPER(sd.name),
                    'country_name', 'UNITED STATES',
                    'location_type', 'current_cd'
                )
            )
        FROM
            rpt.transaction_search
        RIGHT JOIN
            state_data AS sd ON sd.code = recipient_location_state_code
        WHERE
            recipient_location_state_code IS NOT NULL
            AND
            recipient_location_congressional_code_current ~ '^[0-9]{2}$'
    ),
    -- Original Congressional district
    original_cd_pop_cte AS (
        SELECT
            CONCAT(UPPER(pop_state_code), pop_congressional_code) AS location,
            TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'original_cd', CONCAT(UPPER(pop_state_code), '-', pop_congressional_code),
                    'state_name', UPPER(sd.name),
                    'country_name', 'UNITED STATES',
                    'location_type', 'original_cd'
                )
            )
        FROM
            rpt.transaction_search
        RIGHT JOIN
            state_data AS sd ON sd.code = pop_state_code
        WHERE
            pop_state_code IS NOT NULL
            AND
            pop_congressional_code ~ '^[0-9]{2}$'
    ),
    original_cd_rl_cte AS (
        SELECT
            CONCAT(UPPER(recipient_location_state_code), recipient_location_congressional_code) AS location,
            TO_JSONB(
                JSONB_BUILD_OBJECT(
                    'original_cd', CONCAT(UPPER(recipient_location_state_code), '-', recipient_location_congressional_code),
                    'state_name', UPPER(sd.name),
                    'country_name', 'UNITED STATES',
                    'location_type', 'original_cd'
                )
            )
        FROM
            rpt.transaction_search
        RIGHT JOIN
            state_data AS sd ON sd.code = recipient_location_state_code
        WHERE
            recipient_location_state_code IS NOT NULL
            AND
            recipient_location_congressional_code ~ '^[0-9]{2}$'
    )
SELECT
    ROW_NUMBER() OVER (ORDER BY location, location_json) AS id,
    location,
    location_json
FROM
    (
        SELECT * FROM country_cte
        UNION
        SELECT * FROM state_cte
        UNION
        SELECT * FROM city_domestic
        UNION
        SELECT * FROM city_foreign_pop_cte
        UNION
        SELECT * FROM city_foreign_rl_cte
        UNION
        SELECT * FROM county_cte
        UNION
        SELECT * FROM zip_cte
        UNION
        SELECT * FROM current_cd_pop_cte
        UNION
        SELECT * FROM current_cd_rl_cte
        UNION
        SELECT * FROM original_cd_pop_cte
        UNION
        SELECT * FROM original_cd_rl_cte
    ) AS union_all
