DROP VIEW IF EXISTS location_delta_view;

CREATE VIEW location_delta_view AS
with locations_cte as (
	select
		case
			when
				pop_country_name = 'UNITED STATES OF AMERICA'
			then
				'UNITED STATES'
			else
				pop_country_name
		end as country_name,
		case
			when
				pop_country_name = 'UNITED STATES'
				and
				LENGTH(pop_state_name) > 2  -- filter out state codes that were inserted as state names
				and
				pop_state_name ~ '[^0-9]'  -- filter out fips codes that were inserted as state names
			then
				pop_state_name
			else
				null
		end as state_name,
		case
			when
				pop_country_name = 'UNITED STATES'
				and
				pop_state_name is not null
				and
				pop_city_name ~ '^[a-zA-z]'
			then
				pop_city_name
			else
				null
		end as city_name,
		case
			when
				pop_country_name = 'UNITED STATES'
				and
				pop_state_name is not null
				and
				pop_county_name ~ '^[a-zA-z]'
			then
				pop_county_name
			else
				null
		end as county_name,
		case
			when
				pop_country_name = 'UNITED STATES'
				and
				pop_state_name is not null
			then
				pop_zip5
			else
				null
		end as zip_code,
		case
			when
				pop_country_name = 'UNITED STATES'
				and
				(
					sd.code is not null
					and
					pop_congressional_code_current is not null
				)
			then
				CONCAT(sd.code, pop_congressional_code_current)
			else
				null
		end as current_congressional_district,
		case
			when
				pop_country_name = 'UNITED STATES'
				and
				(
					sd.code is not null
					and
					pop_congressional_code is not null
				)
			then
				CONCAT(sd.code, pop_congressional_code)
			else
				null
		end as original_congressional_district
	from	
		rpt.transaction_search
	left join
		state_data sd on pop_state_name = upper(sd.name)
	where
		pop_country_name is not null
	union
	select
		case
			when
				recipient_location_country_name = 'UNITED STATES OF AMERICA'
			then
				'UNITED STATES'
			else
				recipient_location_country_name 
		end as country_name,
		case
			when
				recipient_location_country_name = 'UNITED STATES'
				and
				LENGTH(recipient_location_state_name) > 2
				and
				recipient_location_state_name ~ '^[a-zA-z]'
			then
				recipient_location_state_name
			else
				null 
		end as state_name,
		case
			when
				recipient_location_country_name = 'UNITED STATES'
				and
				recipient_location_state_name is not null
				and
				recipient_location_city_name ~ '^[a-zA-z]'
			then
				recipient_location_city_name
			else
				null
		end as city_name,
		case
			when
				recipient_location_country_name = 'UNITED STATES'
				and
				recipient_location_state_name is not null
				and
				recipient_location_county_name ~ '^[a-zA-z]'
			then
				recipient_location_county_name
			else
				null
		end as county_name,
		case
			when
				recipient_location_country_name = 'UNITED STATES'
				and
				recipient_location_state_name is not null
			then
				recipient_location_zip5
			else
				null
		end as zip_code,
		case
			when
				recipient_location_country_name = 'UNITED STATES'
				and
				(
					sd.code is not null
					and
					recipient_location_congressional_code_current is not null
				)
			then
				CONCAT(sd.code, recipient_location_congressional_code_current)
			else
				null
		end as current_congressional_district,
		case
			when
				recipient_location_country_name = 'UNITED STATES'
				and
				(
					sd.code is not null
					and
					recipient_location_congressional_code is not null
				)
			then
				CONCAT(sd.code, recipient_location_congressional_code)
			else
				null
		end as original_congressional_district
	from
		rpt.transaction_search
	left join
		state_data sd on recipient_location_state_name = upper(sd.name)
	where
		recipient_location_country_name is not null
)
select
    row_number() over() as id,
	country_name,
	state_name,
	array_agg(distinct(city_name)) filter (where city_name is not null) as cities,
	array_agg(distinct(county_name)) filter (where county_name is not null) as counties,
	array_agg(distinct(zip_code)) filter (where zip_code is not null) as zip_codes,
	array_agg(distinct(current_congressional_district)) filter (where current_congressional_district is not null) as current_congressional_districts,
	array_agg(distinct(original_congressional_district)) filter (where original_congressional_district is not null) as original_congressional_districts
from
	locations_cte
where
	-- require state name for UNITED STATES
	(
		country_name = 'UNITED STATES'
		and
		state_name is not null
	)
	or
	-- only need country name for foreign countries since we don't support foreign "states"
	(
		country_name != 'UNITED STATES'
		and
		state_name is null
	)
group by
	country_name,
	state_name
