-- Create temp table to help with address updates.



drop table if exists temp_load_subawards_address_lookup;



-- Create a temporary working table containing all of the address information
-- from ref_city_county_code that we'll need in subsequent steps.
-- This will be dropped in the cleanup.sql file.
create unlogged table
    temp_load_subawards_address_lookup as

select
    county_code, county_name, state_code, city_code, city_name

from (
        select
            row_number() over (partition by state_code, city_name order by update_date desc) as row_num,
            county_code, county_name, state_code, city_code, city_name

        from
            ref_city_county_code

        where
            coalesce(county_code, '') !=  '' and
            coalesce(county_name, '') !=  '' and
            coalesce(state_code, '') !=  '' and
            coalesce(city_code, '') !=  '' and
            coalesce(city_name, '') !=  ''
    ) t

where
    row_num = 1;



create index
    idx_temp_load_subawards_address_lookup
on
    temp_load_subawards_address_lookup (
        city_name, state_code, county_code, county_name, city_code
    );
