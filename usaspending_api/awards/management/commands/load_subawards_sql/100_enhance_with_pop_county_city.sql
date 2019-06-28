-- Add place of performance county and city info.



update
    temp_load_subawards_subaward

set
    pop_county_code = ccl.county_code,
    pop_county_name = ccl.county_name,
    pop_city_code = ccl.city_code

from
    temp_load_subawards_address_lookup ccl

where
    ccl.state_code = temp_load_subawards_subaward.pop_state_code and
    ccl.city_name = temp_load_subawards_subaward.pop_city_name and
    temp_load_subawards_subaward.pop_state_code is not null and
    temp_load_subawards_subaward.pop_city_name is not null;
