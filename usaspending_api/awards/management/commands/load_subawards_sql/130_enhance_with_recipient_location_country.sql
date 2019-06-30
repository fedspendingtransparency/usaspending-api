-- Add in recipient location country info.



update
    temp_load_subawards_subaward

set
    recipient_location_country_name = cc.country_name

from
    ref_country_code cc

where
    cc.country_code = temp_load_subawards_subaward.recipient_location_country_code and
    temp_load_subawards_subaward.pop_country_code is not null;
