-- Enhance subawards with cfda data.



update
    temp_load_subawards_subaward

set
    cfda_id = cfda.id,
    cfda_number = cfda.program_number,
    cfda_title = cfda.program_title

from
    broker_subaward bs
    inner join references_cfda cfda on
        cfda.program_number = split_part(bs.cfda_numbers, ',', 1)

where
    temp_load_subawards_subaward.id = bs.id and
    bs.cfda_numbers is not null;
