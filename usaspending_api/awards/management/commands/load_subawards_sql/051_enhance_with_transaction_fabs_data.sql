-- Enhance subawards with transactio FABS data.

update
    temp_load_subawards_subaward

set
    place_of_perform_scope = fabs.place_of_performance_scope

from
    transaction_fabs fabs

where
    fabs.transaction_id = temp_load_subawards_subaward.latest_transaction_id;
