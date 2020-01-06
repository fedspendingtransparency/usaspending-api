-- Enhance subawards with transaction FPDS data.



update
    temp_load_subawards_subaward

set
    extent_competed = fpds.extent_competed,
    product_or_service_code = fpds.product_or_service_code,
    product_or_service_description = psc.description,
    pulled_from = fpds.pulled_from,
    type_of_contract_pricing = fpds.type_of_contract_pricing,
    type_set_aside = fpds.type_set_aside

from
    transaction_fpds fpds
    left outer join psc on fpds.product_or_service_code = psc.code

where
    fpds.transaction_id = temp_load_subawards_subaward.latest_transaction_id;
