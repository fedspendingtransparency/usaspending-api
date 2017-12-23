
-- Update recipient locations with data from broker

UPDATE references_location as loc
SET
    address_line1 = broker.legal_entity_address_line1,
    address_line2 = broker.legal_entity_address_line2,
    address_line3 = broker.legal_entity_address_line3,
    city_name = broker.legal_entity_city_name,
    city_code = broker.legal_entity_city_code,
    congressional_code = broker.legal_entity_congressional,
    location_country_code = broker.legal_entity_country_code,
    country_name = broker.legal_entity_country_name,
    county_code = broker.legal_entity_county_code,
    county_name = broker.legal_entity_county_name,
    foreign_city_name = broker.legal_entity_foreign_city,
    foreign_postal_code = broker.legal_entity_foreign_posta,
    foreign_province = broker.legal_entity_foreign_provi,
    state_code = broker.legal_entity_state_code,
    state_name = broker.legal_entity_state_name,
    zip5 = broker.legal_entity_zip5,
    zip_last4 = broker.legal_entity_zip_last4
FROM fabs_transactions_to_update_location broker,
	transaction_fabs,
	transaction_normalized,
	legal_entity
WHERE  broker.afa_generated_unique = transaction_fabs.afa_generated_unique
AND transaction_fabs.transaction_id = transaction_normalized.id
AND legal_entity.legal_entity_id = transaction_normalized.recipient_id
AND legal_entity.location_id = loc.location_id
AND broker.le_loc_change = TRUE;



-- Update place of performance locations with data from broker

UPDATE references_location as loc
SET
    city_name = broker.place_of_performance_city,
    performance_code = broker.place_of_performance_code,
    congressional_code = broker.place_of_performance_congr,
    location_country_code = broker.place_of_perform_country_c,
    country_name = broker.place_of_perform_country_n,
    county_code = broker.place_of_perform_county_co,
    county_name = broker.place_of_perform_county_na,
    foreign_location_description = broker.place_of_performance_forei,
    state_name = broker.place_of_perform_state_nam,
--    state_code = broker.place_of_perfor_state_code, TODO: Uncomment when fields added to broker
--    zip5 = broker.place_of_performance_zip5, TODO: Uncomment when fields added to broker
    zip4 = broker.place_of_performance_zip4a
--    zip_last4 = broker.place_of_perform_zip_last4 TODO: Uncomment when fields added to broker
FROM fabs_transactions_to_update_location broker,
    transaction_fabs,
    transaction_normalized
WHERE  broker.afa_generated_unique = transaction_fabs.afa_generated_unique
AND transaction_fabs.transaction_id = transaction_normalized.id
AND loc.location_id = transaction_normalized.place_of_performance_id
AND broker.pop_change = TRUE;
