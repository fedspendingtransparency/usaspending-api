-- Update transaction_fabs with data from broker

UPDATE transaction_fabs as website
SET 
    legal_entity_address_line1 = broker.legal_entity_address_line1,
    legal_entity_address_line2 = broker.legal_entity_address_line2,
    legal_entity_address_line3 = broker.legal_entity_address_line3,
    legal_entity_city_name = broker.legal_entity_city_name,
    legal_entity_city_code = broker.legal_entity_city_code,
    legal_entity_congressional = broker.legal_entity_congressional,
    legal_entity_country_code = broker.legal_entity_country_code,
    legal_entity_country_name = broker.legal_entity_country_name,
    legal_entity_county_code = broker.legal_entity_county_code,
    legal_entity_county_name = broker.legal_entity_county_name,
    legal_entity_foreign_city = broker.legal_entity_foreign_city,
    legal_entity_foreign_posta = broker.legal_entity_foreign_posta,
    legal_entity_foreign_provi = broker.legal_entity_foreign_provi,
    legal_entity_state_code = broker.legal_entity_state_code,
    legal_entity_state_name = broker.legal_entity_state_name,
    legal_entity_zip5 = broker.legal_entity_zip5,
    legal_entity_zip_last4 = broker.legal_entity_zip_last4,
    place_of_performance_city = broker.place_of_performance_city,
    place_of_performance_code = broker.place_of_performance_code,
    place_of_performance_congr = broker.place_of_performance_congr,
    place_of_perform_country_c = broker.place_of_perform_country_c,
    place_of_perform_country_n = broker.place_of_perform_country_n,
    place_of_perform_county_co = broker.place_of_perform_county_co,
    place_of_perform_county_na = broker.place_of_perform_county_na,
    place_of_performance_forei = broker.place_of_performance_forei,
    place_of_perform_state_nam = broker.place_of_perform_state_nam,
--    place_of_performance_zip5 = broker.place_of_performance_zip5, TODO: Uncomment when fields added to broker
    place_of_performance_zip4a = broker.place_of_performance_zip4a
FROM
    fabs_transactions_to_update as broker
WHERE
    broker.afa_generated_unique = website.afa_generated_unique;



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
FROM fabs_transactions_to_update broker,
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
--    zip5 = broker.place_of_performance_zip5, TODO: Uncomment when fields added to broker
    zip4 = broker.place_of_performance_zip4a
FROM fabs_transactions_to_update broker,
    transaction_fabs,
    transaction_normalized
WHERE  broker.afa_generated_unique = transaction_fabs.afa_generated_unique
AND transaction_fabs.transaction_id = transaction_normalized.id
AND loc.location_id = transaction_normalized.place_of_performance_id
AND broker.pop_change = TRUE;
