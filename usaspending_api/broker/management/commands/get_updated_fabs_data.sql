
--Create table in website database to select rows in transaction_fabs need to be updated

CREATE TABLE public.fabs_transactions_to_update AS
SELECT * from dblink('broker_server','
    SELECT
        published_award_financial_assistance_id ,
		afa_generated_unique ,
		action_date ,
		legal_entity_address_line1 ,
		legal_entity_address_line2 ,
		legal_entity_address_line3 ,
		legal_entity_city_name ,
		legal_entity_city_code ,
		legal_entity_congressional ,
		legal_entity_country_code ,
		legal_entity_country_name ,
		legal_entity_county_code ,
		legal_entity_county_name ,
		legal_entity_foreign_city ,
		legal_entity_foreign_posta ,
		legal_entity_foreign_provi ,
		legal_entity_state_code ,
		legal_entity_state_name ,
		legal_entity_zip5 ,
		legal_entity_zip_last4 ,
		place_of_performance_city ,
		place_of_performance_code ,
		place_of_performance_congr ,
		place_of_perform_country_c ,
		place_of_perform_country_n ,
		place_of_perform_county_co ,
		place_of_perform_county_na ,
		place_of_performance_forei ,
		place_of_perform_state_nam ,
		place_of_performance_zip4a ,
		updated_at
        from published_award_financial_assistance
        where is_active = TRUE and action_date::date >= '%(fy_start)s'::date and
        action_date::date <= '%(fy_end)s'::date;
')
AS (
		published_award_financial_assistance_id  text,
        afa_generated_unique  text,
        action_date  text,
        legal_entity_address_line1  text,
        legal_entity_address_line2  text,
        legal_entity_address_line3  text,
        legal_entity_city_name  text,
        legal_entity_city_code  text,
        legal_entity_congressional  text,
        legal_entity_country_code  text,
        legal_entity_country_name  text,
        legal_entity_county_code  text,
        legal_entity_county_name  text,
        legal_entity_foreign_city  text,
        legal_entity_foreign_posta  text,
        legal_entity_foreign_provi  text,
        legal_entity_state_code  text,
        legal_entity_state_name  text,
        legal_entity_zip5  text,
        legal_entity_zip_last4  text,
        place_of_performance_city  text,
        place_of_performance_code  text,
        place_of_performance_congr  text,
        place_of_perform_country_c  text,
        place_of_perform_country_n  text,
        place_of_perform_county_co  text,
        place_of_perform_county_na  text,
        place_of_performance_forei  text,
        place_of_perform_state_nam  text,
        place_of_performance_zip4a  text,
        updated_at timestamp
      )
       EXCEPT
      	SELECT
      	published_award_financial_assistance_id ,
		afa_generated_unique ,
		replace(action_date, '-', ''),
		legal_entity_address_line1 ,
		legal_entity_address_line2 ,
		legal_entity_address_line3 ,
		legal_entity_city_name ,
		legal_entity_city_code ,
		legal_entity_congressional ,
		legal_entity_country_code ,
		legal_entity_country_name ,
		legal_entity_county_code ,
		legal_entity_county_name ,
		legal_entity_foreign_city ,
		legal_entity_foreign_posta ,
		legal_entity_foreign_provi ,
		legal_entity_state_code ,
		legal_entity_state_name ,
		legal_entity_zip5 ,
		legal_entity_zip_last4 ,
		place_of_performance_city ,
		place_of_performance_code ,
		place_of_performance_congr ,
		place_of_perform_country_c ,
		place_of_perform_country_n ,
		place_of_perform_county_co ,
		place_of_perform_county_na ,
		place_of_performance_forei ,
		place_of_perform_state_nam ,
		place_of_performance_zip4a ,
		updated_at
        from transaction_fabs
        where action_date::date >= %(fy_start)s::date and
        action_date::date <= %(fy_end)s::date;

-- Include columns to determine whether we need a place of performance change or recipient location
ALTER TABLE fabs_transactions_to_update
add COLUMN pop_change boolean, add COLUMN le_loc_change boolean;

update fabs_transactions_to_update tmp_fabs
SET pop_change = (
	CASE  WHEN
			transaction_fabs.place_of_performance_city IS DISTINCT FROM tmp_fabs.place_of_performance_city or
			transaction_fabs.place_of_performance_city IS DISTINCT FROM tmp_fabs.place_of_performance_city or
			transaction_fabs.place_of_performance_code IS DISTINCT FROM tmp_fabs.place_of_performance_code or
			transaction_fabs.place_of_performance_congr IS DISTINCT FROM tmp_fabs.place_of_performance_congr or
			transaction_fabs.place_of_perform_country_c IS DISTINCT FROM tmp_fabs.place_of_perform_country_c or
			transaction_fabs.place_of_perform_country_n IS DISTINCT FROM tmp_fabs.place_of_perform_country_n or
			transaction_fabs.place_of_perform_county_co IS DISTINCT FROM tmp_fabs.place_of_perform_county_co or
			transaction_fabs.place_of_perform_county_na IS DISTINCT FROM tmp_fabs.place_of_perform_county_na or
			transaction_fabs.place_of_performance_forei IS DISTINCT FROM tmp_fabs.place_of_performance_forei or
			transaction_fabs.place_of_perform_state_nam IS DISTINCT FROM tmp_fabs.place_of_perform_state_nam or
			transaction_fabs.place_of_performance_zip4a IS DISTINCT FROM tmp_fabs.place_of_performance_zip4a
		THEN TRUE ELSE FALSE END
	),
    le_loc_change = (
	CASE  WHEN
		transaction_fabs.legal_entity_address_line1 IS DISTINCT FROM tmp_fabs.legal_entity_address_line1 or
		transaction_fabs.legal_entity_address_line2 IS DISTINCT FROM tmp_fabs.legal_entity_address_line2 or
		transaction_fabs.legal_entity_address_line3 IS DISTINCT FROM tmp_fabs.legal_entity_address_line3 or
		transaction_fabs.legal_entity_city_name IS DISTINCT FROM tmp_fabs.legal_entity_city_name or
		transaction_fabs.legal_entity_city_code IS DISTINCT FROM tmp_fabs.legal_entity_city_code or
		transaction_fabs.legal_entity_congressional IS DISTINCT FROM tmp_fabs.legal_entity_congressional or
		transaction_fabs.legal_entity_country_code IS DISTINCT FROM tmp_fabs.legal_entity_country_code or
		transaction_fabs.legal_entity_country_name IS DISTINCT FROM tmp_fabs.legal_entity_country_name or
		transaction_fabs.legal_entity_county_code IS DISTINCT FROM tmp_fabs.legal_entity_county_code or
		transaction_fabs.legal_entity_county_name IS DISTINCT FROM tmp_fabs.legal_entity_county_name or
		transaction_fabs.legal_entity_foreign_city IS DISTINCT FROM tmp_fabs.legal_entity_foreign_city or
		transaction_fabs.legal_entity_foreign_posta IS DISTINCT FROM tmp_fabs.legal_entity_foreign_posta or
		transaction_fabs.legal_entity_foreign_provi IS DISTINCT FROM tmp_fabs.legal_entity_foreign_provi or
		transaction_fabs.legal_entity_state_code IS DISTINCT FROM tmp_fabs.legal_entity_state_code or
		transaction_fabs.legal_entity_state_name IS DISTINCT FROM tmp_fabs.legal_entity_state_name or
		transaction_fabs.legal_entity_zip5 IS DISTINCT FROM tmp_fabs.legal_entity_zip5 or
		transaction_fabs.legal_entity_zip_last4 IS DISTINCT FROM tmp_fabs.legal_entity_zip_last4
		THEN TRUE ELSE FALSE END
	)
	FROM transaction_fabs
	WHERE tmp_fabs.published_award_financial_assistance_id = transaction_fabs.published_award_financial_assistance_id;

-- Delete rows where there is no transaction in the table
DELETE FROM fabs_transactions_to_update where pop_change is null and le_loc_change is null;