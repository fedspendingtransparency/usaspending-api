CREATE TABLE public.fabs_transactions_to_update AS
SELECT * from dblink_fetch('broker', %(batch)s)
AS (
		published_award_financial_assistance_id  text,
        afa_generated_unique  text,
        action_date  text,
        action_type  text,
        assistance_type  text,
        award_description  text,
        awardee_or_recipient_legal  text,
        awardee_or_recipient_uniqu  text,
        awarding_agency_code  text,
        awarding_agency_name  text,
        awarding_office_code  text,
        awarding_office_name  text,
        awarding_sub_tier_agency_c  text,
        awarding_sub_tier_agency_n  text,
        award_modification_amendme  text,
        business_funds_indicator  text,
        business_types  text,
        cfda_number  text,
        cfda_title  text,
        correction_late_delete_ind  text,
        face_value_loan_guarantee  decimal,
        fain  text,
        federal_action_obligation  decimal,
        fiscal_year_and_quarter_co  text,
        funding_agency_code  text,
        funding_agency_name  text,
        funding_office_code  text,
        funding_office_name  text,
        funding_sub_tier_agency_co  text,
        funding_sub_tier_agency_na  text,
        is_active  boolean,
        is_historical  boolean,
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
        modified_at  timestamp,
        non_federal_funding_amount  decimal,
        original_loan_subsidy_cost  decimal,
        period_of_performance_curr  text,
        period_of_performance_star  text,
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
        record_type  integer,
        sai_number  text,
        total_funding_amount  text,
        uri  text,
        created_at timestamp,
        updated_at timestamp
      )
       EXCEPT
      	SELECT
      	published_award_financial_assistance_id ,
		afa_generated_unique ,
		action_date ,
		action_type ,
		assistance_type ,
		award_description ,
		awardee_or_recipient_legal ,
		awardee_or_recipient_uniqu ,
		awarding_agency_code ,
		awarding_agency_name ,
		awarding_office_code ,
		awarding_office_name ,
		awarding_sub_tier_agency_c ,
		awarding_sub_tier_agency_n ,
		award_modification_amendme ,
		business_funds_indicator ,
		business_types ,
		cfda_number ,
		cfda_title ,
		correction_late_delete_ind ,
		face_value_loan_guarantee ,
		fain ,
		federal_action_obligation ,
		fiscal_year_and_quarter_co ,
		funding_agency_code ,
		funding_agency_name ,
		funding_office_code ,
		funding_office_name ,
		funding_sub_tier_agency_co ,
		funding_sub_tier_agency_na ,
		is_active ,
		is_historical ,
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
		modified_at ,
		non_federal_funding_amount ,
		original_loan_subsidy_cost ,
		period_of_performance_curr ,
		period_of_performance_star ,
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
		record_type ,
		sai_number ,
		total_funding_amount ,
		uri ,
		created_at,
		updated_at
        from transaction_fabs
        where updated_at::date = %(updated_date)s;

ALTER TABLE fabs_transactions_to_update
add COLUMN pop_change boolean, add COLUMN le_loc_change boolean, add COLUMN trans_change boolean, add COLUMN le_change boolean;


update fabs_transactions_to_update
SET pop_change = (
	CASE fabs_transactions_to_update.pop_change WHEN
			transaction_fabs.place_of_performance_city != tmp_fabs.place_of_performance_city or
			transaction_fabs.place_of_performance_city != tmp_fabs.place_of_performance_city or
			transaction_fabs.place_of_performance_code != tmp_fabs.place_of_performance_code or
			transaction_fabs.place_of_performance_congr != tmp_fabs.place_of_performance_congr or
			transaction_fabs.place_of_perform_country_c != tmp_fabs.place_of_perform_country_c or
			transaction_fabs.place_of_perform_country_n != tmp_fabs.place_of_perform_country_n or
			transaction_fabs.place_of_perform_county_co != tmp_fabs.place_of_perform_county_co or
			transaction_fabs.place_of_perform_county_na != tmp_fabs.place_of_perform_county_na or
			transaction_fabs.place_of_performance_forei != tmp_fabs.place_of_performance_forei or
			transaction_fabs.place_of_perform_state_nam != tmp_fabs.place_of_perform_state_nam or
			transaction_fabs.place_of_performance_zip4a != tmp_fabs.place_of_performance_zip4a
		THEN True ELSE FALSE END
	)
    le_loc_change = (
	CASE fabs_transactions_to_update.le_loc_change WHEN
		transaction_fabs.legal_entity_address_line1 != tmp_fabs.legal_entity_address_line1 or
		transaction_fabs.legal_entity_address_line2 != tmp_fabs.legal_entity_address_line2 or
		transaction_fabs.legal_entity_address_line3 != tmp_fabs.legal_entity_address_line3 or
		transaction_fabs.legal_entity_city_name != tmp_fabs.legal_entity_city_name or
		transaction_fabs.legal_entity_city_code != tmp_fabs.legal_entity_city_code or
		transaction_fabs.legal_entity_congressional != tmp_fabs.legal_entity_congressional or
		transaction_fabs.legal_entity_country_code != tmp_fabs.legal_entity_country_code or
		transaction_fabs.legal_entity_country_name != tmp_fabs.legal_entity_country_name or
		transaction_fabs.legal_entity_county_code != tmp_fabs.legal_entity_county_code or
		transaction_fabs.legal_entity_county_name != tmp_fabs.legal_entity_county_name or
		transaction_fabs.legal_entity_foreign_city != tmp_fabs.legal_entity_foreign_city or
		transaction_fabs.legal_entity_foreign_posta != tmp_fabs.legal_entity_foreign_posta or
		transaction_fabs.legal_entity_foreign_provi != tmp_fabs.legal_entity_foreign_provi or
		transaction_fabs.legal_entity_state_code != tmp_fabs.legal_entity_state_code or
		transaction_fabs.legal_entity_state_name != tmp_fabs.legal_entity_state_name or
		transaction_fabs.legal_entity_zip5 != tmp_fabs.legal_entity_zip5 or
		transaction_fabs.legal_entity_zip_last4 != tmp_fabs.legal_entity_zip_last4
		THEN True ELSE FALSE END
	)
    trans_change = (
	CASE fabs_transactions_to_update.trans_change WHEN
	    transaction_fabs.period_of_performance_curr != tmp_fabs.period_of_performance_curr or
        transaction_fabs.period_of_performance_curr != tmp_fabs.period_of_performance_star or
		transaction_fabs.assistance_type != tmp_fabs.assistance_type or
		transaction_fabs.action_type != tmp_fabs.action_type or
		transaction_fabs.award_description != tmp_fabs.award_description or
		transaction_fabs.modified_at != tmp_fabs.modified_at or
		transaction_fabs.create_date != tmp_fabs.create_date or
		transaction_fabs.action_date!= tmp_fabs.action_date or
		transaction_fabs.federal_action_obligation != tmp_fabs.federal_action_obligation
		THEN True ELSE FALSE END
	)
    le_change = (
	CASE fabs_transactions_to_update.le_change WHEN
	    transaction_fabs.awardee_or_recipient_legal = tmp_fabs.awardee_or_recipient_legal or
	    transaction_fabs.awardee_or_recipient_uniqu = tmp_fabs.awardee_or_recipient_uniqu
		THEN True ELSE FALSE END
	)
	from fabs_transactions_to_update as tmp_fabs
	inner join transaction_fabs
	on tmp_fabs.published_award_financial_assistance_id = transaction_fabs.published_award_financial_assistance_id;
