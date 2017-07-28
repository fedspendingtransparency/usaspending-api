-- All table modifications in the load_*.sql files should be moved into this
-- script and run first, or the
-- `cannot ALTER TABLE "references_location" because it has pending trigger events`
-- error will result:
-- https://stackoverflow.com/questions/12838111/south-cannot-alter-table-because-it-has-pending-trigger-events
CREATE UNIQUE INDEX references_location_uniq_nullable_idx ON references_location
(
  coalesce(location_country_code, ''),
  coalesce(country_name, ''),
  coalesce(state_code, ''),
  coalesce(state_name, ''),
  coalesce(state_description, ''),
  coalesce(city_name, ''),
  coalesce(city_code, ''),
  coalesce(county_name, ''),
  coalesce(county_code, ''),
  coalesce(address_line1, ''),
  coalesce(address_line2, ''),
  coalesce(address_line3, ''),
  coalesce(foreign_location_description, ''),
  coalesce(zip4, ''),
  coalesce(congressional_code, ''),
  coalesce(performance_code, ''),
  coalesce(zip_last4, ''),
  coalesce(zip5, ''),
  coalesce(foreign_postal_code, ''),
  coalesce(foreign_province, ''),
  coalesce(foreign_city_name, ''),
  coalesce(reporting_period_start, '1900-01-01'::DATE),
  coalesce(reporting_period_end, '1900-01-01'::DATE),
  recipient_flag,
  place_of_performance_flag
);



ALTER TABLE references_location ADD COLUMN award_financial_assistance_ids INTEGER[];


ALTER TABLE references_location ADD COLUMN place_of_performance_award_financial_assistance_ids INTEGER[];


drop index if exists legal_entity_unique_nullable;


create unique index if not exists legal_entity_unique_nullable on legal_entity (
      COALESCE(data_source, ''),
      COALESCE(parent_recipient_unique_id, ''),
      COALESCE(recipient_name, ''),
      COALESCE(vendor_doing_as_business_name, ''),
      COALESCE(vendor_phone_number, ''),
      COALESCE(vendor_fax_number, ''),
      COALESCE(business_types, ''),
      COALESCE(business_types_description, ''),
      COALESCE(recipient_unique_id, ''),
      COALESCE(domestic_or_foreign_entity_description, ''),
      COALESCE(division_name, ''),
      COALESCE(division_number, ''),
      COALESCE(city_township_government, ''),
      COALESCE(special_district_government, ''),
      COALESCE(small_business, ''),
      COALESCE(small_business_description, ''),
      COALESCE(individual, ''),
      COALESCE(location_id, -1)
    );



ALTER TABLE legal_entity ADD COLUMN award_financial_assistance_ids INTEGER[];


drop index if exists awards_unique_nullable;


create unique index if not exists awards_unique_nullable on awards (
  COALESCE(data_source, ''),
  COALESCE(type, ''),
  COALESCE(type_description, ''),
  COALESCE(piid, ''),
  COALESCE(fain, ''),
  COALESCE(uri, ''),
  COALESCE(total_obligation, -1),
  COALESCE(total_outlay, -1),
  COALESCE(date_signed, '1900-01-01'::DATE),
  COALESCE(description, ''),
  COALESCE(period_of_performance_start_date, '1900-01-01'::DATE),
  COALESCE(period_of_performance_current_end_date, '1900-01-01'::DATE),
  COALESCE(potential_total_value_of_award, -1),
  COALESCE(last_modified_date, '1900-01-01'::DATE),
  COALESCE(certified_date, '1900-01-01'::DATE),
  -- COALESCE(create_date, '1900-01-01'::DATE),
  -- COALESCE(update_date, '1900-01-01'::DATE),
  COALESCE(total_subaward_amount, -1),
  COALESCE(subaward_count, -1),
  COALESCE(awarding_agency_id, -1),
  COALESCE(funding_agency_id, -1),
  COALESCE(latest_transaction_id, -1),
  COALESCE(parent_award_id, -1),
  COALESCE(place_of_performance_id, -1),
  COALESCE(recipient_id, -1),
  COALESCE(category, '')
);


ALTER TABLE awards ADD COLUMN award_financial_assistance_ids INTEGER[];


ALTER TABLE transaction ADD COLUMN award_financial_assistance_id INTEGER;


ALTER TABLE transaction ADD COLUMN award_procurement_id INTEGER;


ALTER TABLE references_location ADD COLUMN award_procurement_ids INTEGER[];


ALTER TABLE references_location ADD COLUMN place_of_performance_award_procurement_ids INTEGER[];


ALTER TABLE legal_entity ADD COLUMN award_procurement_ids INTEGER[];


ALTER TABLE awards ADD COLUMN award_procurement_ids INTEGER[];
