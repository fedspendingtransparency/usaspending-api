-- We're going to frame out subawards.  This is everything we can populate
-- using Broker data.  We will progressively enhance using other SQLs in
-- this directory.



drop table if exists temp_load_subawards_subaward;



create temporary table temp_load_subawards_subaward as
    select * from subaward where 0 = 1;



insert into temp_load_subawards_subaward (
    id,
    subaward_number,
    amount,
    description,
    recovery_model_question1,
    recovery_model_question2,
    action_date,
    award_report_fy_month,
    award_report_fy_year,
    award_type,
    broker_award_id,
    internal_id,
    fain,
    parent_recipient_unique_id,
    parent_recipient_uei,
    piid,
    pop_congressional_code,
    pop_country_code,
    pop_state_code,
    pop_zip4,
    business_categories,
    prime_recipient_name,
    recipient_location_congressional_code,
    recipient_location_country_code,
    recipient_location_state_code,
    recipient_location_zip5,
    recipient_name,
    recipient_unique_id,
    recipient_uei,
    pop_city_name,
    pop_state_name,
    pop_street_address,
    recipient_location_city_name,
    dba_name,
    parent_recipient_name,
    business_type_description,
    officer_1_amount,
    officer_1_name,
    officer_2_amount,
    officer_2_name,
    officer_3_amount,
    officer_3_name,
    officer_4_amount,
    officer_4_name,
    officer_5_amount,
    officer_5_name,
    recipient_location_foreign_postal_code,
    recipient_location_state_name,
    recipient_location_street_address,
    recipient_location_zip4,
    updated_at,
    unique_award_key
)

select
    bs.id,                                                  -- id
    upper(subaward_number),                                 -- subaward_number
    subaward_amount,                                        -- amount
    upper(subaward_description),                            -- description
    sub_recovery_model_q1,                                  -- recovery_model_question1
    sub_recovery_model_q2,                                  -- recovery_model_question2
    sub_action_date,                                        -- action_date
    subaward_report_month,                                  -- award_report_fy_month
    subaward_report_year,                                   -- award_report_fy_year
    case
      when subaward_type = 'sub-grant' then 'grant'
      when subaward_type = 'sub-contract' then 'procurement'
      else null
    end,                                                    -- award_type
    prime_id,                                               -- broker_award_id
    upper(internal_id),                                     -- internal_id
    case
      when subaward_type = 'sub-grant' then award_id
      else null
    end,                                                    -- fain
    sub_ultimate_parent_unique_ide,                         -- parent_recipient_unique_id
    upper(sub_ultimate_parent_uei),                         -- parent_recipient_uei
    case
        when subaward_type = 'sub-contract' then award_id
        else null
    end,                                                    -- piid
    upper(sub_place_of_perform_congressio),                 -- pop_congressional_code
    upper(sub_place_of_perform_country_co),                 -- pop_country_code
    upper(sub_place_of_perform_state_code),                 -- pop_state_code
    sub_place_of_performance_zip,                           -- pop_zip4
    '{}'::text[],                                           -- business_categories
    upper(awardee_or_recipient_legal),                      -- prime_recipient_name
    upper(sub_legal_entity_congressional),                  -- recipient_location_congressional_code
    upper(sub_legal_entity_country_code),                   -- recipient_location_country_code
    upper(sub_legal_entity_state_code),                     -- recipient_location_state_code
    left(coalesce(sub_legal_entity_zip, ''), 5),            -- recipient_location_zip5
    upper(sub_awardee_or_recipient_legal),                  -- recipient_name
    upper(sub_awardee_or_recipient_uniqu),                  -- recipient_unique_id
    upper(sub_awardee_or_recipient_uei),                    -- recipient_uei
    upper(sub_place_of_perform_city_name),                  -- pop_city_name
    upper(sub_place_of_perform_state_name),                 -- pop_state_name
    upper(sub_place_of_perform_street),                     -- pop_street_address
    upper(sub_legal_entity_city_name),                      -- recipient_location_city_name
    upper(sub_dba_name),                                    -- dba_name
    upper(sub_ultimate_parent_legal_enti),                  -- parent_recipient_name
    upper(sub_business_types),                              -- business_type_description
    sub_high_comp_officer1_amount,                          -- officer_1_amount
    sub_high_comp_officer1_full_na,                         -- officer_1_name
    sub_high_comp_officer2_amount,                          -- officer_2_amount
    sub_high_comp_officer2_full_na,                         -- officer_2_name
    sub_high_comp_officer3_amount,                          -- officer_3_amount
    sub_high_comp_officer3_full_na,                         -- officer_3_name
    sub_high_comp_officer4_amount,                          -- officer_4_amount
    sub_high_comp_officer4_full_na,                         -- officer_4_name
    sub_high_comp_officer5_amount,                          -- officer_5_amount
    sub_high_comp_officer5_full_na,                         -- officer_5_name
    sub_legal_entity_foreign_posta,                         -- recipient_location_foreign_postal_code
    upper(sub_legal_entity_state_name),                     -- recipient_location_state_name
    upper(sub_legal_entity_address_line1),                  -- recipient_location_street_address
    sub_legal_entity_zip,                                   -- recipient_location_zip4
    now(),                                                  -- updated_at
    unique_award_key                                        -- unique_award_key

from
    broker_subaward bs
    inner join temp_load_subawards_new_or_updated t on t.id = bs.id

where
    subaward_number is not null;
