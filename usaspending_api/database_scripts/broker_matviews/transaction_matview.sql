CREATE MATERIALIZED VIEW transaction_matview_new AS
(
SELECT
    *,
    fy(action_date) AS fiscal_year
FROM
    dblink ('broker_server', '(select
            -- unique ids + cols used for unique id
            ''cont_tx_'' || detached_award_proc_unique as generated_unique_transaction_id,
            ''cont_aw_'' ||
                coalesce(agency_id,''-none-'') || ''_'' ||
                coalesce(referenced_idv_agency_iden,''-none-'') || ''_'' ||
                coalesce(piid,''-none-'') || ''_'' ||
                coalesce(parent_award_id,''-none-'') AS generated_unique_award_id,
            piid,
            parent_award_id AS parent_award_piid,
            NULL AS fain,
            NULL AS uri,
            agency_id,
            referenced_idv_agency_iden,
            award_modification_amendme,
            NULLIF(transaction_number, '''') AS transaction_number,

            -- duns
            awardee_or_recipient_uniqu,
            awardee_or_recipient_legal,

            -- recipient
            legal_entity_address_line1 AS recipient_location_address_line1,
            legal_entity_address_line2 AS recipient_location_address_line2,
            legal_entity_address_line3 AS recipient_location_address_line3,
            NULL AS recipient_location_foreign_province,
            legal_entity_country_code AS recipient_location_country_code,
            legal_entity_country_name AS recipient_location_country_name,
            legal_entity_state_code AS recipient_location_state_code,
            legal_entity_state_descrip AS recipient_location_state_name,
            legal_entity_county_code AS recipient_location_county_code,
            legal_entity_county_name AS recipient_location_county_name,
            legal_entity_city_name AS recipient_location_city_name,
            legal_entity_zip5 AS recipient_location_zip5,
            legal_entity_congressional AS recipient_location_congressional_code,

            -- place of performance
            place_of_perform_country_c AS pop_country_code,
            place_of_perf_country_desc AS pop_country_name,
            place_of_performance_state AS pop_state_code,
            place_of_perfor_state_desc AS pop_state_name,
            place_of_perform_county_co AS pop_county_code,
            place_of_perform_county_na AS pop_county_name,
            place_of_perform_city_name AS pop_city_name,
            place_of_performance_zip5 AS pop_zip5,
            place_of_performance_congr AS pop_congressional_code,

            -- other (fpds specific)
            awarding_agency_code,
            awarding_agency_name,
            awarding_sub_tier_agency_c,
            awarding_sub_tier_agency_n,
            awarding_office_code,
            awarding_office_name,
            funding_agency_code,
            funding_agency_name,
            funding_sub_tier_agency_co,
            funding_sub_tier_agency_na,
            funding_office_code,
            funding_office_name,

            contract_award_type,
            contract_award_type_desc,
            referenced_idv_type,
            referenced_idv_type_desc,
            federal_action_obligation,
            NULLIF(action_date, '''')::DATE AS action_date,
            award_description,
            NULLIF(period_of_performance_star, '''')::DATE AS period_of_performance_star,
            NULLIF(period_of_performance_curr, '''')::DATE AS period_of_performance_curr,
            base_and_all_options_value,
            last_modified::DATE AS last_modified_date,
            pulled_from,
            product_or_service_code,
            product_or_service_co_desc,
            extent_competed,
            extent_compete_description,
            type_of_contract_pricing,
            naics,
            naics_description,
            idv_type,
            idv_type_description,
            type_set_aside,
            type_set_aside_description,

            -- other (fabs specific)
            NULL AS assistance_type,
            NULL AS original_loan_subsidy_cost,
            NULL AS record_type,
            NULL AS business_funds_indicator,
            NULL AS business_types,
            NULL AS business_types_description,
            NULL AS cfda_number,
            NULL AS cfda_title,
            NULL AS sai_number
        FROM detached_award_procurement)

        union all

        (select
            -- unique ids + cols used for unique id
            ''asst_tx_'' || afa_generated_unique as generated_unique_transaction_id,
            ''asst_aw_'' ||
                coalesce(awarding_sub_tier_agency_c,''-none-'') || ''_'' ||
                coalesce(fain, ''-none-'') || ''_'' ||
                coalesce(uri, ''-none-'') AS generated_unique_award_id,
            NULL AS piid,
            NULL AS parent_award_piid,
            fain,
            uri,
            NULL AS agency_id,
            NULL AS referenced_idv_agency_iden,
            award_modification_amendme,
            NULL AS transaction_number,

            -- duns
            awardee_or_recipient_uniqu,
            awardee_or_recipient_legal,

            -- recipient
            legal_entity_address_line1 AS recipient_location_address_line1,
            legal_entity_address_line2 AS recipient_location_address_line2,
            legal_entity_address_line3 AS recipient_location_address_line3,
            legal_entity_foreign_provi AS recipient_location_foreign_province,
            legal_entity_country_code AS recipient_location_country_code,
            legal_entity_country_name AS recipient_location_country_name,
            legal_entity_state_code AS recipient_location_state_code,
            legal_entity_state_name AS recipient_location_state_name,
            legal_entity_county_code AS recipient_location_county_code,
            legal_entity_county_name recipient_location_county_name,
            legal_entity_city_name AS recipient_location_city_name,
            legal_entity_zip5 AS recipient_location_zip5,
            legal_entity_congressional AS recipient_location_congressional_code,

            -- place of performance
            place_of_perform_country_c AS pop_country_code,
            place_of_perform_country_n AS pop_country_name,
            place_of_perfor_state_code AS pop_state_code,
            place_of_perform_state_nam AS pop_state_name,
            place_of_perform_county_co AS pop_county_code,
            place_of_perform_county_na AS pop_county_name,
            place_of_performance_city AS pop_city_name,
            place_of_performance_zip5 AS pop_zip5,
            place_of_performance_congr AS pop_congressional_code,

            -- other (fpds specific)
            awarding_agency_code,
            awarding_agency_name,
            awarding_sub_tier_agency_c,
            awarding_sub_tier_agency_n,
            awarding_office_code,
            awarding_office_name,
            funding_agency_code,
            funding_agency_name,
            funding_sub_tier_agency_co,
            funding_sub_tier_agency_na,
            funding_office_code,
            funding_office_name,

            NULL AS contract_award_type,
            NULL AS contract_award_type_desc,
            NULL AS referenced_idv_type,
            NULL AS referenced_idv_type_desc,
            federal_action_obligation,
            NULLIF(action_date, '''')::DATE AS action_date,
            award_description,
            NULLIF(period_of_performance_star, '''')::DATE AS period_of_performance_star,
            NULLIF(period_of_performance_curr, '''')::DATE AS period_of_performance_curr,
            NULL AS base_and_all_options_value,
            modified_at::DATE AS last_modified_date,
            NULL AS pulled_from,
            NULL AS product_or_service_code,
            NULL AS product_or_service_co_desc,
            NULL AS extent_competed,
            NULL AS extent_compete_description,
            NULL AS type_of_contract_pricing,
            NULL AS naics,
            NULL AS naics_description,
            NULL AS idv_type,
            NULL AS idv_type_description,
            NULL AS type_set_aside,
            NULL AS type_set_aside_description,

            -- other (fabs specific)
            assistance_type,
            original_loan_subsidy_cost,
            record_type,
            business_funds_indicator,
            business_types,
            CASE
                WHEN UPPER(business_types) = ''A'' THEN ''State government''
                WHEN UPPER(business_types) = ''B'' THEN ''County Government''
                WHEN UPPER(business_types) = ''C'' THEN ''City or Township Government''
                WHEN UPPER(business_types) = ''D'' THEN ''Special District Government''
                WHEN UPPER(business_types) = ''E'' THEN ''Regional Organization''
                WHEN UPPER(business_types) = ''F'' THEN ''U.S. Territory or Possession''
                WHEN UPPER(business_types) = ''G'' THEN ''Independent School District''
                WHEN UPPER(business_types) = ''H'' THEN ''Public/State Controlled Institution of Higher Education''
                WHEN UPPER(business_types) = ''I'' THEN ''Indian/Native American Tribal Government (Federally Recognized)''
                WHEN UPPER(business_types) = ''J'' THEN ''Indian/Native American Tribal Government (Other than Federally Recognized)''
                WHEN UPPER(business_types) = ''K'' THEN ''Indian/Native American Tribal Designated Organization''
                WHEN UPPER(business_types) = ''L'' THEN ''Public/Indian Housing Authority''
                WHEN UPPER(business_types) = ''M'' THEN ''Nonprofit with 501(c)(3) IRS Status (Other than Institution of Higher Education)''
                WHEN UPPER(business_types) = ''N'' THEN ''Nonprofit without 501(c)(3) IRS Status (Other than Institution of Higher Education)''
                WHEN UPPER(business_types) = ''O'' THEN ''Private Institution of Higher Education''
                WHEN UPPER(business_types) = ''P'' THEN ''Individual''
                WHEN UPPER(business_types) = ''Q'' THEN ''For-Profit Organization (Other than Small Business)''
                WHEN UPPER(business_types) = ''R'' THEN ''Small Business''
                WHEN UPPER(business_types) = ''S'' THEN ''Hispanic-serving Institution''
                WHEN UPPER(business_types) = ''T'' THEN ''Historically Black Colleges and Universities (HBCUs)''
                WHEN UPPER(business_types) = ''U'' THEN ''Tribally Controlled Colleges and Universities (TCCUs)''
                WHEN UPPER(business_types) = ''V'' THEN ''Alaska Native and Native Hawaiian Serving Institutions''
                WHEN UPPER(business_types) = ''W'' THEN ''Non-domestic (non-US) Entity''
                WHEN UPPER(business_types) = ''X'' THEN ''Other''
                ELSE ''Unknown Types''
            END AS business_types_description,
            cfda_number,
            cfda_title,
            sai_number
        FROM published_award_financial_assistance
        WHERE is_active=TRUE)') AS transaction
        (
            -- unique ids + cols used for unique id
            generated_unique_transaction_id text,
            generated_unique_award_id text,
            piid text,
            parent_award_piid text,
            fain text,
            uri text,
            agency_id text,
            referenced_idv_agency_iden text,
            award_modification_amendme text,
            transaction_number int,

            -- duns
            awardee_or_recipient_uniqu text,
            awardee_or_recipient_legal text,

            -- recipient
            recipient_location_address_line1 text,
            recipient_location_address_line2 text,
            recipient_location_address_line3 text,
            recipient_location_foreign_province text,
            recipient_location_country_code text,
            recipient_location_country_name text,
            recipient_location_state_code text,
            recipient_location_state_name text,
            recipient_location_county_code text,
            recipient_location_county_name text,
            recipient_location_city_name text,
            recipient_location_zip5 text,
            recipient_location_congressional_code text,

            -- place of performance
            pop_country_code text,
            pop_country_name text,
            pop_state_code text,
            pop_state_name text,
            pop_county_code text,
            pop_county_name text,
            pop_city_name text,
            pop_zip5 text,
            pop_congressional_code text,

            -- other (fpds specific)
            awarding_agency_code text,
            awarding_agency_name text,
            awarding_sub_tier_agency_c text,
            awarding_sub_tier_agency_n text,
            awarding_office_code text,
            awarding_office_name text,
            funding_agency_code text,
            funding_agency_name text,
            funding_sub_tier_agency_co text,
            funding_sub_tier_agency_na text,
            funding_office_code text,
            funding_office_name text,
            contract_award_type text,
            contract_award_type_desc text,
            referenced_idv_type text,
            referenced_idv_type_desc text,
            federal_action_obligation float(2),
            action_date date,
            award_description text,
            period_of_performance_star date,
            period_of_performance_curr date,
            base_and_all_options_value float(2),
            last_modified_date date,
            pulled_from text,
            product_or_service_code text,
            product_or_service_co_desc text,
            extent_competed text,
            extent_compete_description text,
            type_of_contract_pricing text,
            naics text,
            naics_description text,
            idv_type text,
            idv_type_description text,
            type_set_aside text,
            type_set_aside_description text,

            -- other (fabs specific)
            assistance_type text,
            original_loan_subsidy_cost float(2),
            record_type int,
            business_funds_indicator text,
            business_types text,
            business_types_description text,
            cfda_number text,
            cfda_title text,
            sai_number text
        )
);

ALTER MATERIALIZED VIEW IF EXISTS transaction_matview RENAME TO transaction_matview_old;
ALTER MATERIALIZED VIEW IF EXISTS transaction_matview_new RENAME TO transaction_matview;
DROP MATERIALIZED VIEW IF EXISTS transaction_matview_old;