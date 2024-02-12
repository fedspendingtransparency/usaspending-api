from usaspending_api.common.data_classes import TransactionColumn

TRANSACTION_FABS_COLUMN_INFO = [
    TransactionColumn("action_date", "action_date", "STRING", "string_datetime_remove_timestamp"),
    TransactionColumn("action_type", "action_type", "STRING"),
    TransactionColumn("action_type_description", "action_type_description", "STRING"),
    TransactionColumn("afa_generated_unique", "afa_generated_unique", "STRING"),
    TransactionColumn("assistance_type", "assistance_type", "STRING"),
    TransactionColumn("assistance_type_desc", "assistance_type_desc", "STRING"),
    TransactionColumn("award_description", "award_description", "STRING"),
    TransactionColumn("award_modification_amendme", "award_modification_amendme", "STRING"),
    TransactionColumn("awardee_or_recipient_legal", "awardee_or_recipient_legal", "STRING"),
    TransactionColumn("awardee_or_recipient_uniqu", "awardee_or_recipient_uniqu", "STRING"),
    TransactionColumn("awarding_agency_code", "awarding_agency_code", "STRING"),
    TransactionColumn("awarding_agency_name", "awarding_agency_name", "STRING"),
    TransactionColumn("awarding_office_code", "awarding_office_code", "STRING"),
    TransactionColumn("awarding_office_name", "awarding_office_name", "STRING"),
    TransactionColumn("awarding_sub_tier_agency_c", "awarding_sub_tier_agency_c", "STRING"),
    TransactionColumn("awarding_sub_tier_agency_n", "awarding_sub_tier_agency_n", "STRING"),
    TransactionColumn("business_funds_ind_desc", "business_funds_ind_desc", "STRING"),
    TransactionColumn("business_funds_indicator", "business_funds_indicator", "STRING"),
    TransactionColumn("business_types", "business_types", "STRING"),
    TransactionColumn("business_types_desc", "business_types_desc", "STRING"),
    TransactionColumn("cfda_number", "assistance_listing_number", "STRING"),
    TransactionColumn("cfda_title", "assistance_listing_title", "STRING"),
    TransactionColumn("correction_delete_ind_desc", "correction_delete_ind_desc", "STRING"),
    TransactionColumn("correction_delete_indicatr", "correction_delete_indicatr", "STRING"),
    TransactionColumn("created_at", "created_at", "TIMESTAMP"),
    TransactionColumn("face_value_loan_guarantee", "face_value_loan_guarantee", "NUMERIC(23,2)"),
    TransactionColumn("fain", "fain", "STRING"),
    TransactionColumn("federal_action_obligation", "federal_action_obligation", "NUMERIC(23,2)"),
    TransactionColumn("fiscal_year_and_quarter_co", "fiscal_year_and_quarter_co", "STRING"),
    TransactionColumn("funding_agency_code", "funding_agency_code", "STRING"),
    TransactionColumn("funding_agency_name", "funding_agency_name", "STRING"),
    TransactionColumn("funding_office_code", "funding_office_code", "STRING"),
    TransactionColumn("funding_office_name", "funding_office_name", "STRING"),
    TransactionColumn("funding_opportunity_goals", "funding_opportunity_goals", "STRING"),
    TransactionColumn("funding_opportunity_number", "funding_opportunity_number", "STRING"),
    TransactionColumn("funding_sub_tier_agency_co", "funding_sub_tier_agency_co", "STRING"),
    TransactionColumn("funding_sub_tier_agency_na", "funding_sub_tier_agency_na", "STRING"),
    TransactionColumn("indirect_federal_sharing", "indirect_federal_sharing", "NUMERIC(38,18)"),
    TransactionColumn("is_active", "is_active", "BOOLEAN"),
    TransactionColumn("is_historical", "is_historical", "BOOLEAN", "leave_null"),
    TransactionColumn("legal_entity_address_line1", "legal_entity_address_line1", "STRING"),
    TransactionColumn("legal_entity_address_line2", "legal_entity_address_line2", "STRING"),
    TransactionColumn("legal_entity_address_line3", "legal_entity_address_line3", "STRING"),
    TransactionColumn("legal_entity_city_code", "legal_entity_city_code", "STRING"),
    TransactionColumn("legal_entity_city_name", "legal_entity_city_name", "STRING"),
    TransactionColumn("legal_entity_congressional", "legal_entity_congressional", "STRING"),
    TransactionColumn(
        "legal_entity_country_code",
        "legal_entity_country_code",
        "STRING",
        scalar_transformation="CASE {input} \
            WHEN 'UNITED STATES' THEN 'USA' \
            ELSE {input} \
            END",
    ),
    TransactionColumn(
        "legal_entity_country_name",
        "legal_entity_country_name",
        "STRING",
        scalar_transformation="CASE \
            WHEN {input} = 'USA' THEN 'UNITED STATES' \
            WHEN COALESCE({input}, '') = '' AND legal_entity_country_code = 'UNITED STATES' THEN 'UNITED STATES' \
            ELSE {input} \
            END",
    ),
    TransactionColumn("legal_entity_county_code", "legal_entity_county_code", "STRING"),
    TransactionColumn("legal_entity_county_name", "legal_entity_county_name", "STRING"),
    TransactionColumn("legal_entity_foreign_city", "legal_entity_foreign_city", "STRING"),
    TransactionColumn("legal_entity_foreign_descr", "legal_entity_foreign_descr", "STRING"),
    TransactionColumn("legal_entity_foreign_posta", "legal_entity_foreign_posta", "STRING"),
    TransactionColumn("legal_entity_foreign_provi", "legal_entity_foreign_provi", "STRING"),
    TransactionColumn("legal_entity_state_code", "legal_entity_state_code", "STRING"),
    TransactionColumn("legal_entity_state_name", "legal_entity_state_name", "STRING"),
    TransactionColumn("legal_entity_zip5", "legal_entity_zip5", "STRING"),
    TransactionColumn("legal_entity_zip_last4", "legal_entity_zip_last4", "STRING"),
    TransactionColumn("modified_at", "modified_at", "TIMESTAMP"),
    TransactionColumn("non_federal_funding_amount", "non_federal_funding_amount", "NUMERIC(23,2)"),
    TransactionColumn("officer_1_amount", "high_comp_officer1_amount", "NUMERIC(23,2)", "cast"),
    TransactionColumn("officer_1_name", "high_comp_officer1_full_na", "STRING"),
    TransactionColumn("officer_2_amount", "high_comp_officer2_amount", "NUMERIC(23,2)", "cast"),
    TransactionColumn("officer_2_name", "high_comp_officer2_full_na", "STRING"),
    TransactionColumn("officer_3_amount", "high_comp_officer3_amount", "NUMERIC(23,2)", "cast"),
    TransactionColumn("officer_3_name", "high_comp_officer3_full_na", "STRING"),
    TransactionColumn("officer_4_amount", "high_comp_officer4_amount", "NUMERIC(23,2)", "cast"),
    TransactionColumn("officer_4_name", "high_comp_officer4_full_na", "STRING"),
    TransactionColumn("officer_5_amount", "high_comp_officer5_amount", "NUMERIC(23,2)", "cast"),
    TransactionColumn("officer_5_name", "high_comp_officer5_full_na", "STRING"),
    TransactionColumn("original_loan_subsidy_cost", "original_loan_subsidy_cost", "NUMERIC(23,2)"),
    TransactionColumn("period_of_performance_curr", "period_of_performance_curr", "STRING"),
    TransactionColumn("period_of_performance_star", "period_of_performance_star", "STRING"),
    TransactionColumn("place_of_perfor_state_code", "place_of_perfor_state_code", "STRING"),
    TransactionColumn(
        "place_of_perform_country_c",
        "place_of_perform_country_c",
        "STRING",
        scalar_transformation="CASE {input} \
            WHEN 'UNITED STATES' THEN 'USA' \
            ELSE {input} \
            END",
    ),
    TransactionColumn(
        "place_of_perform_country_n",
        "place_of_perform_country_n",
        "STRING",
        scalar_transformation="CASE \
            WHEN {input} = 'USA' THEN 'UNITED STATES' \
            WHEN COALESCE({input}, '') = '' AND place_of_perform_country_c = 'UNITED STATES' THEN 'UNITED STATES' \
            ELSE {input} \
            END",
    ),
    TransactionColumn("place_of_perform_county_co", "place_of_perform_county_co", "STRING"),
    TransactionColumn("place_of_perform_county_na", "place_of_perform_county_na", "STRING"),
    TransactionColumn("place_of_perform_state_nam", "place_of_perform_state_nam", "STRING"),
    TransactionColumn("place_of_perform_zip_last4", "place_of_perform_zip_last4", "STRING"),
    TransactionColumn("place_of_performance_city", "place_of_performance_city", "STRING"),
    TransactionColumn("place_of_performance_code", "place_of_performance_code", "STRING"),
    TransactionColumn("place_of_performance_congr", "place_of_performance_congr", "STRING"),
    TransactionColumn("place_of_performance_forei", "place_of_performance_forei", "STRING"),
    TransactionColumn("place_of_performance_scope", "place_of_performance_scope", "STRING"),
    TransactionColumn("place_of_performance_zip4a", "place_of_performance_zip4a", "STRING"),
    TransactionColumn("place_of_performance_zip5", "place_of_performance_zip5", "STRING"),
    TransactionColumn("published_fabs_id", "published_fabs_id", "INTEGER"),
    TransactionColumn("record_type", "record_type", "INTEGER"),
    TransactionColumn("record_type_description", "record_type_description", "STRING"),
    TransactionColumn("sai_number", "sai_number", "STRING"),
    TransactionColumn("submission_id", "submission_id", "INTEGER"),
    TransactionColumn("total_funding_amount", "total_funding_amount", "STRING"),
    TransactionColumn("transaction_id", None, "LONG NOT NULL"),
    TransactionColumn("uei", "uei", "STRING"),
    TransactionColumn("ultimate_parent_legal_enti", "ultimate_parent_legal_enti", "STRING"),
    TransactionColumn("ultimate_parent_uei", "ultimate_parent_uei", "STRING"),
    TransactionColumn("ultimate_parent_unique_ide", "ultimate_parent_unique_ide", "STRING"),
    TransactionColumn("unique_award_key", "unique_award_key", "STRING"),
    TransactionColumn("updated_at", "updated_at", "TIMESTAMP"),
    TransactionColumn("uri", "uri", "STRING"),
]

TRANSACTION_FABS_COLUMNS = [col.dest_name for col in TRANSACTION_FABS_COLUMN_INFO]

delta_columns_not_in_view = [
    "fiscal_year_and_quarter_co",
    "is_active",
    "is_historical",
    "submission_id",
    "created_at",
    "updated_at",
]

TRANSACTION_FABS_VIEW_COLUMNS = [
    col.dest_name for col in TRANSACTION_FABS_COLUMN_INFO if col.dest_name not in delta_columns_not_in_view
]

transaction_fabs_sql_string = rf"""
    CREATE OR REPLACE TABLE {{DESTINATION_TABLE}} (
        {", ".join([f'{col.dest_name} {col.delta_type}' for col in TRANSACTION_FABS_COLUMN_INFO])}
    )
    USING DELTA
    LOCATION 's3a://{{SPARK_S3_BUCKET}}/{{DELTA_LAKE_S3_PATH}}/{{DESTINATION_DATABASE}}/{{DESTINATION_TABLE}}'
    """

# Mapping from raw.published_fabs to int.transaction_normalized columns, where a simple mapping exists
FABS_TO_NORMALIZED_COLUMN_INFO = [
    # action_date seen as: mm/dd/YYYY, YYYYmmdd, YYYY-mm-dd, so need special parsing
    TransactionColumn("action_date", "action_date", "DATE", "parse_string_datetime_to_date"),
    TransactionColumn("action_type", "action_type", "STRING"),
    TransactionColumn("action_type_description", "action_type_description", "STRING"),
    TransactionColumn("certified_date", "NULL", "DATE", "literal"),
    TransactionColumn("description", "award_description", "STRING"),
    TransactionColumn("face_value_loan_guarantee", "face_value_loan_guarantee", "NUMERIC(23,2)"),
    TransactionColumn("federal_action_obligation", "federal_action_obligation", "NUMERIC(23,2)"),
    TransactionColumn("indirect_federal_sharing", "indirect_federal_sharing", "NUMERIC(23, 2)", "cast"),
    TransactionColumn("is_fpds", "FALSE", "BOOLEAN", "literal"),
    TransactionColumn("last_modified_date", "modified_at", "DATE", "cast"),
    TransactionColumn("modification_number", "award_modification_amendme", "STRING"),
    TransactionColumn("non_federal_funding_amount", "non_federal_funding_amount", "NUMERIC(23,2)"),
    TransactionColumn("original_loan_subsidy_cost", "original_loan_subsidy_cost", "NUMERIC(23,2)"),
    # period_of_performance_* fields seen as: mm/dd/YYYY as well as YYYYmmdd, so need special parsing
    TransactionColumn(
        "period_of_performance_current_end_date", "period_of_performance_curr", "DATE", "parse_string_datetime_to_date"
    ),
    TransactionColumn(
        "period_of_performance_start_date", "period_of_performance_star", "DATE", "parse_string_datetime_to_date"
    ),
    TransactionColumn("transaction_unique_id", "afa_generated_unique", "STRING"),
    TransactionColumn("type", "assistance_type", "STRING"),
    TransactionColumn("type_description", "assistance_type_desc", "STRING"),
    TransactionColumn("unique_award_key", "unique_award_key", "STRING"),
    TransactionColumn("usaspending_unique_transaction_id", "NULL", "STRING", "literal"),
]
