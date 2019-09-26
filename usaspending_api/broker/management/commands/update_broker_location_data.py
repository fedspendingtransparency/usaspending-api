location_fields_mappings = {
    "fabs": {
        "recipient": {
            "address_line1": "legal_entity_address_line1",
            "address_line2": "legal_entity_address_line2",
            "address_line3": "legal_entity_address_line3",
            "city_name": "legal_entity_city_name",
            "city_code": "legal_entity_city_code",
            "congressional_code": "legal_entity_congressional",
            "location_country_code": "legal_entity_country_code",
            "country_name": "legal_entity_country_name",
            "county_code": "legal_entity_county_code",
            "county_name": "legal_entity_county_name",
            "foreign_city_name": "legal_entity_foreign_city",
            "foreign_postal_code": "legal_entity_foreign_posta",
            "foreign_province": "legal_entity_foreign_provi",
            "state_code": "legal_entity_state_code",
            "state_name": "legal_entity_state_name",
            "zip5": "legal_entity_zip5",
            "zip_last4": "legal_entity_zip_last4",
        },
        "place_of_performance": {
            "city_name": "place_of_performance_city",
            "performance_code": "place_of_performance_code",
            "congressional_code": "place_of_performance_congr",
            "location_country_code": "place_of_perform_country_c",
            "country_name": "place_of_perform_country_n",
            "county_code": "place_of_perform_county_co",
            "county_name": "place_of_perform_county_na",
            "foreign_location_description": "place_of_performance_forei",
            "state_name": "place_of_perform_state_nam",
            "state_code": "place_of_perfor_state_code",
            "zip5": "place_of_performance_zip5",
            "zip_last4": "place_of_perform_zip_last4",
            "zip4": "place_of_performance_zip4a",
        },
    },
    "fpds": {
        "recipient": {
            "address_line1": "legal_entity_address_line1",
            "address_line2": "legal_entity_address_line2",
            "address_line3": "legal_entity_address_line3",
            "city_name": "legal_entity_city_name",
            "congressional_code": "legal_entity_congressional",
            "location_country_code": "legal_entity_country_code",
            "country_name": "legal_entity_country_name",
            "county_code": "legal_entity_county_code",
            "county_name": "legal_entity_county_name",
            "state_code": "legal_entity_state_code",
            "state_name": "legal_entity_state_descrip",
            "zip5": "legal_entity_zip5",
            "zip_last4": "legal_entity_zip_last4",
            "zip4": "broker.legal_entity_zip4",
        },
        "place_of_performance": {
            "city_name": "place_of_performance_city_name",
            "congressional_code": "place_of_performance_congr",
            "location_country_code": "place_of_perform_country_c",
            "country_name": "place_of_perf_country_desc",
            "county_code": "place_of_perform_county_co",
            "county_name": "place_of_perform_county_na",
            "state_name": "place_of_perfor_state_desc",
            "state_code": "place_of_performance_state",
            "zip5": "place_of_performance_zip5",
            "zip_last4": "place_of_perform_zip_last4",
            "zip4": "place_of_performance_zip4a",
        },
    },
}


def update_tmp_table_location_changes(file_type, database_columns, unique_identifier, fiscal_year):
    """
        Adds columns to temporary table to specify which rows have a legal entity and/or a place of performance change
    """
    le_loc_columns_distinct = " OR ".join(
        [
            "website.{column} IS DISTINCT FROM broker.{column}".format(column=column)
            for column in database_columns
            if column[:12] == "legal_entity"
        ]
    )

    pop_loc_columns_distinct = " OR ".join(
        [
            "website.{column} IS DISTINCT FROM broker.{column}".format(column=column)
            for column in database_columns
            if column[:13] == "place_of_perf"
        ]
    )

    sql_statement = """
         -- Include columns to determine whether we need a place of performance change or recipient location
       ALTER TABLE {file_type}_transactions_to_update_{fiscal_year}
       ADD COLUMN place_of_performance_change boolean, add COLUMN recipient_change boolean;

        UPDATE {file_type}_transactions_to_update_{fiscal_year} broker
        SET
        recipient_change = (
            CASE  WHEN
            {le_loc_columns_distinct}
            THEN TRUE ELSE FALSE END
            ),
        place_of_performance_change = (
            CASE  WHEN
           {pop_loc_columns_distinct}
           THEN TRUE ELSE FALSE END
           )
        FROM transaction_{file_type} website
        WHERE broker.{unique_identifier} = website.{unique_identifier};


        -- Delete rows where there is no transaction in the table
        DELETE FROM {file_type}_transactions_to_update_{fiscal_year}
        WHERE place_of_performance_change IS NULL
        AND recipient_change IS NULL;

        -- Adding index to table to improve speed on update
        CREATE INDEX {file_type}_le_loc_idx ON {file_type}_transactions_to_update_{fiscal_year}(recipient_change);
        CREATE INDEX {file_type}_pop_idx ON
            {file_type}_transactions_to_update_{fiscal_year}(place_of_performance_change);
        ANALYZE {file_type}_transactions_to_update_{fiscal_year};
        """.format(
        file_type=file_type,
        unique_identifier=unique_identifier,
        fiscal_year=fiscal_year,
        le_loc_columns_distinct=le_loc_columns_distinct,
        pop_loc_columns_distinct=pop_loc_columns_distinct,
    )
    return sql_statement


def update_location_table(file_type, loc_scope, database_columns, unique_identifier, fiscal_year):
    """
    Returns script to update references location for legal entity and place of performance locations from
    the temporary table with broker data
    """
    location_update_code = ", ".join(
        [
            "{loc_col} = broker.{broker_col}".format(loc_co=loc_col, broker_col=broker_col)
            for loc_col, broker_col in location_fields_mappings[file_type][loc_scope].items()
            if broker_col in database_columns
        ]
    )

    # Legal Entity has an additional table and join from transactions_normalized
    legal_entity_table = ", legal_entity" if loc_scope == "recipient" else ""
    location_join = (
        "AND legal_entity.legal_entity_id = transaction_normalized.recipient_id"
        + " AND legal_entity.location_id = loc.location_id"
        if loc_scope == "recipient"
        else "AND loc.location_id = transaction_normalized.place_of_performance_id"
    )

    sql_statement = """
    UPDATE references_location AS loc
    SET
        {location_update_code}
    FROM {file_type}_transactions_to_update_{fiscal_year} broker,
        transaction_{file_type},
        transaction_normalized
        {legal_entity_table}
    WHERE broker.{unique_identifier} = transaction_{file_type}.{unique_identifier}
    AND transaction_{file_type}.transaction_id = transaction_normalized.id
    {location_join}
    AND broker.{loc_scope}_change = TRUE;
    """.format(
        location_update_code=location_update_code,
        loc_scope=loc_scope,
        file_type=file_type,
        fiscal_year=fiscal_year,
        legal_entity_table=legal_entity_table,
        unique_identifier=unique_identifier,
        location_join=location_join,
    )

    return sql_statement
