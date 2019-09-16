import psycopg2
import logging
from os import environ

from usaspending_api.data_load.field_mappings_fpds import (
    transaction_fpds_columns,
    transaction_normalized_columns,
    transaction_normalized_functions,
    legal_entity_columns,
    legal_entity_boolean_columns,
    legal_entity_functions,
    recipient_location_columns,
    recipient_location_functions,
    place_of_performance_columns,
    place_of_performance_functions,
    award_functions,
    transaction_fpds_boolean_columns,
    transaction_fpds_functions,
)
from usaspending_api.data_load.data_load_helpers import (
    capitalize_if_string,
    false_if_null,
    setup_load_lists,
    setup_mass_load_lists,
)
from usaspending_api.common.helpers.timing_helpers import Timer
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string


USASPENDING_CONNECTION_STRING = get_database_dsn_string()
BROKER_CONNECTION_STRING = environ.get("DATA_BROKER_DATABASE_URL", None)

DESTROY_ORPHANS_LEGAL_ENTITY_SQL = (
    "DELETE FROM legal_entity legal WHERE legal.legal_entity_id in "
    "(SELECT l.legal_entity_id FROM legal_entity l "
    "LEFT JOIN transaction_normalized t ON t.recipient_id = l.legal_entity_id "
    "LEFT JOIN awards a ON a.recipient_id = l.legal_entity_id "
    "WHERE t is null and a.id is null); "
)
DESTROY_ORPHANS_REFERENCES_LOCATION_SQL = (
    "DELETE FROM references_location location WHERE location.location_id in "
    "(SELECT l.location_id FROM references_location l "
    "LEFT JOIN transaction_normalized t ON t.place_of_performance_id = l.location_id "
    "LEFT JOIN legal_entity e ON e.location_id = l.location_id "
    "LEFT JOIN awards a ON a.place_of_performance_id = l.location_id "
    "WHERE t.id is null and a.id is null and e.legal_entity_id is null)"
)

CHUNK_SIZE = 5000

logger = logging.getLogger("console")


def destroy_orphans():
    """cleans up tables after run_fpds_load is called"""
    with psycopg2.connect(dsn=USASPENDING_CONNECTION_STRING) as connection:
        with connection.cursor() as cursor:
            cursor.execute(DESTROY_ORPHANS_LEGAL_ENTITY_SQL)
            cursor.execute(DESTROY_ORPHANS_REFERENCES_LOCATION_SQL)


def run_fpds_load(id_list):
    """
    Run transaction load for the provided ids. This will create any new rows in other tables to support the transaction
    data, but does NOT update "secondary" award values like total obligations or C -> D linkages. If transactions are
    being reloaded, this will also leave behind rows in supporting tables that won't be removed unless destory_orphans
    is called.
    returns ids for each award touched
    """
    chunks = [id_list[x : x + CHUNK_SIZE] for x in range(0, len(id_list), CHUNK_SIZE)]

    modified_awards = []
    for chunk in chunks:
        with Timer() as timer:
            logger.info("> loading {} ids (ids {}-{})".format(len(chunk), chunk[0], chunk[-1]))
            modified_awards.extend(load_chunk(chunk))
        logger.info("ran load in {}".format(str(timer.elapsed)))
    return modified_awards


def load_chunk(chunk):
    broker_transactions = fetch_broker_objects(chunk)

    load_objects = generate_load_objects(broker_transactions)

    return load_transactions(load_objects)


def fetch_broker_objects(id_list):
    detached_award_procurements = []

    formatted_id_list = "({})".format(",".join(map(str, id_list)))

    with psycopg2.connect(dsn=BROKER_CONNECTION_STRING) as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            sql = "SELECT * from detached_award_procurement where detached_award_procurement_id in {}".format(
                formatted_id_list
            )

            cursor.execute(sql)
            results = cursor.fetchall()
            for result in results:
                detached_award_procurements.append(result)

    return detached_award_procurements


def create_load_object(broker_object, non_boolean_column_map, boolean_column_map, function_map):
    retval = {}
    if non_boolean_column_map:
        retval.update(
            {non_boolean_column_map[key]: capitalize_if_string(broker_object[key]) for key in non_boolean_column_map}
        )

    if boolean_column_map:
        retval.update({boolean_column_map[key]: false_if_null(broker_object[key]) for key in boolean_column_map})

    if function_map:
        for key in function_map:
            retval[key] = function_map[key](broker_object)

    return retval


def generate_load_objects(broker_objects):
    retval = []

    for broker_object in broker_objects:
        connected_objects = {}

        connected_objects["recipient_location"] = create_load_object(
            broker_object, recipient_location_columns, None, recipient_location_functions
        )

        connected_objects["legal_entity"] = create_load_object(
            broker_object, legal_entity_columns, legal_entity_boolean_columns, legal_entity_functions
        )

        connected_objects["place_of_performance_location"] = create_load_object(
            broker_object, place_of_performance_columns, None, place_of_performance_functions
        )

        # matching award. NOT a real db object, but needs to be stored when making the link in load_transactions
        connected_objects["generated_unique_award_id"] = broker_object["unique_award_key"]

        # award. NOT used if a matching award is found later
        connected_objects["award"] = create_load_object(broker_object, None, None, award_functions)

        connected_objects["transaction_normalized"] = create_load_object(
            broker_object, transaction_normalized_columns, None, transaction_normalized_functions
        )

        connected_objects["transaction_fpds"] = create_load_object(
            broker_object, transaction_fpds_columns, transaction_fpds_boolean_columns, transaction_fpds_functions
        )

        retval.append(connected_objects)
    return retval


def load_transactions(load_objects):
    """returns ids for each award touched"""
    retval = []
    with psycopg2.connect(dsn=USASPENDING_CONNECTION_STRING) as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            # First cr
            load_recipient_locations(cursor, load_objects)
            load_recipients(cursor, load_objects)
            load_place_of_performance(cursor, load_objects)

            for load_object in load_objects:

                # AWARD
                # Try to find an award for this transaction to belong to
                find_matching_award_sql = "select id from awards where generated_unique_award_id = '{}'".format(
                    load_object["generated_unique_award_id"]
                )
                cursor.execute(find_matching_award_sql)
                results = cursor.fetchall()

                if results:
                    load_object["transaction_normalized"]["award_id"] = results[0][0]
                    retval.append(results[0][0])
                # If there is no award, we need to create one
                else:
                    columns, values, pairs = setup_load_lists(load_object, "award")
                    generate_matching_award_sql = "INSERT INTO awards {} VALUES {} RETURNING id".format(columns, values)
                    cursor.execute(generate_matching_award_sql)
                    results = cursor.fetchall()
                    load_object["transaction_normalized"]["award_id"] = results[0][0]
                    retval.append(results[0][0])

                # TRANSACTION
                # Determine if we are making a new transaction, or updating an old one
                find_matching_transaction_sql = (
                    "select transaction_id from transaction_fpds "
                    "where detached_award_proc_unique = '{}'".format(
                        load_object["transaction_fpds"]["detached_award_proc_unique"]
                    )
                )
                cursor.execute(find_matching_transaction_sql)
                results = cursor.fetchall()

                if results:
                    # If there is a transaction (transaction_normalized and transaction_fpds should be one-to-one)
                    # we update all values
                    columns, values, pairs = setup_load_lists(load_object, "transaction_fpds")
                    transaction_fpds_sql = (
                        "UPDATE transaction_fpds SET {} "
                        "where detached_award_procurement_id = {}".format(
                            pairs, load_object["transaction_fpds"]["detached_award_procurement_id"]
                        )
                    )
                    cursor.execute(transaction_fpds_sql)

                    columns, values, pairs = setup_load_lists(load_object, "transaction_normalized")
                    load_object["transaction_fpds"]["transaction_id"] = results[0][0]
                    load_object["award"]["latest_tranaction_id"] = results[0][0]
                    transaction_normalized_sql = "UPDATE transaction_normalized SET {} " "where id  = '{}'".format(
                        pairs, load_object["transaction_fpds"]["transaction_id"]
                    )
                    cursor.execute(transaction_normalized_sql)

                    logger.debug("updated fpds transaction {}".format(results[0][0]))
                else:
                    # If there is no transaction we create a new one.
                    # transaction_normalized and transaction_fpds should be one-to-one
                    columns, values, pairs = setup_load_lists(load_object, "transaction_normalized")
                    transaction_normalized_sql = "INSERT INTO transaction_normalized {} VALUES {} RETURNING id".format(
                        columns, values
                    )
                    cursor.execute(transaction_normalized_sql)
                    results = cursor.fetchall()
                    load_object["transaction_fpds"]["transaction_id"] = results[0][0]
                    load_object["award"]["latest_tranaction_id"] = results[0][0]

                    columns, values, pairs = setup_load_lists(load_object, "transaction_fpds")
                    transaction_fpds_sql = "INSERT INTO transaction_fpds {} VALUES {} RETURNING transaction_id".format(
                        columns, values
                    )
                    cursor.execute(transaction_fpds_sql)
                    results = cursor.fetchall()

                    logger.debug("created fpds transaction {}".format(results[0][0]))

                # No matter what, we need to go back and update the award's latest transaction to the award we just made
                update_award_lastest_transaction_sql = "UPDATE awards SET latest_transaction_id = {} where id = {}".format(
                    results[0][0], load_object["award"]["latest_tranaction_id"]
                )
                cursor.execute(update_award_lastest_transaction_sql)

    return retval


def load_recipient_locations(cursor, load_objects):
    sql_to_execute = ""
    columns, values = setup_mass_load_lists(load_objects, "recipient_location")
    recipient_location_sql = "INSERT INTO references_location {} VALUES {} RETURNING location_id;".format(
        columns, values
    )
    sql_to_execute += recipient_location_sql

    cursor.execute(sql_to_execute)
    results = cursor.fetchall()

    for index in range(0, len(results)):
        load_objects[index]["legal_entity"]["location_id"] = results[index][0]


def load_recipients(cursor, load_objects):
    sql_to_execute = ""
    columns, values = setup_mass_load_lists(load_objects, "legal_entity")
    recipient_sql = "INSERT INTO legal_entity {} VALUES {} RETURNING legal_entity_id;".format(columns, values)
    sql_to_execute += recipient_sql

    cursor.execute(sql_to_execute)
    results = cursor.fetchall()

    for index in range(0, len(results)):
        load_objects[index]["transaction_normalized"]["recipient_id"] = results[index][0]
        load_objects[index]["award"]["recipient_id"] = results[index][0]


def load_place_of_performance(cursor, load_objects):
    sql_to_execute = ""
    columns, values = setup_mass_load_lists(load_objects, "place_of_performance_location")
    recipient_sql = "INSERT INTO references_location {} VALUES {} RETURNING location_id;".format(columns, values)
    sql_to_execute += recipient_sql

    cursor.execute(sql_to_execute)
    results = cursor.fetchall()

    for index in range(0, len(results)):
        load_objects[index]["transaction_normalized"]["place_of_performance_id"] = results[index][0]
        load_objects[index]["award"]["place_of_performance_id"] = results[index][0]
