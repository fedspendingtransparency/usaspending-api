from os import environ
import psycopg2
import logging
import time
import math

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
    subtier_agency_list,
    capitalize_if_string,
    false_if_null,
    format_value_for_sql,
)
from usaspending_api.common.helpers.sql_helpers import get_database_dsn_string, get_broker_dsn_string

# DEFINE THESE ENVIRONMENT VARIABLES BEFORE RUNNING!
USASPENDING_CONNECTION_STRING = get_database_dsn_string()
BROKER_CONNECTION_STRING = get_broker_dsn_string()

CHUNK_SIZE = 1000

logger = logging.getLogger("console")


class Timer:
    def __enter__(self):
        self.start = time.perf_counter()
        return self

    def __exit__(self, *args, **kwargs):
        self.end = time.perf_counter()
        self.elapsed = self.end - self.start
        self.elapsed_as_string = self.pretty_print_duration(self.elapsed)

    @staticmethod
    def pretty_print_duration(elapsed):
        f, s = math.modf(elapsed)
        m, s = divmod(s, 60)
        h, m = divmod(m, 60)
        return "%d:%02d:%02d.%04d" % (h, m, s, f * 10000)


def run_fpds_load(id_list):
    if not subtier_agency_list:
        load_reference_data()

    chunks = [id_list[x : x + CHUNK_SIZE] for x in range(0, len(id_list), CHUNK_SIZE)]

    for chunk in chunks:
        with Timer() as timer:
            logger.info("loading {} ids (ids {}-{})".format(len(chunk), chunk[0], chunk[-1]))
            broker_transactions = fetch_broker_objects(chunk)

            load_objects = generate_load_objects(broker_transactions)

            load_transactions(load_objects)
        logger.info("ran load in {}".format(timer.elapsed_as_string))


def load_reference_data():
    with psycopg2.connect(dsn=USASPENDING_CONNECTION_STRING) as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            sql = (
                "SELECT * FROM subtier_agency "
                "JOIN agency "
                "ON subtier_agency.subtier_agency_id = agency.subtier_agency_id"
            )

            cursor.execute(sql)
            results = cursor.fetchall()
            for result in results:
                subtier_agency_list[result["subtier_code"]] = result


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
        for key in non_boolean_column_map:
            retval[non_boolean_column_map[key]] = capitalize_if_string(broker_object[key])

    if boolean_column_map:
        for key in boolean_column_map:
            retval[boolean_column_map[key]] = false_if_null(broker_object[key])

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
    with psycopg2.connect(dsn=USASPENDING_CONNECTION_STRING) as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            load_recipient_locations(cursor, load_objects)
            load_recipients(cursor, load_objects)
            load_place_of_performance(cursor, load_objects)

            for load_object in load_objects:

                # Try to find an award for this transaction to belong to
                find_matching_award_sql = "select id from awards where generated_unique_award_id = '{}'".format(
                    load_object["generated_unique_award_id"]
                )
                cursor.execute(find_matching_award_sql)
                results = cursor.fetchall()

                # If there is an award, we still need to update it with values from its new latest transaction
                if len(results) > 0:
                    load_object["transaction_normalized"]["award_id"] = results[0][0]
                    update_award_str = "place_of_performance_id = {}, recipient_id = {}".format(
                        load_object["award"]["place_of_performance_id"], load_object["award"]["recipient_id"]
                    )
                    update_award_sql = "UPDATE awards SET {} where id = {}".format(update_award_str, results[0][0])
                    cursor.execute(update_award_sql)
                # If there is no award, we need to create one
                else:
                    columns, values, pairs = setup_load_lists(load_object, "award")
                    generate_matching_award_sql = "INSERT INTO awards {} VALUES {} RETURNING id".format(columns, values)
                    cursor.execute(generate_matching_award_sql)
                    results = cursor.fetchall()
                    load_object["transaction_normalized"]["award_id"] = results[0][0]

                # Determine if we are making a new transaction, or updating an old one
                find_matching_transaction_sql = (
                    "select transaction_id from transaction_fpds "
                    "where detached_award_proc_unique = '{}'".format(
                        load_object["transaction_fpds"]["detached_award_proc_unique"]
                    )
                )
                cursor.execute(find_matching_transaction_sql)
                results = cursor.fetchall()

                if len(results) > 0:
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


def setup_mass_load_lists(load_objects, table):
    columns = []
    values = []

    for index in range(0, len(load_objects)):
        values.append([])

    for key in load_objects[0][table].keys():
        columns.append('"{}"'.format(key))
        for index in range(0, len(load_objects)):
            val = format_value_for_sql(load_objects[index][table][key])
            values[index].append(val)

    col_string = "({})".format(",".join(map(str, columns)))
    val_string = ",".join(["({})".format(",".join(map(str, value))) for value in values])

    return col_string, val_string


def setup_load_lists(load_object, table):
    columns = []
    values = []
    update_pairs = []
    for key in load_object[table].keys():
        columns.append('"{}"'.format(key))
        val = format_value_for_sql(load_object[table][key])
        values.append(val)
        if key not in ["create_date", "created_at"]:
            update_pairs.append(" {}={}".format(key, val))

    col_string = "({})".format(",".join(map(str, columns)))
    val_string = "({})".format(",".join(map(str, values)))
    pairs_string = ",".join(update_pairs)

    return col_string, val_string, pairs_string
