from os import environ
import psycopg2
from collections import OrderedDict

from usaspending_api.data_load.field_mappings_fpds import transaction_fpds_columns, transaction_normalized_columns, \
    transaction_normalized_functions, legal_entity_columns, legal_entity_functions, recipient_location_columns, \
    recipient_location_functions, place_of_performance_columns, place_of_performance_functions, award_functions
from usaspending_api.data_load.data_load_helpers import subtier_agency_list, format_value_for_sql

# DEFINE THESE ENVIRONMENT VARIABLES BEFORE RUNNING!
USASPENDING_CONNECTION_STRING = environ["DATABASE_URL"]
BROKER_CONNECTION_STRING = environ["DATA_BROKER_DATABASE_URL"]


def run_fpds_load(id_list):
    load_reference_data()

    broker_transactions = fetch_broker_objects(id_list)

    load_objects = generate_load_objects(broker_transactions)

    load_transactions(load_objects)


def load_reference_data():
    with psycopg2.connect(dsn=USASPENDING_CONNECTION_STRING) as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            sql = "SELECT * FROM subtier_agency " \
                  "JOIN agency " \
                  "ON subtier_agency.subtier_agency_id = agency.subtier_agency_id"

            cursor.execute(sql)
            results = cursor.fetchall()
            for result in results:
                subtier_agency_list[result["subtier_code"]] = result


def fetch_broker_objects(id_list):
    detached_award_procurements = []

    formatted_id_list = "({})".format(",".join(map(str, id_list)))

    with psycopg2.connect(dsn=BROKER_CONNECTION_STRING) as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            sql = "SELECT * from detached_award_procurement where detached_award_procurement_id in {}" \
                .format(formatted_id_list)

            cursor.execute(sql)
            results = cursor.fetchall()
            for result in results:
                detached_award_procurements.append(result)

    return detached_award_procurements


def generate_load_objects(broker_objects):
    retval = []

    for broker_object in broker_objects:
        connected_objects = {}

        # recipient_location
        recipient_location = {"is_fpds": True}
        for key in recipient_location_columns:
            recipient_location[recipient_location_columns[key]] = broker_object[key]

        for key in recipient_location_functions:
            recipient_location[key] = recipient_location_functions[key](broker_object)

        connected_objects["recipient_location"] = recipient_location

        # legal entity
        legal_entity = {"is_fpds": True}
        for key in legal_entity_columns:
            legal_entity[legal_entity_columns[key]] = broker_object[key]

        for key in legal_entity_functions:
            legal_entity[key] = legal_entity_functions[key](broker_object)

        connected_objects["legal_entity"] = legal_entity

        # place_of_performance_location
        place_of_performance_location = {"is_fpds": True}
        for key in place_of_performance_columns:
            place_of_performance_location[place_of_performance_columns[key]] = broker_object[key]

        for key in place_of_performance_functions:
            place_of_performance_location[key] = place_of_performance_functions[key](broker_object)

        connected_objects["place_of_performance_location"] = place_of_performance_location

        # matching award. NOT a real db object, but needs to be stored when making the link in load_transactions
        connected_objects["generated_unique_award_id"] = broker_object["unique_award_key"]

        # award. NOT used if a matching award is found later
        award = {"is_fpds": True}
        for key in award_functions:
            award[key] = award_functions[key](broker_object)
        connected_objects["award"] = award

        # transaction_normalized
        transaction_normalized = {"is_fpds": True}
        for key in transaction_normalized_columns:
            transaction_normalized[transaction_normalized_columns[key]] = broker_object[key]

        for key in transaction_normalized_functions:
            transaction_normalized[key] = transaction_normalized_functions[key](broker_object)

        connected_objects["transaction_normalized"] = transaction_normalized

        # transaction_fpds
        transaction_fpds = {}
        for key in transaction_fpds_columns:
            transaction_fpds[transaction_fpds_columns[key]] = broker_object[key]
        connected_objects["transaction_fpds"] = transaction_fpds

        retval.append(connected_objects)
    return retval


def load_transactions(load_objects):
    with psycopg2.connect(dsn=USASPENDING_CONNECTION_STRING) as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            for load_object in load_objects:

                columns, values, pairs = setup_load_lists(load_object, "recipient_location")
                recipient_location_sql = "INSERT INTO references_location {} VALUES {} " \
                                         "RETURNING location_id"\
                    .format(columns, values, pairs)
                cursor.execute(recipient_location_sql)
                results = cursor.fetchall()
                load_object["legal_entity"]["location_id"] = results[0][0]

                columns, values, pairs = setup_load_lists(load_object, "legal_entity")
                recipient_sql = "INSERT INTO legal_entity {} VALUES {} " \
                                "RETURNING legal_entity_id" \
                    .format(columns, values, pairs)
                cursor.execute(recipient_sql)
                results = cursor.fetchall()
                load_object["transaction_normalized"]["recipient_id"] = results[0][0]
                load_object["award"]["recipient_id"] = results[0][0]

                columns, values, pairs = setup_load_lists(load_object, "place_of_performance_location")
                pop_location_sql = "INSERT INTO references_location {} VALUES {} "\
                                   "RETURNING location_id"\
                    .format(columns, values, pairs)
                cursor.execute(pop_location_sql)
                results = cursor.fetchall()
                load_object["transaction_normalized"]["place_of_performance_id"] = results[0][0]
                load_object["award"]["place_of_performance_id"] = results[0][0]

                # Try to find an award for this transaction to belong to
                find_matching_award_sql = "select id from awards where generated_unique_award_id = \'{}\'"\
                    .format(load_object["generated_unique_award_id"])
                cursor.execute(find_matching_award_sql)
                results = cursor.fetchall()

                # If there is an award, we still need to update it with values from its new latest transaction
                if len(results) > 0:
                    load_object["transaction_normalized"]["award_id"] = results[0][0]
                    update_award_str = "place_of_performance_id = {}, recipient_id = {}"\
                        .format(load_object["award"]["place_of_performance_id"], load_object["award"]["recipient_id"])
                    update_award_sql = "UPDATE awards SET {} where id = {}".format(update_award_str, results[0][0])
                    cursor.execute(update_award_sql)
                # If there is no award, we need to create one
                else:
                    columns, values = setup_load_lists(load_object, "award")
                    generate_matching_award_sql = "INSERT INTO awards {} VALUES {} RETURNING id"\
                        .format(columns, values)
                    cursor.execute(generate_matching_award_sql)
                    results = cursor.fetchall()
                    load_object["transaction_normalized"]["award_id"] = results[0][0]

                # Determine if we are making a new transaction, or updating an old one
                find_matching_transaction_sql = "select id from transaction_normalized " \
                                                "where transaction_unique_id = \'{}\'"\
                    .format(load_object["transaction_normalized"]["transaction_unique_id"])
                cursor.execute(find_matching_transaction_sql)
                results = cursor.fetchall()

                if len(results) > 0:
                    columns, values, pairs = setup_load_lists(load_object, "transaction_normalized")
                    load_object["transaction_fpds"]["transaction_id"] = results[0][0]
                    load_object["award"]["latest_tranaction_id"] = results[0][0]
                    transaction_normalized_sql = "UPDATE transaction_normalized SET {} " \
                                                 "where transaction_unique_id = \'{}\'" \
                        .format(pairs, load_object["transaction_normalized"]["transaction_unique_id"])
                    cursor.execute(transaction_normalized_sql)

                    columns, values, pairs = setup_load_lists(load_object, "transaction_fpds")
                    transaction_fpds_sql = "UPDATE transaction_fpds SET {} " \
                                           "where transaction_id = {}" \
                        .format(pairs, load_object["transaction_fpds"]["transaction_id"])
                    cursor.execute(transaction_fpds_sql)

                    print("updated fpds transaction {}".format(results[0][0]))
                else:
                    columns, values, pairs = setup_load_lists(load_object, "transaction_normalized")
                    transaction_normalized_sql = "INSERT INTO transaction_normalized {} VALUES {} " \
                                                 "RETURNING id"\
                        .format(columns, values)
                    cursor.execute(transaction_normalized_sql)
                    results = cursor.fetchall()
                    load_object["transaction_fpds"]["transaction_id"] = results[0][0]
                    load_object["award"]["latest_tranaction_id"] = results[0][0]

                    columns, values, pairs = setup_load_lists(load_object, "transaction_fpds")
                    transaction_fpds_sql = "INSERT INTO transaction_fpds {} VALUES {} " \
                                           "RETURNING transaction_id" \
                        .format(columns, values)
                    cursor.execute(transaction_fpds_sql)
                    results = cursor.fetchall()

                    print("created fpds transaction {}".format(results[0][0]))

                # No matter what, we need to go back and update the award's latest transaction to the award we just made
                update_award_lastest_transaction_sql = "UPDATE awards SET latest_transaction_id = {} where id = {}"\
                    .format(results[0][0], load_object["award"]["latest_tranaction_id"])
                cursor.execute(update_award_lastest_transaction_sql)


def setup_load_lists(load_object, table):
    columns = []
    values = []
    update_pairs = []
    for key in OrderedDict(load_object[table]).keys():
        columns.append(key)
        val = format_value_for_sql(load_object[table][key])
        values.append(val)
        update_pairs.append(" {}={}".format(key, val))

    col_string = "({})".format(",".join(map(str, columns)))
    val_string = "({})".format(",".join(map(str, values)))
    pairs_string = ",".join(update_pairs)

    return col_string, val_string, pairs_string

