from os import environ
import psycopg2
from collections import OrderedDict

from usaspending_api.data_load.field_mappings_fpds import transaction_fpds_columns, transaction_normalized_columns, \
    transaction_normalized_functions, legal_entity_columns, legal_entity_functions, recipient_location_columns, \
    recipient_location_functions
from usaspending_api.data_load.reference_values import subtier_agency_list

# DEFINE THESE ENVIRONMENT VARIABLES BEFORE RUNNING!
USASPENDING_CONNECTION_STRING = environ["DATABASE_URL"]
BROKER_CONNECTION_STRING = environ["DATA_BROKER_DATABASE_URL"]

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

def run_fpds_load(id_list):
    load_reference_data()

    broker_transactions = fetch_broker_objects(id_list)

    load_objects = generate_load_objects(broker_transactions)

    load_transactions(load_objects)


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

        # references_location
        recipient_location = {"is_fpds": True}
        for key in recipient_location_columns:
            recipient_location[recipient_location_columns[key]] = broker_object[key]

        for key in recipient_location_functions:
            recipient_location[key] = recipient_location_functions[key](broker_object)

        connected_objects["references_location"] = recipient_location

        # legal entity
        legal_entity = {"is_fpds": True}
        for key in legal_entity_columns:
            legal_entity[legal_entity_columns[key]] = broker_object[key]

        for key in legal_entity_functions:
            legal_entity[key] = legal_entity_functions[key](broker_object)

        connected_objects["legal_entity"] = legal_entity

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
            transaction_fpds[key] = broker_object[key]
        connected_objects["transaction_fpds"] = transaction_fpds

        retval.append(connected_objects)
    return retval


def load_transactions(load_objects):
    with psycopg2.connect(dsn=USASPENDING_CONNECTION_STRING) as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            for load_object in load_objects:
                columns, values = setup_load_lists(load_object, "references_location")

                recipient_location_sql = "INSERT INTO references_location {} VALUES {} RETURNING location_id"\
                    .format(columns, values)
                cursor.execute(recipient_location_sql)
                results = cursor.fetchall()

                load_object["legal_entity"]["location_id"] = results[0][0]

                columns, values = setup_load_lists(load_object, "legal_entity")

                recipient_location_sql = "INSERT INTO legal_entity {} VALUES {} RETURNING legal_entity_id" \
                    .format(columns, values)
                cursor.execute(recipient_location_sql)
                results = cursor.fetchall()
                print("Behold! I have made a new legal entity with a primary key of {}!".format(results[0][0]))


def setup_load_lists(load_object, table):
    columns = []
    values = []
    for key in OrderedDict(load_object[table]).keys():
        columns.append(key)
        val = load_object[table][key]
        if isinstance(val, str):
            values.append("\'{}\'".format(val))
        elif val is None:
            values.append("null")
        elif isinstance(val, list):
            print(",".join(val))
            values.append("\'{" + (",".join(val)) + "}\'")  # noqa
        else:
            values.append(val)

    col_string = "({})".format(",".join(map(str, columns)))
    val_string = "({})".format(",".join(map(str, values)))

    return col_string, val_string

