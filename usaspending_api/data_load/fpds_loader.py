from os import environ
import psycopg2

from usaspending_api.data_load.field_mappings_fpds import transaction_fpds_columns, transaction_normalized_columns, transaction_normalized_functions

# DEFINE THESE ENVIRONMENT VARIABLES BEFORE RUNNING!
USASPENDING_CONNECTION_STRING = environ["DATABASE_URL"]
BROKER_CONNECTION_STRING = environ["DATA_BROKER_DATABASE_URL"]

subtier_agency_list = {}

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

        # transaction_fpds values
        transaction_fpds = {}
        for key in transaction_fpds_columns:
            transaction_fpds[key] = broker_object[key]
        connected_objects["transaction_fpds"] = transaction_fpds

        # transaction_normalized values
        transaction_normalized = {"is_fpds": True}
        for key in transaction_normalized_columns:
            transaction_normalized[transaction_normalized_columns[key]] = broker_object[key]

        for key in transaction_normalized_functions:
            transaction_normalized[key] = transaction_normalized_functions[key](broker_object)

        awarding_agency = subtier_agency_list.get(broker_object["awarding_sub_tier_agency_c"], None)
        if awarding_agency is not None:
            transaction_normalized["awarding_agency_id"] = awarding_agency["id"]
        else:
            transaction_normalized["awarding_agency_id"] = None

        funding_agency = subtier_agency_list.get(broker_object["funding_sub_tier_agency_co"], None)
        if funding_agency is not None:
            transaction_normalized["funding_agency_id"] = funding_agency["id"]
        else:
            transaction_normalized["funding_agency_id"] = None

        connected_objects["transaction_normalized"] = transaction_normalized

        retval.append(connected_objects)
    return retval


def load_transactions(load_objects):
    with psycopg2.connect(dsn=USASPENDING_CONNECTION_STRING) as connection:
        with connection.cursor(cursor_factory=psycopg2.extras.DictCursor) as cursor:
            for load_object in load_objects:
                print(load_object["transaction_normalized"]["awarding_agency_id"])
