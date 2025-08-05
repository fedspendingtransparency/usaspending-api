import logging
from psycopg2.extras import DictCursor
from psycopg2 import Error
from django.db import connection

from usaspending_api.etl.transaction_loaders.field_mappings_fpds import (
    transaction_fpds_nonboolean_columns,
    transaction_normalized_nonboolean_columns,
    transaction_normalized_functions,
    award_nonboolean_columns,
    award_functions,
    transaction_fpds_boolean_columns,
    transaction_fpds_functions,
    all_broker_columns,
)
from usaspending_api.etl.transaction_loaders.data_load_helpers import capitalize_if_string, false_if_null
from usaspending_api.etl.transaction_loaders.generic_loaders import (
    update_transaction_fpds,
    update_transaction_normalized,
    insert_transaction_normalized,
    insert_transaction_fpds,
    insert_award,
)
from usaspending_api.common.helpers.timing_helpers import ConsoleTimer as Timer


logger = logging.getLogger("script")

failed_ids = []


def delete_stale_fpds(detached_award_procurement_ids):
    """
    Removed transaction_fpds and transaction_normalized records matching any of the
    provided detached_award_procurement_id list
    Returns list of awards touched
    """
    if not detached_award_procurement_ids:
        return []

    ids_to_delete = ",".join([str(id) for ids in detached_award_procurement_ids.values() for id in ids])
    logger.debug(f"Obtained these delete record IDs: [{ids_to_delete}]")

    with connection.cursor() as cursor:
        cursor.execute(
            f"select transaction_id from transaction_fpds where detached_award_procurement_id in ({ids_to_delete})"
        )
        # assumes that this won't be too many IDs and lead to degraded performance or require too much memory
        transaction_normalized_ids = [str(row[0]) for row in cursor.fetchall()]

        if not transaction_normalized_ids:
            return []

        txn_id_str = ",".join(transaction_normalized_ids)

        cursor.execute(f"select distinct award_id from transaction_normalized where id in ({txn_id_str})")
        awards_touched = cursor.fetchall()

        # Set backreferences from Awards to Transaction Normalized to null. These FKs will be updated later
        cursor.execute(
            "update award_search set latest_transaction_id = null, earliest_transaction_id = null "
            "where latest_transaction_id in ({ids}) or earliest_transaction_id in ({ids}) "
            "returning id".format(ids=txn_id_str)
        )
        deleted_awards = cursor.fetchall()
        logger.info(f"{len(deleted_awards):,} awards were unlinked from transactions due to pending deletes")

        cursor.execute(f"delete from transaction_fpds where transaction_id in ({txn_id_str}) returning transaction_id")
        deleted_fpds = set(cursor.fetchall())

        cursor.execute(f"delete from transaction_search where transaction_id in ({txn_id_str})")

        cursor.execute(f"delete from transaction_normalized where id in ({txn_id_str}) returning id")
        deleted_transactions = set(cursor.fetchall())

        if deleted_transactions != deleted_fpds:
            msg = "Delete Mismatch! Counts of transaction_normalized ({}) and transaction_fpds ({}) deletes"
            raise RuntimeError(msg.format(len(deleted_transactions), len(deleted_fpds)))

        logger.info(f"{len(deleted_transactions):,} transactions deleted")

        return awards_touched


def load_fpds_transactions(chunk):
    """
    Run transaction load for the provided ids. This will create any new rows in other tables to support the transaction
    data, but does NOT update "secondary" award values like total obligations or C -> D linkages.

    returns ids for each award touched
    """
    with Timer() as timer:
        retval = []
        if chunk:
            broker_transactions = _extract_broker_objects(chunk)
            if broker_transactions:
                load_objects = _transform_objects(broker_transactions)

                retval = _load_transactions(load_objects)
    logger.info("batch completed in {}".format(timer.as_string(timer.elapsed)))
    return retval


def _extract_broker_objects(id_list):

    connection.ensure_connection()
    with connection.connection.cursor(cursor_factory=DictCursor) as cursor:
        sql = "SELECT {} from source_procurement_transaction where detached_award_procurement_id in %s".format(
            ",".join(all_broker_columns())
        )
        cursor.execute(sql, (tuple(id_list),))

        results = cursor.fetchall()

    return results


def _create_load_object(broker_object, non_boolean_column_map, boolean_column_map, function_map):
    retval = {}
    if non_boolean_column_map:
        retval.update(
            {non_boolean_column_map[key]: capitalize_if_string(broker_object[key]) for key in non_boolean_column_map}
        )

    if boolean_column_map:
        retval.update({boolean_column_map[key]: false_if_null(broker_object[key]) for key in boolean_column_map})

    if function_map:
        retval.update({key: func(broker_object) for key, func in function_map.items()})

    return retval


def _transform_objects(broker_objects):
    retval = []

    for broker_object in broker_objects:
        connected_objects = {
            # award. NOT used if a matching award is found later
            "award": _create_load_object(broker_object, award_nonboolean_columns, None, award_functions),
            "transaction_normalized": _create_load_object(
                broker_object, transaction_normalized_nonboolean_columns, None, transaction_normalized_functions
            ),
            "transaction_fpds": _create_load_object(
                broker_object,
                transaction_fpds_nonboolean_columns,
                transaction_fpds_boolean_columns,
                transaction_fpds_functions,
            ),
        }
        retval.append(connected_objects)

    return retval


def _load_transactions(load_objects):
    """returns ids for each award touched"""
    ids_of_awards_created_or_updated = set()
    connection.ensure_connection()
    with connection.connection.cursor(cursor_factory=DictCursor) as cursor:

        # Handle transaction-to-award relationship for each transaction to be loaded
        for load_object in load_objects:
            try:
                # AWARD GET OR CREATE
                award_id = _matching_award(cursor, load_object)
                if not award_id:
                    # If there is no award, we need to create one
                    award_id = insert_award(cursor, load_object)

                load_object["transaction_normalized"]["award_id"] = award_id
                ids_of_awards_created_or_updated.add(award_id)

                # TRANSACTION UPSERT
                transaction_id = _lookup_existing_transaction(cursor, load_object)
                if transaction_id:
                    # Inject the Primary Key of transaction_normalized+transaction_fpds that was found, so that the
                    # following updates can find it to update
                    load_object["transaction_fpds"]["transaction_id"] = transaction_id
                    _update_fpds_transaction(cursor, load_object, transaction_id)
                else:
                    # If there is no transaction we create a new one.
                    transaction_id = _insert_fpds_transaction(cursor, load_object)
                load_object["transaction_fpds"]["transaction_id"] = transaction_id
                load_object["award"]["latest_transaction_id"] = transaction_id

            except Error as e:
                logger.error(
                    f"load failed for Broker ids {load_object['transaction_fpds']['detached_award_procurement_id']}!"
                    f"\nDetails: {e.pgerror}"
                )
                failed_ids.append(load_object["transaction_fpds"]["detached_award_procurement_id"])

    return list(ids_of_awards_created_or_updated)


def _matching_award(cursor, load_object):
    """Try to find an award for this transaction to belong to by unique_award_key"""
    find_matching_award_sql = "select id from vw_awards where generated_unique_award_id = '{}'".format(
        load_object["transaction_fpds"]["unique_award_key"]
    )
    cursor.execute(find_matching_award_sql)
    results = cursor.fetchall()
    return results[0][0] if results else None


def _lookup_existing_transaction(cursor, load_object):
    """find existing fpds transaction, if any"""
    find_matching_transaction_sql = (
        "select transaction_id from vw_transaction_fpds "
        "where detached_award_proc_unique = '{}'".format(load_object["transaction_fpds"]["detached_award_proc_unique"])
    )
    cursor.execute(find_matching_transaction_sql)
    results = cursor.fetchall()
    return results[0][0] if results else None


def _update_fpds_transaction(cursor, load_object, transaction_id):
    update_transaction_fpds(cursor, load_object)
    update_transaction_normalized(cursor, load_object)
    logger.debug("updated fpds transaction {}".format(transaction_id))


def _insert_fpds_transaction(cursor, load_object):
    # transaction_normalized and transaction_fpds should be one-to-one
    transaction_normalized_id = insert_transaction_normalized(cursor, load_object)

    # Inject the Primary Key of transaction_normalized row that this record is mapped to in the one-to-one relationship
    load_object["transaction_fpds"]["transaction_id"] = transaction_normalized_id

    transaction_fpds_id = insert_transaction_fpds(cursor, load_object)
    logger.debug("created fpds transaction {}".format(transaction_fpds_id))
    return transaction_fpds_id
