from usaspending_api.etl.transaction_loaders.data_load_helpers import (
    format_insert_or_update_column_sql,
    format_bulk_insert_list_column_sql,
)


def bulk_insert_recipient_location(cursor, load_objects):
    columns, values = format_bulk_insert_list_column_sql(cursor, load_objects, "recipient_location")
    recipient_location_sql = "INSERT INTO references_location {} VALUES {} RETURNING location_id;".format(
        columns, values
    )

    cursor.execute(recipient_location_sql)
    return [tup[0] for tup in cursor.fetchall()]


def bulk_insert_recipient(cursor, load_objects):
    columns, values = format_bulk_insert_list_column_sql(cursor, load_objects, "legal_entity")
    recipient_sql = "INSERT INTO legal_entity {} VALUES {} RETURNING legal_entity_id;".format(columns, values)

    cursor.execute(recipient_sql)
    return [tup[0] for tup in cursor.fetchall()]


def bulk_insert_place_of_performance(cursor, load_objects):
    columns, values = format_bulk_insert_list_column_sql(cursor, load_objects, "place_of_performance_location")
    recipient_sql = "INSERT INTO references_location {} VALUES {} RETURNING location_id;".format(columns, values)

    cursor.execute(recipient_sql)
    return [tup[0] for tup in cursor.fetchall()]


def insert_award(cursor, load_object):
    columns, values, pairs = format_insert_or_update_column_sql(cursor, load_object, "award")
    generate_matching_award_sql = "INSERT INTO awards {} VALUES {} RETURNING id".format(columns, values)
    cursor.execute(generate_matching_award_sql)
    return cursor.fetchall()[0][0]


def update_transaction_fpds(cursor, load_object):
    columns, values, pairs = format_insert_or_update_column_sql(cursor, load_object, "transaction_fpds")
    transaction_fpds_sql = "UPDATE transaction_fpds SET {} where detached_award_procurement_id = {}".format(
        pairs, load_object["transaction_fpds"]["detached_award_procurement_id"]
    )
    cursor.execute(transaction_fpds_sql)


def update_transaction_normalized(cursor, load_object):
    columns, values, pairs = format_insert_or_update_column_sql(cursor, load_object, "transaction_normalized")
    transaction_normalized_sql = "UPDATE transaction_normalized SET {} where id  = '{}'".format(
        pairs, load_object["transaction_fpds"]["transaction_id"]
    )
    cursor.execute(transaction_normalized_sql)


def insert_transaction_normalized(cursor, load_object):
    columns, values, pairs = format_insert_or_update_column_sql(cursor, load_object, "transaction_normalized")
    transaction_normalized_sql = "INSERT INTO transaction_normalized {} VALUES {} RETURNING id".format(columns, values)
    cursor.execute(transaction_normalized_sql)
    created_transaction_normalized = cursor.fetchall()
    transaction_normalized_id = created_transaction_normalized[0][0]
    return transaction_normalized_id


def insert_transaction_fpds(cursor, load_object):
    columns, values, pairs = format_insert_or_update_column_sql(cursor, load_object, "transaction_fpds")
    transaction_fpds_sql = "INSERT INTO transaction_fpds {} VALUES {} RETURNING transaction_id".format(columns, values)
    cursor.execute(transaction_fpds_sql)
    created_transaction_fpds = cursor.fetchall()
    return created_transaction_fpds
