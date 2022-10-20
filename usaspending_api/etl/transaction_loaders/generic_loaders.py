from usaspending_api.etl.transaction_loaders.data_load_helpers import format_insert_or_update_column_sql


def insert_award(cursor, load_object):
    columns, values, pairs = format_insert_or_update_column_sql(cursor, load_object, "award")
    generate_matching_award_sql = "INSERT INTO awards {} VALUES {} RETURNING id".format(columns, values)
    cursor.execute(generate_matching_award_sql)
    return cursor.fetchall()[0][0]
