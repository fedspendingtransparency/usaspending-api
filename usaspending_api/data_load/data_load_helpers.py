import datetime

subtier_agency_list = {}  # global variable, populated by fpds_loader


def capitalize_if_string(val):
    try:
        return val.upper()
    except AttributeError:
        return val


def false_if_null(val):
    if val is None:
        return False
    return val


# TODO: replace this with cursor.morgify() in some way that doesn't need a live connection passed around everywhere
def format_value_for_sql(val):
    retval = val
    if isinstance(val, str):
        retval = "'{}'".format(val.replace("'", "''").replace('"', '""'))
    elif val is None:
        retval = "null"
    elif isinstance(val, int) or isinstance(val, float):
        retval = str(val)
    elif isinstance(val, list):
        if val:
            retval = "ARRAY[{}]".format(",".join([format_value_for_sql(element) for element in val]))
        else:
            retval = "'{}'"
    elif isinstance(val, datetime.datetime):
        retval = "'{}-{}-{} {}:{}:{}'".format(val.year, val.month, val.day, val.hour, val.minute, val.second)

    return retval


def setup_mass_load_lists(load_objects, table):
    values = []

    keys = load_objects[0][table].keys()

    columns = ['"{}"'.format(key) for key in load_objects[0][table].keys()]

    for load_object in load_objects:
        val = [format_value_for_sql(load_object[table][key]) for key in keys]
        values.append(val)

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
