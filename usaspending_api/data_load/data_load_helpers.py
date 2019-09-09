import datetime

subtier_agency_list = {}  # global variable, populated by fpds_loader


def capitalize_if_string(val):
    if isinstance(val, str):
        return val.upper()
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
    elif isinstance(val, int):
        retval = "{}".format(val)
    elif isinstance(val, list):
        retval = "ARRAY[" + ",".join([format_value_for_sql(element) for element in val]) + "]"  # noqa
    elif isinstance(val, datetime.datetime):
        retval = "'{}-{}-{} {}:{}:{}'".format(val.year, val.month, val.day, val.hour, val.minute, val.second)

    return retval
