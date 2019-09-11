import datetime
import time
import math

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
