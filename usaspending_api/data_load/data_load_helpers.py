subtier_agency_list = {}  # global variable, populated by fpds_loader


def format_value_for_sql(val):
    if isinstance(val, str):
        return "\'{}\'".format(val.replace("'", "''").replace("\"","\"\""))
    elif val is None:
        return "null"
    elif isinstance(val, list):
        return "\'{" + (",".join(val)) + "}\'"  # noqa
    else:
        return val
