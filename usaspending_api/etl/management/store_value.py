def store_value(model_instance_or_dict, field, value, reverse=None):
    # turn datetimes into dates
    if field.endswith('date') and isinstance(value, str):
        try:
            value = parser.parse(value).date()
        except (TypeError, ValueError):
            pass

    if reverse and reverse.search(field):
        try:
            value = -1 * Decimal(value)
        except TypeError:
            pass

    if isinstance(model_instance_or_dict, dict):
        model_instance_or_dict[field] = value
    else:
        setattr(model_instance_or_dict, field, value)