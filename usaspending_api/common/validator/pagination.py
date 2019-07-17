from copy import deepcopy

from .utils import get_model_by_name


PAGINATION = [
    {"name": "page", "type": "integer", "default": 1, "min": 1},
    {"name": "limit", "type": "integer", "default": 10, "min": 1, "max": 100},
    {"name": "sort", "type": "text", "text_type": "search"},
    {"name": "order", "type": "enum", "enum_values": ("asc", "desc"), "default": "desc"},
]


for p in PAGINATION:
    p["optional"] = p.get("optional", True)
    p["key"] = p["name"]


def customize_pagination_with_sort_columns(sortable_columns, default_sort_column=None, pagination=None):
    """
    A common customization to TinyShield pagination rules is to enumerate the
    actual sort columns for validation.

    sortable_columns    - An iterable (list, tuple, set, whatever) of allowable
                          sort field names (strings).
    default_sort_column - The sort field name to use if one isn't supplied by
                          the client in the API request.
    pagination          - The pagination model to modify.  If one isn't supplied,
                          the default PAGINATION model will be used.

    Returns the resulting TinyShield model.
    """
    for s in sortable_columns:
        if type(s) is not str:
            raise TypeError("sortable_columns must be an iterable of string field names")

    models = deepcopy(PAGINATION if pagination is None else pagination)

    # Add the list of sortable columns for validation.
    sort_rule = get_model_by_name(models, "sort")
    if sort_rule is None:
        raise ValueError('"sort" rule not found in pagination model')
    sort_rule["type"] = "enum"
    sort_rule["enum_values"] = sortable_columns
    if default_sort_column is not None:
        if default_sort_column not in sortable_columns:
            raise ValueError('default_sort_column "%s" not found in sortable_columns' % default_sort_column)
        sort_rule["default"] = default_sort_column

    return models
