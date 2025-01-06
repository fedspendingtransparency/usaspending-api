from collections import OrderedDict
from collections.abc import MutableMapping
from copy import deepcopy

from django.db.models import F


def delete_keys_from_dict(dictionary):
    """
    Recursive function to remove all keys from a dictionary/OrderedDict which
    start with an underscore: "_"

    parameters:
        - dictionary:  dictionary/OrderedDict

    return:
        OrderedDict
    """
    modified_dict = OrderedDict()
    for key, value in dictionary.items():
        if not key.startswith("_"):
            if isinstance(value, MutableMapping):
                modified_dict[key] = delete_keys_from_dict(value)
            else:
                modified_dict[key] = deepcopy(value)
    return modified_dict


def split_mapper_into_qs(mapper):
    """
    Django ORM has trouble using .annotate() when the destination field conflicts with an
    existing model field, even if it's the same source field (no renaming occurring)

    Assuming there is a dictionary with model field names as keys and the target field as the value,
    Split that into two objects:
        values_list: a list of fields which you wish to retrieve without renaming
            aka when `key` == `value`
        annotate_dict: a dictionary/OrderedDict of target and source fields to rename

    parameters
        - mapper:  dictionary/OrderedDict

    return:
        - values_list: list
        -annotate_dict:  OrderedDict
    """
    values_list = [k for k, v in mapper.items() if k == v]
    annotate_dict = OrderedDict([(v, F(k)) for k, v in mapper.items() if k != v])

    return values_list, annotate_dict
