from collections import OrderedDict, MutableMapping
from copy import deepcopy
from django.db.models import F


def delete_keys_from_dict(dictionary):
    modified_dict = OrderedDict()
    for key, value in dictionary.items():
        if not key.startswith("_"):
            if isinstance(value, MutableMapping):
                modified_dict[key] = delete_keys_from_dict(value)
            else:
                modified_dict[key] = deepcopy(value)
    return modified_dict


def split_mapper_into_qs(mapper):
    values_list = [k for k, v in mapper.items() if k == v]
    annotate_dict = OrderedDict([(v, F(k)) for k, v in mapper.items() if k != v])

    return values_list, annotate_dict
