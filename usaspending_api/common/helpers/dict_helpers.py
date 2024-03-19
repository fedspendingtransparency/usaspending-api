from collections import OrderedDict
from typing import List

from usaspending_api.search.filters.elasticsearch.naics import NaicsCodes
from usaspending_api.search.filters.elasticsearch.tas import TasCodes
from usaspending_api.search.filters.mixins.psc import PSCCodesMixin


def upper_case_dict_values(input_dict):
    for key in input_dict:
        if isinstance(input_dict[key], str):
            input_dict[key] = input_dict[key].upper()


def order_nested_filter_tree_object(nested_object):
    """
    Filter tree searches look a bit like:
        {
          "psc_codes": {
              "require": [["Service", "B", "B5"]],
              "exclude": [["Service", "B", "B5", "B502"]]
          }
        }
    The position of the values in the innermost lists is important.  Exclude them from sorting.
    """
    if not isinstance(nested_object, dict):
        return order_nested_object(nested_object)

    # Sort the keys and sort the outer list of "require" and "exclude" but do not recurse the inner lists.
    # Everything else is handled using the default sorting behavior.
    return OrderedDict(
        [
            (
                key,
                (
                    sorted(nested_object[key])
                    if key in ("require", "exclude") and isinstance(nested_object[key], list)
                    else order_nested_object(nested_object[key])
                ),
            )
            for key in sorted(nested_object.keys())
        ]
    )


def order_nested_object(nested_object):
    """
    Simply recursively order the item. To be used for standardizing objects for JSON dumps
    """
    if isinstance(nested_object, list):
        if len(nested_object) > 0 and isinstance(nested_object[0], dict):
            # Lists of dicts aren't handled by python's sorted(), so we handle sorting manually
            sorted_subitems = []
            sort_dict = {}
            # Create a hash using keys & values
            for subitem in nested_object:
                hash_list = ["{}{}".format(key, subitem[key]) for key in sorted(list(subitem.keys()))]
                hash_str = "_".join(str(hash_list))
                sort_dict[hash_str] = order_nested_object(subitem)
            # Sort by the new hash
            for sorted_hash in sorted(list(sort_dict.keys())):
                sorted_subitems.append(sort_dict[sorted_hash])
            return sorted_subitems
        else:
            return sorted([order_nested_object(subitem) for subitem in nested_object])
    elif isinstance(nested_object, dict):
        # Filter tree values are positional and require special handling.  Some filter tree filters
        # support legacy lists so only perform special handling if the filter is the newer style dictionary.
        return OrderedDict(
            [
                (
                    key,
                    (
                        order_nested_filter_tree_object(nested_object[key])
                        if key in (NaicsCodes.underscore_name, PSCCodesMixin.underscore_name, TasCodes.underscore_name)
                        and isinstance(nested_object[key], dict)
                        else order_nested_object(nested_object[key])
                    ),
                )
                for key in sorted(nested_object.keys())
            ]
        )
    else:
        return nested_object


def update_list_of_dictionaries(to_update: List[dict], update_with: List[dict], common_term: str) -> dict:
    """
    Returns a copy of the "to_update" dictionary with the updates from the "update_with" dict.
    The "common_term" is what the two list of dictionaries will be updated around; assumes that the common_term
    should be unique to a list of dictionaries.
    """
    to_update = {val[common_term]: val for val in to_update}
    update_with = {val[common_term]: val for val in update_with}
    to_update.update(update_with)
    return [val for val in to_update.values()]
