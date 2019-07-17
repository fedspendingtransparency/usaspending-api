from collections import OrderedDict


def upper_case_dict_values(input_dict):
    for key in input_dict:
        if isinstance(input_dict[key], str):
            input_dict[key] = input_dict[key].upper()


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
        return OrderedDict([(key, order_nested_object(nested_object[key])) for key in sorted(nested_object.keys())])
    else:
        return nested_object
