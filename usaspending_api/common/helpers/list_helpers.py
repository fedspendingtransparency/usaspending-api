

def safe_add(list1, list2):
    if list1 is not None and list2 is not None:
        return list1 + list2
    if list1 is None and list2 is not None:
        return list2
    if list2 is None and list1 is not None:
        return list1
    return list()