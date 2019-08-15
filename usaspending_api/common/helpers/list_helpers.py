

def safe_add(list1, list2):
    retval = list()
    if list1 is not None and list2 is not None:
        retval = list1 + list2
    if list1 is None and list2 is not None:
        retval = list2
    if list2 is None and list1 is not None:
        retval = list1
    return retval
