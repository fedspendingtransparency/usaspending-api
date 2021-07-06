from usaspending_api.common.containers import Bunch
from usaspending_api.references.models import ObjectClass


OBJECT_CLASSES = None


def reset_object_class_cache():
    """
    An unfortunate side effect of globals is that they don't get refreshed automatically
    for tests.  So, to keep the performance of caching object classes globally but still
    allow tests to function properly, we need a way to reset the object class cache.
    """
    global OBJECT_CLASSES
    OBJECT_CLASSES = None


def get_object_class_row(row):
    """Lookup an object class record.

    (As ``get_object_class``, but arguments are bunched into a ``row`` object.)

     Args:
         row.object_class: object class from the broker
         row.by_direct_reimbursable_fun: direct/reimbursable flag from the broker
    """
    global OBJECT_CLASSES
    if OBJECT_CLASSES is None:
        OBJECT_CLASSES = {(oc.object_class, oc.direct_reimbursable): oc for oc in ObjectClass.objects.all()}

    # Object classes are numeric strings so let's ensure the one we're passed is actually a string before we begin.
    object_class = str(row.object_class).zfill(3) if type(row.object_class) is int else row.object_class

    # As per DEV-4030, "000" object class is a special case due to common spreadsheet mangling issues.  If
    # we receive an object class that is all zeroes, convert it to "000".  This also handles the special
    # case of "0000".
    if object_class is not None and object_class == "0" * len(object_class):
        object_class = "000"

    # Alias this to cut down on line lengths a little below.
    ocdr = ObjectClass.DIRECT_REIMBURSABLE

    if len(object_class) == 4:
        # this is a 4 digit object class, first three digits should be the code and last one's a redundant 0
        if object_class[3] == "0":
            object_class = object_class[:3]
        else:
            raise ValueError(f"Invalid format for object_class={object_class}.")

    # grab direct/reimbursable information from a separate field
    try:
        direct_reimbursable = ocdr.BY_DIRECT_REIMBURSABLE_FUN_MAPPING[row.by_direct_reimbursable_fun]
    except KeyError:
        # So Broker sort of validates this data, but not really.  It warns submitters that their data
        # is bad but doesn't force them to actually fix it.  As such, we are going to just ignore
        # anything we do not recognize.  Terrible solution, but it's what we've been doing to date
        # and I don't have a better one.
        direct_reimbursable = None

    object_class = f"{object_class[:2]}.{object_class[2:]}"

    # This will throw an exception if the object class does not exist which is the new desired behavior.
    try:
        return OBJECT_CLASSES[(object_class, direct_reimbursable)]
    except KeyError:
        raise ObjectClass.DoesNotExist(
            f"Unable to find object class for object_class={object_class}, direct_reimbursable={direct_reimbursable}."
        )


def get_object_class(row_object_class, row_direct_reimbursable):
    """Lookup an object class record.

    Args:
        row_object_class: object class from the broker
        row_direct_reimbursable: direct/reimbursable flag from the broker
    """
    row = Bunch(object_class=row_object_class, by_direct_reimbursable_fun=row_direct_reimbursable)
    return get_object_class_row(row)
