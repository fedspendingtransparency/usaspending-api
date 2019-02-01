from enum import Enum


# Award id is currently defined as bigint in the database.  These are minimum
# and maximum values for bigint.  While using sys.maxsize may look appealing,
# it is based on the word size of the machine which will cause problems on
# anything other than a 64 bit machine thus making it incorrect for this purpose.
MIN_INT = -2**63     # -9223372036854775808
MAX_INT = 2**63 - 1  # +9223372036854775807


class AwardIdType(Enum):
    """
    Multi-state return value for detect_award_id_type.
    """
    internal = 1   # award_id is probably an internal (integer) award id
    generated = 2  # award_id is probably a generated_unique_award_id
    unknown = 3    # type is unknown


def detect_award_id_type(award_id):
    """
    THE PROBLEM:  We have several places in code where we need to determine
    whether a provided value is an internal award id (integer), a generated
    award id (string), or neither.

        internal award id:          123456
        also an internal award id:  "123456"
        generated unique award id:  "CONT_AW_4732_-NONE-_47QRAA18D0081_-NONE-"

        not an award id:            123.456
        also not an award id:       {"award_id": "123456"}

    At present, this is being performed a few different ways, each of which has
    its own set of problems.  Simply attempting to cast using int will truncate
    fractional bits for floats which is bad.  Checking isdigit or isdecimal
    will allow values to slip through that the database will not understand,
    for example U+0660 (Arabic-Indic Digit Zero).

    THE SOLUTION:  Here we will provide what we hope to be THE single solution
    for this problem in a nice, little, encapsulated function using a
    combination of type interrogation, hard casting, and exception handling.

    Input:
        award_id - Can be anything, but hopefully it will be something capable
        of being a valid award id.  Only strings and integers have any hope of
        being award ids at this time.

    Returns:
        recast_award_id, AwardIdType - If award_id is an internal award id, an
        integer will be returned along with AwardIdType.internal.  If award_id
        is a generated award id, a string will be returned along with
        AwardIdType.generated.  Otherwise, the original award_id and
        AwardIdType.unknown will be returned.

    Examples:
        detect_award_id_type(123456)           => 123456, AwardIdType.internal
        detect_award_id_type("123456")         => 123456, AwardIdType.internal
        detect_award_id_type("CONT_AW_-NONE-") => "CONT_AW_-NONE-", AwardIdType.generated
        detect_award_id_type(123.456)          => 123.456, AwardIdType.unknown

    IMPORTANT THING 1:  I highly recommend you use the return value in your
    queries instead of the original award_id provided to this function since
    it is being cast to an appropriate data type for you.  This eliminates odd
    situations where the award_id is successfully cast to an integer, yet the
    original value is something that the database does not understand (for
    example, there are certain unicode characters that are numbers but the
    database will not see them as such).

    IMPORTANT THING A:  We will NOT be querying the database here.  We are
    strictly attempting to discern the type of the id based on its value.
    """
    if type(award_id) in (int, str):
        try:
            _award_id = int(award_id)
            if MIN_INT <= _award_id <= MAX_INT:
                return _award_id, AwardIdType.internal
        except ValueError:
            pass
        return str(award_id).upper(), AwardIdType.generated
    return award_id, AwardIdType.unknown  # ¯\_(ツ)_/¯
