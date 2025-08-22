from usaspending_api.references.models import ProgramActivityPark


PROGRAM_ACTIVITY_PARK = None


def get_program_activity_park(row: dict) -> ProgramActivityPark | None:
    """Encapsulate fetching PARK to utilize a 'poor man's caching' pattern"""
    global PROGRAM_ACTIVITY_PARK
    if not PROGRAM_ACTIVITY_PARK:  # testing for falsy values instead of just null
        PROGRAM_ACTIVITY_PARK = {park.code: park for park in ProgramActivityPark.objects.all()}

    program_activity_park = row["program_activity_reporting_key"]
    if not program_activity_park:
        return None
    try:
        return PROGRAM_ACTIVITY_PARK[program_activity_park]
    except KeyError:
        raise ProgramActivityPark.DoesNotExist(f"Unable to find PARK for '{program_activity_park}'.")
