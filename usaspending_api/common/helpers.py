import datetime


def fy(raw_date):
    'Federal fiscal year corresponding to date'

    if raw_date is None:
        return None

    try:
        result = raw_date.year
        if raw_date.month > 9:
            result += 1
    except AttributeError:
        raise TypeError('{} needs year and month attributes'.format(raw_date))

    return result
