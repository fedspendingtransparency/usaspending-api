def get_date_from_datetime(date_time, **kwargs):
    """
    Pass a keyword argument called "default" if you wish to have a specific
    value returned when the date cannot be extracted from date_time, otherwise
    date_time will be returned.
    """
    try:
        return date_time.date()
    except Exception:
        return kwargs.get('default', date_time)


def test_get_date_from_datetime():
    from datetime import date, datetime

    assert get_date_from_datetime(1) == 1
    assert get_date_from_datetime(1, default=2) == 2
    assert get_date_from_datetime(1, default=None) is None

    assert get_date_from_datetime('a') == 'a'
    assert get_date_from_datetime('a', default='b') == 'b'

    assert get_date_from_datetime(datetime(2000, 1, 2)) == date(2000, 1, 2)
    assert get_date_from_datetime(datetime(2000, 1, 2), default='no') == date(2000, 1, 2)

    assert get_date_from_datetime(datetime(2000, 1, 2, 3, 4, 5)) == date(2000, 1, 2)
    assert get_date_from_datetime(datetime(2000, 1, 2, 3, 4, 5), default='maybe') == date(2000, 1, 2)
