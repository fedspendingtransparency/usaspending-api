from datetime import datetime

from usaspending_api.common.exceptions import InvalidParameterException


def fy_filter(now):

    # Define fiscal quarters
    q1_start = datetime.strptime('10-01', '%m-%d').strftime('%m-%d')
    q1_end = datetime.strptime('12-31', '%m-%d').strftime('%m-%d')
    q2_start = datetime.strptime('01-01', '%m-%d').strftime('%m-%d')
    q2_end = datetime.strptime('03-31', '%m-%d').strftime('%m-%d')
    q3_start = datetime.strptime('04-01', '%m-%d').strftime('%m-%d')
    q3_end = datetime.strptime('06-30', '%m-%d').strftime('%m-%d')
    q4_end = datetime.strptime('09-30', '%m-%d').strftime('%m-%d')

    # Evaluate current date
    year = datetime.strptime(str(now), '%Y-%m-%d').strftime('%Y-')
    fiscal_date = datetime.strptime(str(now), '%Y-%m-%d').strftime('%m-%d')
    if q1_start <= fiscal_date <= q1_end:
        fy = str(year) + str(q4_end)
    elif q2_start <= fiscal_date <= q2_end:
        year = int(year) - 1
        fy = str(year) + str(q1_end)
    elif q3_start <= fiscal_date <= q3_end:
        fy = str(year) + str(q2_end)
    else:
        fy = str(year) + str(q3_end)
    return fy


def validate_fy(fy):
    # Define fiscal quarters
    q1_start = datetime.strptime('10-01', '%m-%d').strftime('%m-%d')
    q1_end = datetime.strptime('12-31', '%m-%d').strftime('%m-%d')
    q2_start = datetime.strptime('01-01', '%m-%d').strftime('%m-%d')
    q2_end = datetime.strptime('03-31', '%m-%d').strftime('%m-%d')
    q3_start = datetime.strptime('04-01', '%m-%d').strftime('%m-%d')
    q3_end = datetime.strptime('06-30', '%m-%d').strftime('%m-%d')
    q4_end = datetime.strptime('09-30', '%m-%d').strftime('%m-%d')

    # Validate fiscal year and use most recent quarter
    try:
        year = datetime.strptime(fy, '%Y')
        year = year.strftime('%Y-')
        fiscal_date = datetime.strptime(str(datetime.now().date()), '%Y-%m-%d').strftime('%m-%d')
        if q1_start <= fiscal_date <= q1_end:
            fy = str(year) + str(q4_end)
        elif q2_start <= fiscal_date <= q2_end:
            year = int(year) - 1
            fy = str(year) + str(q1_end)
        elif q3_start <= fiscal_date <= q3_end:
            fy = str(year) + str(q2_end)
        else:
            fy = str(year) + str(q3_end)
        return fy
    except ValueError:
        raise InvalidParameterException('Incorrect fiscal year format, should be four digit year: YYYY')
