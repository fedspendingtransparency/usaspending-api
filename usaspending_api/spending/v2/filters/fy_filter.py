from datetime import datetime

from usaspending_api.common.exceptions import InvalidParameterException


def fy_filter(fy, now):

    # Define fiscal reporting quarters
    q1_start = datetime.strptime('10-01', '%m-%d').strftime('%m-%d')
    q1_end = datetime.strptime('12-31', '%m-%d').strftime('%m-%d')
    q2_start = datetime.strptime('01-01', '%m-%d').strftime('%m-%d')
    q2_end = datetime.strptime('03-31', '%m-%d').strftime('%m-%d')
    q3_start = datetime.strptime('04-01', '%m-%d').strftime('%m-%d')
    q3_end = datetime.strptime('06-30', '%m-%d').strftime('%m-%d')
    q4_end = datetime.strptime('09-30', '%m-%d').strftime('%m-%d')

    # Validate fiscal year and return fiscal_quarter and fiscal_date fro results
    try:
        year = datetime.strptime(str(fy), '%Y').strftime('%Y-')
        fiscal_date = datetime.strptime(str(now), '%Y-%m-%d').strftime('%m-%d')
        if q1_start <= fiscal_date <= q1_end:
            fiscal_date = str(year) + str(q4_end)
            fiscal_quarter = '3'
        elif q2_start <= fiscal_date <= q2_end:
            year = int(year) - 1
            fiscal_date = str(year) + str(q1_end)
            fiscal_quarter = '4'
        elif q3_start <= fiscal_date <= q3_end:
            fiscal_date = str(year) + str(q2_end)
            fiscal_quarter = '1'
        else:
            fiscal_date = str(year) + str(q3_end)
            fiscal_quarter = '2'
        return fiscal_date, fiscal_quarter
    except ValueError:
        raise InvalidParameterException('Incorrect or Missing fiscal year: YYYY')
