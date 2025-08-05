def create_fiscal_year_filter(year):
    return [{"start_date": "{}-10-01".format(int(year) - 1), "end_date": "{}-09-30".format(year)}]
