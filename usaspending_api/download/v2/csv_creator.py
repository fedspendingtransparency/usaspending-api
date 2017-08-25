import csv
from usaspending_api.download.v2.download_column_lookups import transaction_d1_columns, transaction_d2_columns, transaction_columns_unique


def create_transaction_csv(columns, transaction_contract_queryset, transaction_assistance_queryset):
    if columns == {}:
        columns = transaction_columns_unique

    # counts are used multiple times, so they are stored in a variable to prevent multiple look ups.
    tc_count = transaction_contract_queryset.count()
    ta_count = transaction_assistance_queryset.count()

    # d1 - contracts
    if tc_count != 0:

        # make tc file
        with open('test.csv', 'wb') as csvfile:
            tcwriter = csv.writer(csvfile)

            for tc in transaction_contract_queryset:
                # craft row
                csv_d1_row = []
                for column in columns:
                    # if column is special (its a function)
                    if False:
                        pass
                    else:
                        # dynamically iterate through the db to find the var we need
                        path_to_var = tc[transaction_d1_columns[column]].split('__')
                        obj = tc
                        for lvl in path_to_var:
                            obj = obj[lvl]
                            if obj is None:
                                print("OBJECT IN TRANSACTION_d1_columns " + tc[transaction_d1_columns[column]] +
                                      " returned NULL at " + lvl)
                                obj = ""
                                continue
                        # add obj to row in csv.
                        csv_d1_row.append(obj)
                # add row to csv
                tcwriter.writerow(csv_d1_row)

    # d2 - assistance
    if ta_count != 0:
        # make ta file
        for ta in transaction_assistance_queryset:
            # craft row
            pass

    return {
            "total_size": -1,
            "total_columns": len(columns),
            "total_rows": tc_count + ta_count,
            "file_name": "test_file"
    }


def create_award_csv(columns, award_queryset):

    return {
            "total_size": -1,
            "total_columns": 0,
            "total_rows": 0,
            "file_name": "test_file"
    }
