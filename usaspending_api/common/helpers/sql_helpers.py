import os
import logging

from django.db.models import Func, IntegerField

from usaspending_api.common.exceptions import InvalidParameterException


logger = logging.getLogger('console')


def read_sql_file(file_path):
    # Read in SQL file and extract commands into a list
    _, file_extension = os.path.splitext(file_path)

    if file_extension != '.sql':
        raise InvalidParameterException("Invalid file provided. A file with extension '.sql' is required.")

    # Open and read the file as a single buffer
    fd = open(file_path, 'r')
    sql_file = fd.read()
    fd.close()

    # all SQL commands (split on ';') and trimmed for whitespaces
    return [command.strip() for command in sql_file.split(';') if command]


class FiscalMonth(Func):
    function = "EXTRACT"
    template = "%(function)s(MONTH from (%(expressions)s) + INTERVAL '3 months')"
    output_field = IntegerField()


class FiscalQuarter(Func):
    function = "EXTRACT"
    template = "%(function)s(QUARTER from (%(expressions)s) + INTERVAL '3 months')"
    output_field = IntegerField()


class FiscalYear(Func):
    function = "EXTRACT"
    template = "%(function)s(YEAR from (%(expressions)s) + INTERVAL '3 months')"
    output_field = IntegerField()
