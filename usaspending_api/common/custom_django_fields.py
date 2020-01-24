from django.db import models


class NumericField(models.Field):
    """default numeric type for Postgres"""

    def db_type(self, connection):
        return "numeric"


class NaiveTimestampField(models.Field):
    """timestamp column type without timezone"""

    def db_type(self, connection):
        return "timestamp without time zone"


class BooleanFieldWithDefault(models.Field):
    """Django is broken, default value ignored in BooleanField"""

    def db_type(self, connection):
        return "boolean NOT NULL default false"
