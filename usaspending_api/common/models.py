from django.db import models
from django.contrib.postgres.fields import JSONField
from django.core.exceptions import ObjectDoesNotExist
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.csv_helpers import resolve_path_to_view, create_filename_from_options, format_path
from enum import Enum
import hashlib
import pickle
import datetime
import json


# This is an abstract model that should be the foundation for any model that
# requires data source tracking - history tracking will then be added to this
# and it will cascade down to appropriate classes
class DataSourceTrackedModel(models.Model):
    DATASOURCES = (
        ('USA', 'USAspending'),
        ('DBR', 'DATA Act Broker')
    )

    data_source = models.CharField(max_length=3, choices=DATASOURCES, null=True, help_text="The source of this entry, either Data Broker (DBR) or USASpending (USA)")  # Allowing null for the time being

    class Meta:
        abstract = True


class DeleteIfChildlessMixin(object):

    def delete_if_childless(self):
        """Deletes the instance if no child records point to it.

        For cleaning up leftover parent records after child deletions."""

        for attr_name in dir(self):
            if attr_name.endswith('_set'):
                children = getattr(self, attr_name)
                if children.exists():
                    return (0, {})  # was not an orphan

        # No child records found, so
        return self.delete()


class FiscalYear(models.Transform):
    """Allows Date and DateTime fields to support `__fy` operations

    Requires that the FY function be defined in the database
    (raw SQL is in helpers.py, run in a migration)"""
    lookup_name = 'fy'
    function = 'FY'

    @property
    def output_field(self):
        return models.IntegerField()


models.DateField.register_lookup(FiscalYear)
models.DateTimeField.register_lookup(FiscalYear)
