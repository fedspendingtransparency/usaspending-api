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


class RequestCatalog(models.Model):
    """Stores a POST request and generates a string identifier (checksum), allowing it to be re-run via a GET request"""

    request = JSONField(null=False, help_text="The serialized form of the POST request")
    checksum = models.CharField(max_length=256, unique=True, null=False, db_index=True, help_text="The SHA-256 checksum of the serialized POST request")
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    last_accessed = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    @staticmethod
    def get_or_create_from_request(request):
        """
        This method takes a POST or GET request, and checks for its existence in the RequestCatalog
        This method returns two fields, a boolean of whether the record was created or not, and the
        RequestCatalog itself.

        If a checksum is supplied in the request, but there is no matching Request, an
        InvalidParameterException is generated
        """
        # Get the useful parts
        query_params = request.query_params.copy()
        data = request.data.copy()

        # First, check for the "req" parameter
        checksum = None
        if "req" in data:
            checksum = data["req"]
        elif "req" in query_params:
            checksum = query_params["req"]

        if checksum:
            try:
                request_catalog = RequestCatalog.objects.get(checksum=checksum)
                request_catalog.last_accessed = datetime.datetime.now()
                request_catalog.save()
                return False, request_catalog
            except ObjectDoesNotExist:
                # The checksum they sent doesn't exist
                raise InvalidParameterException("Requested 'req' '" + checksum + "' does not exist or has expired.")

        # Delete any pagination data
        for item in ["page", "limit"]:
            if data.get(item, None):
                del data[item]
            if query_params.get(item, None):
                del query_params[item]

        json_request = RequestCatalog.get_checksumable_request(data, query_params)

        # See if the request exists, and return that catalog. Otherwise, create it
        created = False
        request_catalog = None
        try:
            request_catalog = RequestCatalog.objects.get(request=json_request)
            request_catalog.last_accessed = datetime.datetime.now()
            request_catalog.save()
            created = False
        except ObjectDoesNotExist:
            # Make the checksum
            checksum = hashlib.sha256(json.dumps(json_request).encode('utf-8')).hexdigest()

            # Take the last 11
            checksum = checksum[-11:]

            # Check if we already use this checksum, if so hash the checksum again
            # The likelyhood of this happening is astronomically low, but should
            # still be accounted for since we are only using a slice of the
            # full SHA-256 checksum
            while RequestCatalog.objects.filter(checksum=checksum).exists():
                checksum = hashlib.sha256(checksum.encode('utf-8')).hexdigest()[-11:]

            request_catalog = RequestCatalog.objects.create(request=json_request, checksum=checksum)
            created = True

        return created, request_catalog

    @staticmethod
    def get_checksumable_request(data, query_params):
        '''
        Constructs a checksummable request with specific set of fields
        '''

        # Set of request parameters to save, so we don't store page/limit and we also
        # don't store any garbage that gets passed in
        storable_parameters = [
            "exclude",
            "fields",
            "order",
            "verbose",
            "filters",
            "field",
            "aggregate",
            "group",
            "date_part",
            "value",
            "mode",
            "matched_objects",
            "scope",
            "usage"
        ]

        checksumable_request = {
            "data": {k: v for (k, v) in data.items() if k in storable_parameters},
            "query_params": query_params
        }

        return checksumable_request

    class Meta:
        managed = True
        db_table = 'request_catalog'


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
