from django.db import models
from django.contrib.postgres.fields import JSONField
from django.core.exceptions import ObjectDoesNotExist
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
                raise Exception("Requested 'req' " + checksum + " does not exist or has expired.")

        # Delete any pagination data
        for item in ["page", "limit"]:
            if data.get(item, None):
                del data[item]
            if query_params.get(item, None):
                del query_params[item]

        json_request = {"data": data, "query_params": query_params}

        print(json_request)

        # Make the checksum
        checksum = hashlib.sha256(json.dumps(json_request).encode('utf-8')).hexdigest()

        # See if the checksum exists, and return that catalog. Otherwise, create it
        created = False
        request_catalog = None
        try:
            request_catalog = RequestCatalog.objects.get(request=json_request)
            request_catalog.last_accessed = datetime.datetime.now()
            request_catalog.save()
            created = False
        except ObjectDoesNotExist:
            request_catalog = RequestCatalog.objects.create(request=json_request, checksum=checksum)
            created = True

        return created, request_catalog

    def merge_requests(self, request):
        '''Merges a request with the stored values (i.e. taking pagination data from incoming request)'''
        req = self.request

        # Copy over pagination data from the incoming request, and merge it with stored search
        for item in ["page", "limit"]:
            if request.data.get(item, None):
                req["data"][item] = request.data.get(item)
            if request.query_params.get(item, None):
                req["query_params"][item] = request.query_params.get(item)

        return req

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
