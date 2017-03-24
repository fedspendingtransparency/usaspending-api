from django.db import models
import hashlib

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

    request = models.TextField(null=False, help_text="The serialized form of the POST request")
    checksum = models.CharField(max_length=256, unique=True, null=False, db_index=True, help_text="The SHA-256 checksum of the serialized POST request")
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    last_accessed = models.DateTimeField(auto_now_add=True, blank=True, null=True)

    def save(self, *args, **kwargs):
        self.checksum = hashlib.sha256(self.request).hexdigest()
        super(RequestSlugs, self).save(*args, **kwargs)

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
