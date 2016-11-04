from django.db import models


# This is an abstract model that should be the foundation for any model that
# requires data source tracking - history tracking will then be added to this
# and it will cascade down to appropriate classes
class DataSourceTrackedModel(models.Model):
    DATASOURCES = (
        ('USA', 'USAspending'),
        ('DBR', 'DATA Act Broker')
    )

    data_source = models.CharField(max_length=3, choices=DATASOURCES, null=True)  # Allowing null for the time being

    class Meta:
        abstract = True
