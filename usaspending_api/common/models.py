from django.db import models


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
