from django.db import models


# This is an abstract model that should be the foundation for any model that requires data source tracking - history
# tracking will then be added to this and it will cascade down to appropriate classes
class DataSourceTrackedModel(models.Model):
    DATASOURCES = (("USA", "USAspending"), ("DBR", "Data Broker"))

    # Allowing null for the time being
    data_source = models.TextField(
        choices=DATASOURCES,
        null=True,
        help_text="The source of this entry, either Data Broker (DBR) or USASpending (USA)",
    )

    class Meta:
        abstract = True


class DeleteIfChildlessMixin(object):
    def delete_if_childless(self):
        """Deletes the instance if no child records point to it.

        For cleaning up leftover parent records after child deletions."""

        for attr_name in dir(self):
            if attr_name.endswith("_set"):
                children = getattr(self, attr_name)
                if children.exists():
                    return (0, {})  # was not an orphan

        # No child records found, so
        return self.delete()


class FiscalYear(models.Transform):
    """Allows Date and DateTime fields to support `__fy` operations

    Requires that the FY function be defined in the database (raw SQL is in generic_helper.py, run in a migration)
    """

    lookup_name = "fy"
    function = "FY"

    @property
    def output_field(self):
        return models.IntegerField()


models.DateField.register_lookup(FiscalYear)
models.DateTimeField.register_lookup(FiscalYear)
models.TextField.register_lookup(FiscalYear)
