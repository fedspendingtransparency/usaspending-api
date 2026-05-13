from django.db import models


class CDFVersionTracking(models.Model):
    """Tracks the last-processed version of a Delta Lake table using the Change Data Feed (CDF)"""

    table_schema = models.TextField()
    table_name = models.TextField()
    last_processed_version = models.BigIntegerField()
    last_commit_timestamp = models.DateTimeField()
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        managed = True
        db_table = 'cdf_version_tracking'

        constraints = [
            models.UniqueConstraint(
                fields=['table_schema', 'table_name'],
                name='unique_table_schema_and_table_name',
            )
        ]
