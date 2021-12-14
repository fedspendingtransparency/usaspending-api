from django.db import models
from django.db.models import BigAutoField


class DownloadJobLookup(models.Model):
    """
    Model used so that IDs can be temporarily stored next to a Download Job ID and
    referenced for more efficient lookup than using a "IN" (or similar) statement.
    """

    id = BigAutoField(primary_key=True)
    created_at = models.DateTimeField()
    download_job_id = models.IntegerField(db_index=True)
    lookup_id = models.BigIntegerField()
    lookup_id_type = models.TextField()

    class Meta:
        db_table = "download_job_lookup"
        managed = True
