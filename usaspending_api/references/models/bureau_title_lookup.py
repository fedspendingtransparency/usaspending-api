from django.db import models


class BureauTitleLookup(models.Model):
    federal_account_code = models.TextField(primary_key=True)
    bureau_title = models.TextField()
    bureau_slug = models.TextField()

    class Meta:
        managed = True
        db_table = "bureau_title_lookup"
