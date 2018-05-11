from django.db import models


class StateData(models.Model):
    """
    Model representing State Data/Year
    """
    id = models.TextField(primary_key=True)
    fips = models.TextField(db_index=True)
    code = models.TextField()
    name = models.TextField()
    type = models.TextField()
    year = models.IntegerField(db_index=True)
    population = models.BigIntegerField(null=True, blank=True)
    pop_source = models.TextField(null=True, blank=True)
    median_household_income = models.DecimalField(null=True, blank=True, decimal_places=2, max_digits=21)
    mhi_source = models.TextField(null=True, blank=True)  # median household income source

    class Meta:
        db_table = 'state_data'

    def save(self, *args, **kwargs):
        self.fips = self.fips.zfill(2)
        self.id = '{}-{}'.format(self.fips, self.year)
        self.pop_source = self.pop_source.strip() if self.pop_source else None
        self.mhi_source = self.mhi_source.strip() if self.mhi_source else None
        super().save(*args, **kwargs)
