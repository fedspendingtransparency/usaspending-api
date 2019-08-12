from django.db import models
from django.contrib.postgres.fields import ArrayField
from django.contrib.postgres.indexes import GinIndex
from partial_index import PartialIndex, PQ


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
    median_household_income = models.DecimalField(null=True, blank=True, decimal_places=2, max_digits=23)
    mhi_source = models.TextField(null=True, blank=True)  # median household income source

    class Meta:
        db_table = "state_data"

    def save(self, *args, **kwargs):
        self.fips = self.fips.zfill(2)
        self.id = "{}-{}".format(self.fips, self.year)
        self.pop_source = self.pop_source.strip() if self.pop_source else None
        self.mhi_source = self.mhi_source.strip() if self.mhi_source else None
        super().save(*args, **kwargs)


class DUNS(models.Model):
    """
    Model representing DUNS data (imported from the broker)
    """

    awardee_or_recipient_uniqu = models.TextField(primary_key=True)
    legal_business_name = models.TextField(null=True, blank=True)
    dba_name = models.TextField(null=True, blank=True)
    ultimate_parent_unique_ide = models.TextField(null=True, blank=True)
    ultimate_parent_legal_enti = models.TextField(null=True, blank=True)
    address_line_1 = models.TextField(null=True, blank=True)
    address_line_2 = models.TextField(null=True, blank=True)
    city = models.TextField(null=True, blank=True)
    state = models.TextField(null=True, blank=True)
    zip = models.TextField(null=True, blank=True)
    zip4 = models.TextField(null=True, blank=True)
    country_code = models.TextField(null=True, blank=True)
    congressional_district = models.TextField(null=True, blank=True)
    business_types_codes = ArrayField(base_field=models.TextField(), default=list, size=None, null=True)
    entity_structure = models.TextField(null=True, blank=True)
    broker_duns_id = models.TextField()
    update_date = models.DateField()

    class Meta:
        db_table = "duns"


class HistoricParentDUNS(models.Model):
    """
    Model representing DUNS data (imported from the broker)
    """

    awardee_or_recipient_uniqu = models.TextField()
    legal_business_name = models.TextField(null=True, blank=True)
    ultimate_parent_unique_ide = models.TextField(null=True, blank=True)
    ultimate_parent_legal_enti = models.TextField(null=True, blank=True)
    broker_historic_duns_id = models.IntegerField(primary_key=True)
    year = models.IntegerField(null=True, blank=True)

    class Meta:
        db_table = "historic_parent_duns"


class RecipientProfile(models.Model):
    """Table used for speed improvements for the recipient profile listings"""

    recipient_level = models.CharField(max_length=1)
    recipient_hash = models.UUIDField(null=True, db_index=True)
    recipient_unique_id = models.TextField(null=True, db_index=True)
    recipient_name = models.TextField(null=True, db_index=True)
    recipient_affiliations = ArrayField(base_field=models.TextField(), default=list, size=None)
    award_types = ArrayField(base_field=models.TextField(), default=list, size=None)
    last_12_months = models.DecimalField(max_digits=23, decimal_places=2, default=0.00)
    last_12_contracts = models.DecimalField(max_digits=23, decimal_places=2, default=0.00)
    last_12_grants = models.DecimalField(max_digits=23, decimal_places=2, default=0.00)
    last_12_direct_payments = models.DecimalField(max_digits=23, decimal_places=2, default=0.00)
    last_12_loans = models.DecimalField(max_digits=23, decimal_places=2, default=0.00)
    last_12_other = models.DecimalField(max_digits=23, decimal_places=2, default=0.00)
    last_12_months_count = models.IntegerField(null=False, default=0)

    class Meta:
        managed = True
        db_table = "recipient_profile"
        unique_together = ("recipient_hash", "recipient_level")
        # Note:  A custom index was added in the migration because there's
        # currently not a Django native means by which to add a GinIndex with
        # a specific Postgres operator class:
        #
        #     create index idx_recipient_profile_name on
        #         public.recipient_profile using gin (recipient_name public.gin_trgm_ops)
        #
        indexes = [GinIndex(fields=["award_types"]), models.Index(fields=["recipient_unique_id"])]


class RecipientLookup(models.Model):
    recipient_hash = models.UUIDField(unique=True, null=True)
    legal_business_name = models.TextField(null=True, db_index=True)
    duns = models.TextField(unique=True, null=True)
    parent_duns = models.TextField(null=True)
    parent_legal_business_name = models.TextField(null=True)
    address_line_1 = models.TextField(null=True)
    address_line_2 = models.TextField(null=True)
    city = models.TextField(null=True)
    state = models.TextField(null=True)
    zip5 = models.TextField(null=True)
    zip4 = models.TextField(null=True)
    country_code = models.TextField(null=True)
    congressional_district = models.TextField(null=True)
    business_types_codes = ArrayField(base_field=models.TextField(), default=list, size=None, null=True)

    class Meta:
        db_table = "recipient_lookup"
        indexes = [
            PartialIndex(fields=["duns"], unique=True, where=PQ(duns__isnull=False)),
            PartialIndex(fields=["parent_duns"], unique=False, where=PQ(parent_duns__isnull=False)),
        ]


class SummaryAwardRecipient(models.Model):
    award_id = models.IntegerField(primary_key=True)
    action_date = models.DateField(blank=True, db_index=True)
    recipient_hash = models.UUIDField(null=True, db_index=True)
    parent_recipient_unique_id = models.TextField(null=True, db_index=True)

    class Meta:
        managed = True
        db_table = "summary_award_recipient"
