from django.db import models
from django_cte import CTEManager


class DirectReimbursableConstants:

    UNKNOWN = None
    DIRECT = "D"
    REIMBURSABLE = "R"

    UNKNOWN_NAME = None
    DIRECT_NAME = "Direct"
    REIMBURSABLE_NAME = "Reimbursable"

    CHOICES = ((UNKNOWN, UNKNOWN_NAME), (DIRECT, DIRECT_NAME), (REIMBURSABLE, REIMBURSABLE_NAME))

    NAME_CHOICES = ((UNKNOWN_NAME, UNKNOWN), (DIRECT_NAME, DIRECT), (REIMBURSABLE_NAME, REIMBURSABLE))

    LOOKUP = dict(CHOICES)

    BY_DIRECT_REIMBURSABLE_FUN_MAPPING = {None: None, "D": "D", "d": "D", "R": "R", "r": "R"}


class MajorObjectClassConstants:

    UNKNOWN = "00"
    PERSONNEL_COMPENSATION_AND_BENEFITS = "10"
    CONTRACTUAL_SERVICES_AND_SUPPLIES = "20"
    ACQUISITION_OF_ASSETS = "30"
    GRANTS_AND_FIXED_CHARGES = "40"
    OTHER = "90"

    UNKNOWN_NAME = "Unknown"
    PERSONNEL_COMPENSATION_AND_BENEFITS_NAME = "Personnel compensation and benefits"
    CONTRACTUAL_SERVICES_AND_SUPPLIES_NAME = "Contractual services and supplies"
    ACQUISITION_OF_ASSETS_NAME = "Acquisition of assets"
    GRANTS_AND_FIXED_CHARGES_NAME = "Grants and fixed charges"
    OTHER_NAME = "Other"

    CHOICES = (
        (UNKNOWN, UNKNOWN_NAME),
        (PERSONNEL_COMPENSATION_AND_BENEFITS, PERSONNEL_COMPENSATION_AND_BENEFITS_NAME),
        (CONTRACTUAL_SERVICES_AND_SUPPLIES, CONTRACTUAL_SERVICES_AND_SUPPLIES_NAME),
        (ACQUISITION_OF_ASSETS, ACQUISITION_OF_ASSETS_NAME),
        (GRANTS_AND_FIXED_CHARGES, GRANTS_AND_FIXED_CHARGES_NAME),
        (OTHER, OTHER_NAME),
    )

    NAME_CHOICES = (
        (UNKNOWN_NAME, UNKNOWN),
        (PERSONNEL_COMPENSATION_AND_BENEFITS_NAME, PERSONNEL_COMPENSATION_AND_BENEFITS),
        (CONTRACTUAL_SERVICES_AND_SUPPLIES_NAME, CONTRACTUAL_SERVICES_AND_SUPPLIES),
        (ACQUISITION_OF_ASSETS_NAME, ACQUISITION_OF_ASSETS),
        (GRANTS_AND_FIXED_CHARGES_NAME, GRANTS_AND_FIXED_CHARGES),
        (OTHER_NAME, OTHER),
    )

    LOOKUP = dict(CHOICES)


class ObjectClass(models.Model):

    DIRECT_REIMBURSABLE = DirectReimbursableConstants
    MAJOR_OBJECT_CLASS = MajorObjectClassConstants

    major_object_class = models.TextField(db_index=True, choices=MAJOR_OBJECT_CLASS.CHOICES)
    major_object_class_name = models.TextField(choices=MAJOR_OBJECT_CLASS.NAME_CHOICES)
    object_class = models.TextField(db_index=True)
    object_class_name = models.TextField()
    direct_reimbursable = models.TextField(db_index=True, blank=True, null=True, choices=DIRECT_REIMBURSABLE.CHOICES)
    direct_reimbursable_name = models.TextField(blank=True, null=True, choices=DIRECT_REIMBURSABLE.NAME_CHOICES)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    objects = CTEManager()

    class Meta:
        db_table = "object_class"
        unique_together = ("object_class", "direct_reimbursable")
