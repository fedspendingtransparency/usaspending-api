from django.contrib import admin
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.submissions.models import SubmissionProcess


# Register your models here.
@admin.register(SubmissionAttributes)
class SubmissionAttributesAdmin(admin.ModelAdmin):

    pass


@admin.register(SubmissionProcess)
class SubmissionProcessAdmin(admin.ModelAdmin):

    pass
