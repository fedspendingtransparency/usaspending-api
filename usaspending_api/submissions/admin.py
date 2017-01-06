from django.contrib import admin
from usaspending_api.submissions.models import SubmissionAttributes


# Register your models here.
@admin.register(SubmissionAttributes)
class SubmissionAttributesAdmin(admin.ModelAdmin):

    pass
