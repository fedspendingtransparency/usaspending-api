from django.contrib import admin
from usaspending_api.references.models import RefProgramActivity
from usaspending_api.references.models import RefObjectClassCode


# Register your models here.
@admin.register(RefObjectClassCode)
class RefObjectClassCodeAdmin(admin.ModelAdmin):
    pass


@admin.register(RefProgramActivity)
class RefProgramActivityAdmin(admin.ModelAdmin):
    pass
