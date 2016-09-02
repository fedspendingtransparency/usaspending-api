from django.contrib import admin
from usaspending_api.awards.models import Award

@admin.register(Award)
class AwardAdmin(admin.ModelAdmin):
    pass
