from django.contrib import admin

# Register your models here.
@admin.register(SubmissionAttributes)
class SubmissionAttributesAdmin(admin.ModelAdmin):

    pass

@admin.register(SubmissionProcess)
class SubmissionProcessAdmin(admin.ModelAdmin):

    pass
