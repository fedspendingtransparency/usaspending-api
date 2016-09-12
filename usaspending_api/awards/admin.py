from django.contrib import admin
from usaspending_api.awards.models import FinancialAccountsByAwardsTransactionObligations


@admin.register(FinancialAccountsByAwardsTransactionObligations)
class AwardAdmin(admin.ModelAdmin):

    pass
