from django.db import models
from django.db.models import Exists, OuterRef
from django_cte import CTEManager
from usaspending_api.accounts.models import AppropriationAccountBalances
from usaspending_api.references.models.agency import Agency


class ToptierAgencyManager(CTEManager):
    def account_agencies(self, agency_type="funding"):
        """
        Returns only those toptier agencies that have ever had a submission.  This is used primarily to
        filter agencies for agency detail pages or account related agency drop down lists.  Think Files
        A through C (AppropriationAccountBalances, FinancialAccountsByProgramActivityObjectClass,
        FinancialAccountsByAwards, and their ilk).  DO NOT USE THIS FOR AWARD RELATED AGENCY DROPDOWN
        LISTS (see award_agencies method for those).
        """
        aab_filter = {f"treasury_account_identifier__{agency_type}_toptier_agency_id": OuterRef("toptier_agency_id")}
        return (
            self.get_queryset()
            .annotate(has_submission=Exists(AppropriationAccountBalances.objects.filter(**aab_filter).values("pk")))
            .filter(has_submission=True)
        )

    def award_agencies(self):
        """
        Returns only those toptier agencies that have a subtier and their user_selectable flag set.  This
        is used primarily to filter agencies for award related agency drop down lists.  Think File D (Awards,
        TransactionNormalized, TransactionFABS, TransactionFPDS, and their ilk).  DO NOT USE THIS FOR ACCOUNT
        RELATED AGENCY DROPDOWN LISTS OR FOR AGENCY PAGES (see account_agencies method for those).
        """
        return (
            self.get_queryset()
            .annotate(
                user_selectable=Exists(
                    Agency.objects.filter(
                        user_selectable=True, subtier_agency_id__isnull=False, toptier_agency_id=OuterRef("pk")
                    ).values("pk")
                )
            )
            .filter(user_selectable=True)
        )


class ToptierAgency(models.Model):
    toptier_agency_id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True)
    update_date = models.DateTimeField(auto_now=True)
    toptier_code = models.TextField(db_index=True, unique=True)
    abbreviation = models.TextField(blank=True, null=True)
    name = models.TextField(db_index=True)
    mission = models.TextField(blank=True, null=True)
    about_agency_data = models.TextField(blank=True, null=True)
    website = models.URLField(blank=True, null=True)
    justification = models.URLField(blank=True, null=True)
    icon_filename = models.TextField(blank=True, null=True)

    objects = ToptierAgencyManager()

    class Meta:
        db_table = "toptier_agency"
