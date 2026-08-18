from django.db import models
from django.db.models import Exists, OuterRef, QuerySet
from django_cte import CTEManager

from usaspending_api.common.mixins import EmbeddingMixin
from usaspending_api.references.models.agency import Agency


class ToptierAgencyManager(CTEManager):
    def award_agencies(self) -> QuerySet:
        """
        Returns only those toptier agencies that have a subtier and their user_selectable flag set.  This
        is used primarily to filter agencies for award related agency drop down lists.  Think File D (Awards,
        TransactionNormalized, TransactionFABS, TransactionFPDS, and their ilk).  DO NOT USE THIS FOR ACCOUNT
        RELATED AGENCY DROPDOWN LISTS OR FOR AGENCY PAGES (see ToptierAgencyPublishedDABSView for those).
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


class ToptierAgency(EmbeddingMixin, models.Model):

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

    def get_embedding_text(self) -> str | None:
        parts = []

        if self.name:
            parts.append(self.name.strip())

        if self.abbreviation:
            parts.append(f"({self.abbreviation.strip()})")

        if self.mission:
            parts.append(f"Mission: {self.mission.strip()}")

        if self.about_agency_data:
            about = self.about_agency_data.strip()
            if len(about) > 1000:
                about = about[:1000] + "..."
            parts.append(f"About: {about}")

        return " | ".join(parts) if parts else None
