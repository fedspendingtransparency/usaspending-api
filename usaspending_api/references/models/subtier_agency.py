from django.db import models

from usaspending_api.common.mixins import EmbeddingMixin


class SubtierAgency(EmbeddingMixin, models.Model):
    subtier_agency_id = models.AutoField(primary_key=True)
    create_date = models.DateTimeField(auto_now_add=True)
    update_date = models.DateTimeField(auto_now=True)
    subtier_code = models.TextField(db_index=True, unique=True)
    abbreviation = models.TextField(blank=True, null=True)
    name = models.TextField(db_index=True)

    class Meta:
        db_table = "subtier_agency"

    def get_embedding_text(self) -> str | None:
        parts = []
        if self.name:
            parts.append(self.name.strip())
        if self.abbreviation:
            parts.append(f"({self.abbreviation.strip()})")
        return " | ".join(parts) if parts else None
