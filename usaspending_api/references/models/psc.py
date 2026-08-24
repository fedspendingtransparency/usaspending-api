from django.db import models

from usaspending_api.common.mixins import EmbeddingMixin


class PSC(EmbeddingMixin, models.Model):
    """Based on https://www.acquisition.gov/PSC_Manual"""

    code = models.CharField(primary_key=True, max_length=4)
    length = models.IntegerField(null=False, default=0)
    description = models.TextField(null=False)
    start_date = models.DateField(blank=True, null=True)
    end_date = models.DateField(blank=True, null=True)
    full_name = models.TextField(blank=True, null=True)
    excludes = models.TextField(blank=True, null=True)
    notes = models.TextField(blank=True, null=True)
    includes = models.TextField(blank=True, null=True)

    class Meta:
        managed = True
        db_table = "psc"

    def get_embedding_text(self) -> str | None:
        parts = []

        if self.description:
            parts.append(self.description.strip())

        if self.full_name and self.full_name.strip() != self.description.strip():
            parts.append(f"({self.full_name.strip()})")

        if self.includes:
            includes = self.includes.strip()
            if len(includes) > 500:
                includes = includes[:500] + "..."
            parts.append(f"Includes: {includes}")

        if self.excludes:
            excludes = self.excludes.strip()
            if len(excludes) > 300:
                excludes = excludes[:300] + "..."
            parts.append(f"Excludes: {excludes}")

        if self.notes:
            notes = self.notes.strip()
            if len(notes) > 400:
                notes = notes[:400] + "..."
            parts.append(f"Notes: {notes}")

        return " | ".join(parts) if parts else None
