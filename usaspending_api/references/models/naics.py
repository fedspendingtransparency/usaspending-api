from django.db import models

from usaspending_api.common.mixins import EmbeddingMixin


class NAICS(EmbeddingMixin, models.Model):
    """Based on United States Census Bureau"""

    embedding_dimensions = 256

    code = models.TextField(primary_key=True)
    description = models.TextField(null=False)
    long_description = models.TextField(null=True)
    year = models.IntegerField(default=0)
    year_retired = models.IntegerField(null=True)

    class Meta:
        managed = True
        db_table = "naics"

    def get_embedding_text(self) -> str | None:
        parts = []
        if self.description:
            parts.append(self.description.strip())
        if self.long_description:
            parts.append(self.long_description.strip())
        return " | ".join(parts) if parts else None
