from typing import Any

from django.db.models import Field
from model_bakery import baker
from pgvector.django import VectorField


# Model Bakery does not natively support the vector type.
# See https://model-bakery.readthedocs.io/en/latest/how_bakery_behaves.html#customizing-baker
# https://github.com/model-bakers/model_bakery/blob/ff7861f9fe43583382e86f28bc49177307ab1adc/model_bakery/baker.py#L678
class CustomBaker(baker.Baker):
    """Custom Baker that handles pgvector VectorField"""

    def generate_value(self, field: Field, commit: bool = True) -> Any:
        """Generate values for fields, with special handling for VectorField"""
        if isinstance(field, VectorField):
            dimensions = getattr(field, "dimensions", 256)
            return [0.1] * dimensions
        return super().generate_value(field, commit)
