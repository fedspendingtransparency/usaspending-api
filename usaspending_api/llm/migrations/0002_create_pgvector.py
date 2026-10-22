from django.db import migrations
from pgvector.django import VectorExtension


class Migration(migrations.Migration):

    dependencies = [("llm", "0001_initial")]
    # Note when reversing this migration it will be nescessary to first reverse the migrations from any app that adds a vector field.

    operations = [VectorExtension()]
