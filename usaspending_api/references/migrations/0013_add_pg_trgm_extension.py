from django.contrib.postgres.operations import TrigramExtension
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('references', '0012_overalltotals'),
    ]

    operations = [
        TrigramExtension()
    ]