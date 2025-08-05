# Manually created to move duns table to new schema matching placement in delta

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("etl", "0002_summary_state_view_schema_change"),
    ]

    operations = [
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS raw.duns SET SCHEMA int;",
            reverse_sql="ALTER TABLE IF EXISTS int.duns SET SCHEMA raw;"
        ),

    ]
