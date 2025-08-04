# Manually created to move summary_state_view to new schema matching placement in delta

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("etl", "0001_create_schemas"),
    ]

    operations = [
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.summary_state_view SET SCHEMA rpt;",
            reverse_sql="ALTER TABLE IF EXISTS rpt.summary_state_view SET SCHEMA public;"
        ),

    ]
