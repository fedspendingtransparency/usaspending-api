from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("broker", "0006_add_fks_load_tracker"),
    ]

    operations = [
        migrations.RunSQL(
            sql=""""CREATE VIEW public.v_load_tracker AS (
                SELECT lt.*
                       ,ltlt.*
                       ,lts.*

                FROM public.load_tracker lt

                LEFT JOIN public.load_tracker_load_type ltlt
                ON lt.load_tracker_load_type_id = ltlt.load_tracker_load_type_id

                LEFT JOIN public.load_tracker_step lts
                ON lt.load_tracker_step_id = lts.load_tracker_step_id
            )""",
            reverse_sql="DROP VIEW IF EXISTS public.v_load_tracker"
        ),
    ]
