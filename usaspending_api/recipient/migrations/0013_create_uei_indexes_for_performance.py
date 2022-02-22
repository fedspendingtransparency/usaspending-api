from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('recipient', '0012_auto_20220222_1916'),
    ]

    operations = [
        # Performed with RAW SQL because Django migrations don't support 'NULLS LAST'
        migrations.RunSQL(
            "CREATE INDEX recipient_profile_uei_d ON public.recipient_profile USING btree (uei DESC NULLS LAST)",
            reverse_sql="DROP INDEX recipient_profile_uei_d"
        ),
        # Performed with RAW SQL because Django migrations don't support GinIndexes with a specific Postgres operator class:
        migrations.RunSQL(
            "CREATE INDEX idx_uei_gin ON public.recipient_profile USING gin (uei gin_trgm_ops)",
            reverse_sql="DROP INDEX idx_uei_gin"
        )
    ]
