from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [("references", "0065_alter_office_unique_together")]

    operations = [
        migrations.RunSQL(
            sql=[
                """ALTER TABLE office DROP CONSTRAINT IF EXISTS unique_agency_sub_tier_office;
                ALTER TABLE office ADD CONSTRAINT unique_office_code UNIQUE (office_code);"""
            ],
            reverse_sql=["ALTER TABLE office DROP CONSTRAINT IF EXISTS unique_agency_sub_tier_office;"],
        ),
    ]
