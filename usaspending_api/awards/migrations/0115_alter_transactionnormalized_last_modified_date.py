from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("awards", "0114_alter_ctodlinkageupdates_award_id"),
    ]

    operations = [
        migrations.AlterField(
            model_name="transactionnormalized",
            name="last_modified_date",
            field=models.DateTimeField(null=True),
        ),
    ]
