from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("broker", "0005_populate_load_tracker_tables"),
    ]

    operations = [
        migrations.AddField(
            model_name="loadtracker",
            name="load_tracker_load_type",
            field=models.ForeignKey("LoadTrackerLoadType", on_delete=models.deletion.DO_NOTHING),
        ),
        migrations.AddField(
            model_name="loadtracker",
            name="load_tracker_step",
            field=models.ForeignKey("LoadTrackerStep", on_delete=models.deletion.DO_NOTHING),
        ),
    ]
