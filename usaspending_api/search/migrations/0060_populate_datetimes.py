from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("search", "0059_add_datetimes_drop_views"),
    ]

    operations = [
        migrations.RunSQL(
            "UPDATE rpt.transaction_search set last_modified_datetime = last_modified_date::timestamp with time zone",
            migrations.RunSQL.noop,
        ),
        migrations.RunSQL(
            "UPDATE rpt.transaction_search set initial_report_datetime = initial_report_date::timestamp with time zone",
            migrations.RunSQL.noop,
        ),
        migrations.RunSQL(
            "UPDATE rpt.award_search set last_modified_datetime = last_modified_date::timestamp with time zone",
            migrations.RunSQL.noop,
        ),
    ]
