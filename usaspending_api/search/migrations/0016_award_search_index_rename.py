from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("search", "0015_auto_20221026_1505"),
    ]

    operations = [
        migrations.RunSQL(
            sql="ALTER INDEX award_search_period_of_performance_current_end_date_64876276 RENAME TO as_pop_current_end_date_64876276",
            reverse_sql="ALTER INDEX as_pop_current_end_date_64876276 RENAME TO award_search_period_of_performance_current_end_date_64876276",
        ),
    ]
