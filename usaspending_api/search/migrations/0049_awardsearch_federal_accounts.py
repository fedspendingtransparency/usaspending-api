# Generated by Django 3.2.25 on 2024-12-18 21:18

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("search", "0048_alter_awardsearch_award_alter_summarystateview_duh"),
    ]

    operations = [
        migrations.AddField(
            model_name="awardsearch",
            name="federal_accounts",
            field=models.JSONField(null=True),
        ),
    ]
