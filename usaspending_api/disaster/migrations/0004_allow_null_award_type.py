# Generated by Django 3.2.25 on 2024-07-11 14:33

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('disaster', '0003_rename_defc_column'),
    ]

    operations = [
        migrations.AlterField(
            model_name='covidfabaspending',
            name='award_type',
            field=models.TextField(null=True),
        ),
    ]