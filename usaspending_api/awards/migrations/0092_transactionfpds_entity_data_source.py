# Generated by Django 2.2.17 on 2022-03-28 15:58

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0091_auto_20220314_1432'),
    ]

    operations = [
        migrations.AddField(
            model_name='transactionfpds',
            name='entity_data_source',
            field=models.TextField(blank=True, null=True),
        ),
    ]
