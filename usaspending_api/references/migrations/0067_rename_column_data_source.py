# Generated by Django 3.2.25 on 2024-09-06 17:24

from django.db import migrations
from django.db import models

class Migration(migrations.Migration):

    dependencies = [
        ('references', '0066_alter_office_unique_index_code_only'),
    ]

    operations = [
        migrations.AlterField(
            model_name='Cfda',
            name='data_source',
            field=models.TextField(
                choices=[
                    ('USA', 'USAspending'),
                    ('DBR', 'DATA Broker')
                ],
                help_text='The source of this entry, either Data Broker (DBR) or USASpending (USA)',
                null=True,
            ),
        ),
    ]