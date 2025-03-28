# Generated by Django 3.2.25 on 2024-09-06 17:24

from django.db import migrations
from django.db import models

class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0103_transaction_fabs_view_add_current_cd_cols'),
    ]

    operations = [
        migrations.AlterField(
            model_name='award',
            name='data_source',
            field=models.TextField(
                choices=[
                    ('USA', 'USAspending'),
                    ('DBR', 'Data Broker')
                ],
                help_text='The source of this entry, either Data Broker (DBR) or USASpending (USA)',
                null=True,
            ),
        ),
    ]
