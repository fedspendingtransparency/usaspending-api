# Generated by Django 2.2.9 on 2021-02-02 22:35

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reporting', '0004_auto_20210202_2234'),
    ]

    operations = [
        migrations.AlterField(
            model_name='reportingagencyoverview',
            name='unlinked_procurement_c_awards',
            field=models.IntegerField(),
        ),
        migrations.AlterField(
            model_name='reportingagencyoverview',
            name='unlinked_assistance_c_awards',
            field=models.IntegerField(),
        ),
        migrations.AlterField(
            model_name='reportingagencyoverview',
            name='unlinked_procurement_d_awards',
            field=models.IntegerField(),
        ),
        migrations.AlterField(
            model_name='reportingagencyoverview',
            name='unlinked_assistance_d_awards',
            field=models.IntegerField(),
        ),
        migrations.AlterField(
            model_name='reportingagencyoverview',
            name='linked_procurement_awards',
            field=models.IntegerField(),
        ),
        migrations.AlterField(
            model_name='reportingagencyoverview',
            name='linked_assistance_awards',
            field=models.IntegerField(),
        ),
    ]
