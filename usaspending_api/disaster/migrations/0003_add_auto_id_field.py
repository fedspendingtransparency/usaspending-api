# Generated by Django 3.2.25 on 2024-08-01 18:50

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('disaster', '0002_add_indexes'),
    ]

    operations = [
        migrations.AlterField(
            model_name='covidfabaspending',
            name='id',
            field=models.AutoField(primary_key=True, serialize=False),
        ),
    ]
