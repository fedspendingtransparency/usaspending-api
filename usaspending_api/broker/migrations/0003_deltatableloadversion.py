# Generated by Django 3.2.23 on 2024-01-11 22:49

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('broker', '0002_auto_20190402_1457'),
    ]

    operations = [
        migrations.CreateModel(
            name='DeltaTableLoadVersion',
            fields=[
                ('delta_table_load_version_id', models.AutoField(primary_key=True, serialize=False)),
                ('name', models.TextField()),
                ('last_version_copied_to_staging', models.IntegerField(default=-1)),
                ('last_version_copied_to_live', models.IntegerField(default=-1)),
                ('description', models.TextField(blank=True, null=True)),
                ('create_date', models.DateTimeField(auto_now_add=True, null=True)),
                ('update_date', models.DateTimeField(auto_now=True, null=True)),
            ],
            options={
                'db_table': 'delta_table_load_version',
                'managed': True,
            },
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS raw.delta_table_load_version SET SCHEMA public;",
            reverse_sql="ALTER TABLE IF EXISTS public.delta_table_load_version SET SCHEMA raw;"
        ),
    ]