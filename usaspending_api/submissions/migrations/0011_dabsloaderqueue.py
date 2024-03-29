# Generated by Django 2.2.13 on 2020-07-28 11:55

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('submissions', '0009_dabssubmissionwindowschedule'),
    ]

    operations = [
        migrations.CreateModel(
            name='DABSLoaderQueue',
            fields=[
                ('submission_id', models.IntegerField(primary_key=True, serialize=False)),
                ('state', models.TextField(choices=[('READY', 'READY'), ('IN PROGRESS', 'IN PROGRESS'), ('FAILED', 'FAILED')], default='READY')),
                ('force_reload', models.BooleanField(default=False)),
                ('processor_id', models.TextField(default=None, null=True)),
                ('processing_started', models.DateTimeField(null=True)),
                ('heartbeat', models.DateTimeField(null=True)),
                ('processing_failed', models.DateTimeField(null=True)),
                ('exception', models.TextField(default=None, null=True)),
            ],
            options={
                'db_table': 'dabs_loader_queue',
            },
        ),
    ]
