from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("llm", "0005_assistant_updates"),
    ]

    operations = [
        migrations.AlterField(
            model_name="aimodel",
            name="model_id",
            field=models.CharField(max_length=100, unique=True),
        ),
    ]
