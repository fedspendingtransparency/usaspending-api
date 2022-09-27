from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("recipient", "0023_shorter_index_names"),
    ]

    operations = [
        migrations.RunSQL(
            sql="ALTER INDEX recipient_profile_recipient_hash_recipient_level_ee7ecd55_uniq RENAME TO rp_recipient_hash_level_ee7ecd55_uniq",
            reverse_sql="ALTER INDEX rp_recipient_hash_level_ee7ecd55_uniq RENAME TO recipient_profile_recipient_hash_recipient_level_ee7ecd55_uniq",
        ),
    ]
