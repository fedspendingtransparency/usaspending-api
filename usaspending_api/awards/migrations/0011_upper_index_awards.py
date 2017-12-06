# -*- coding: utf-8 -*-
# Created by Alisa 1.11.4 on 2017-12-05 19:20
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0010_recipientlookupview_summarytransactionview'),
    ]

    operations = [
        migrations.RunSQL(
            "CREATE INDEX awards_piid_uppr_idx ON awards (UPPER(piid));"
        ),
    ]
