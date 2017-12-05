# -*- coding: utf-8 -*-
# Created by Alisa 1.11.4 on 2017-12-05 19:20
from __future__ import unicode_literals

import django.contrib.postgres.fields
from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0010_recipientlookupview_summarytransactionview'),
    ]

    operations = [
        migrations.RunSQL(
            "CREATE INDEX awards_piid_uppr_idx ON awards (UPPER(piid));",
            "CREATE INDEX awards_fain_uppr_idx ON awards (UPPER(fain));"
            "CREATE INDEX awards_uri_uppr_idx ON awards (UPPER(uri));"
        ),
    ]
