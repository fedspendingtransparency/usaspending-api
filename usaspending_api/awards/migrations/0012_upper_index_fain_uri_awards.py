# -*- coding: utf-8 -*-
# Created by Alisa 1.11.4 on 2017-12-06 11:10
from __future__ import unicode_literals

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0011_upper_index_awards'),
    ]

    operations = [
        migrations.RunSQL(
            ["CREATE INDEX awards_fain_uppr_idx ON awards (UPPER(fain));"],
            ["DROP INDEX awards_fain_uppr_idx;"]
        ),
        migrations.RunSQL(
            ["CREATE INDEX awards_uri_uppr_idx ON awards (UPPER(uri));"],
            ["DROP INDEX awards_uri_uppr_idx;"]
        ),
    ]