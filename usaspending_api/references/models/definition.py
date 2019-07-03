from django.db import models
from django.utils.text import slugify


class Definition(models.Model):
    id = models.AutoField(primary_key=True)
    term = models.TextField(unique=True, db_index=True, blank=False, null=False)
    data_act_term = models.TextField(blank=True, null=True)
    plain = models.TextField()
    official = models.TextField(blank=True, null=True)
    slug = models.SlugField(max_length=500, null=True)
    resources = models.TextField(blank=True, null=True)

    def save(self, *arg, **kwarg):
        self.slug = slugify(self.term)
        return super(Definition, self).save(*arg, **kwarg)
