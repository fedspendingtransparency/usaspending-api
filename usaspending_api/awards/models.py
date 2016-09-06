from django.db import models


class Award(models.Model):

    AWARD_TYPES = (
        ('C', 'Contract'),
        ('G', 'Grant'),
        ('DP', 'Direct Payment'),
        ('L', 'Loan'),
    )

    award_id = models.CharField(max_length=50)
    type = models.CharField(max_length=5, choices=AWARD_TYPES)

    def __str__(self):
        # define a string representation of an award object
        return '%s #%s' % (self.get_type_display(), self.award_id)

    class Meta:
        # use the meta class to override the table name
        # so it's 'awards' instead of 'awards_awards'
        db_table = 'awards'
