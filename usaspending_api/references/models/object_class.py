from django.db import models


class ObjectClass(models.Model):
    major_object_class = models.TextField(db_index=True)
    major_object_class_name = models.TextField()
    object_class = models.TextField(db_index=True)
    object_class_name = models.TextField()
    direct_reimbursable = models.TextField(db_index=True, blank=True, null=True)
    direct_reimbursable_name = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = "object_class"
        unique_together = ("object_class", "direct_reimbursable")

    @property
    def corrected_major_object_class_name(self):
        if self.major_object_class == "00":
            return "Unknown Object Type"
        else:
            return self.major_object_class_name
