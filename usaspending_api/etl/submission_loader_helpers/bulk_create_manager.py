class BulkCreateManager:
    """Hide the ugliness of batching saves."""

    batch_size = 500  # This number tested well.

    def __init__(self, model):
        self.model = model
        self.instances = []
        self.count = 0

    def append(self, instance):
        self.instances.append(instance)
        self.count += 1
        if self.count >= self.batch_size:
            self._bulk_create()

    def save_stragglers(self):
        self._bulk_create()

    def _bulk_create(self):
        if self.count > 0:
            self.model.objects.bulk_create(self.instances, self.count)
            self.instances = []
            self.count = 0
