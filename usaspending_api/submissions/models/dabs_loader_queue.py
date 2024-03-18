from django.db.models import BooleanField, DateTimeField, IntegerField, Model, TextField


class DABSLoaderQueue(Model):
    """A relatively simplistic queue for multiple submission loaders to coordinate processing."""

    READY = "READY"
    IN_PROGRESS = "IN PROGRESS"
    FAILED = "FAILED"

    STATES = [(READY, READY), (IN_PROGRESS, IN_PROGRESS), (FAILED, FAILED)]

    # Submission unique identifier.  Common to both USAspending and Broker.
    submission_id = IntegerField(primary_key=True)

    # There are also two conceptual states; ABANDONED is an IN_PROGRESS submission whose heartbeat
    # has exceeded ABANDONED_LOCK_MINUTES and COMPLETED which is a submission that was successfully
    # loaded but, since COMPLETED records are immediately deleted, is no longer in the table.
    state = TextField(choices=STATES, default=READY)

    # If we absolutely, positively need to perform a full reload instead of just an update.
    force_reload = BooleanField(default=False)

    # When a loader runs it generates a unique identifier for itself.  This allows the various
    # loaders to coordinate lock ownership.
    processor_id = TextField(null=True, default=None)

    # Timestamp for when the loader started working on this submission.
    processing_started = DateTimeField(null=True)

    # Submission loaders are required to check in on occasion.  If the heartbeat isn't updated
    # regularly, the lock will eventually be considered abandoned and will be picked up by another
    # loader.
    heartbeat = DateTimeField(null=True)

    # Timestamp for when the loader failed.  Check out the 'exception' field for details.
    processing_failed = DateTimeField(null=True)

    # If a loader experiences an error, the text of the exception should be recorded here for
    # debugging purposes.
    exception = TextField(null=True, default=None)

    class Meta:
        db_table = "dabs_loader_queue"
