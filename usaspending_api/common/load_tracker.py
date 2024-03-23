from argparse import ArgumentError
from datetime import datetime
from broker.models import LoadTracker as LoadTrackerModel, LoadTrackerStep, LoadTrackerLoadType
from usaspending_api.broker.lookups import LoadTrackerStepEnum, LoadTrackerLoadTypeEnum
from django.db.models import Max


class LoadTracker:

    def __init__(self, load_type: LoadTrackerLoadType, load_step: LoadTrackerStepEnum):
        self.load_step = LoadTracker.get_load_step(load_step)
        self.load_type = LoadTracker.get_load_type(load_type)
        self._load_tracker = None

    def start(self):
        """Performs the necessary operations to start this LoadTracker."""
        start_time = datetime.now()
        self._load_tracker = LoadTrackerModel.objects.create(
            load_tracker_type=self.load_type, load_tracker_step=self.load_step, start_date_time=start_time
        )

    def end(self):
        """Performs the necessary operations to end this LoadTracker."""
        end_time = datetime.now()
        if self._load_tracker is None:
            raise ValueError(f"LoadTracker requires start to be executed before end is executed.")
        self._load_tracker.end_date_time = end_time
        self._load_tracker.save()

    @staticmethod
    def get_load_step(load_step: LoadTrackerStepEnum) -> LoadTrackerStep:
        """Returns the primary key of for the load type specified.

        Returns:
            The load step record
        """
        load_step_record = LoadTrackerStep.objects.filter(name=load_step.value).first()
        if load_step_record is None:
            raise ArgumentError(f"A load step doest not exist for the specified load step {load_step.value}.")
        return load_step_record

    @staticmethod
    def get_load_type(load_type: LoadTrackerLoadType) -> LoadTrackerLoadType:
        """Returns the primary key of for the load type specified.

        Returns:
            The load type record
        """
        load_type_record = LoadTrackerLoadType.objects.filter(name=load_type.value).first()
        if load_type_record is None:
            raise ArgumentError(f"A load type doest not exist for the specified load type {load_type.value}.")
        return load_type_record

    @staticmethod
    def get_latest_load_tracker_type(load_step: LoadTrackerStepEnum) -> LoadTrackerLoadTypeEnum | None:
        """Returns the load tracker load type of the latest load tracker for the specified load tracker step.
        This function does not consider whether a load tracker step is done or not. In other words,
        if a load tracker record does not have an end date time but is the latest record for the load
        step, its load type will be returned still.

        Args:
            load_step: The step of the pipeline associated with the returned load tracker load type.

        Returns:
            The load tracker load type Enum.
        """
        load_tracker_step_record = LoadTrackerStep.objects.filter(name=load_step.value).first()
        if load_tracker_step_record is None:
            raise ArgumentError(f"No LoadTrackerStep record found for table name {load_step.value}")
        load_tracker_record = (
            LoadTracker.objects.filter(
                load_tracker_step=load_tracker_step_record.load_tracker_step_id,
            )
            .aggregate(Max("load_tracker_id"))
            .first()
        )
        if load_tracker_record is None:
            return None

        latest_load_tracker_record = LoadTrackerModel.objects.filter(
            load_tracker_id=load_tracker_record.load_tracker_id
        ).first()
        load_type_enum = (
            LoadTrackerLoadTypeEnum(latest_load_tracker_record.load_tracker_step.name)
            if latest_load_tracker_record is not None
            else None
        )
        return load_type_enum
