from argparse import ArgumentTypeError
from broker.models import LoadTracker as LoadTrackerModel, LoadTrackerStep
from usaspending_api.broker.lookups import LoadTrackerStepEnum, LoadTrackerLoadTypeEnum
from django.db.models import Max


class LoadTracker:

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
            raise ArgumentTypeError(f"No LoadTrackerStep record found for table name {load_step.value}")
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
