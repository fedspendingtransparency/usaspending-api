from __future__ import annotations
import logging
import pathlib

import boto3
from collections import deque
from itertools import islice
from filelock import FileLock
import pickle
from typing import List
import uuid

from usaspending_api import settings


class _FakeFileBackedSQSQueue:
    """
    Fakes portions of a boto3 SQS resource ``Queue`` object.
    See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#queue

    Queue data was chosen to be persisted in a backing filesystem file rather than in-memory to support the scenario
    of multiple processes/sub-processes accessing the queue. If queue data was in-memory, each process would have
    independent and divergent memory, and so synchronized activity with a single queue would not be possible. A
    single file backing the queue solves this, and pickle is used to read and write queue data state from/to the file.
    """

    _FAKE_AWS_ACCT = uuid.uuid4().hex
    UNITTEST_FAKE_QUEUE = "unittest-fake-queue"
    FAKE_QUEUE_URL = f"https://fake-us-region.queue.amazonaws.com/{_FAKE_AWS_ACCT}/{UNITTEST_FAKE_QUEUE}"
    FAKE_QUEUE_DATA_PATH = pathlib.Path(settings.BULK_DOWNLOAD_LOCAL_PATH) / "local_queue"
    _QUEUE_DATA_FILE = str(FAKE_QUEUE_DATA_PATH / f"{_FAKE_AWS_ACCT}_{UNITTEST_FAKE_QUEUE}.pickle")

    _INSTANCE_STATE_MEMENTO = None

    def __init__(self, queue_url=FAKE_QUEUE_URL, max_receive_count=1):
        self.max_receive_count = max_receive_count
        self.queue_url = queue_url
        self.FAKE_QUEUE_DATA_PATH.mkdir(parents=True, exist_ok=True)
        with open(self._QUEUE_DATA_FILE, "w+b") as queue_data_file:
            pickle.dump(deque([]), queue_data_file, protocol=pickle.HIGHEST_PROTOCOL)
        # Make sure this stays last
        self._INSTANCE_STATE_MEMENTO = self.__dict__.copy()

    def reset_instance_state(self):
        """Because multiple tests in a single test session may be using the same FAKE_QUEUE instance, this method can
           and should be used to reset any changed state/config on the queue back to its original state when it was
           instantiated.

           Using a loose implementation of the memento pattern.
        """
        if not self._INSTANCE_STATE_MEMENTO:
            raise ValueError("Prior instance state to restore was not saved. Saved instance state is None")
        self.__dict__.update(self._INSTANCE_STATE_MEMENTO)

    @classmethod
    def _enqueue(cls, msg: FakeSQSMessage):
        with FileLock(cls._QUEUE_DATA_FILE + ".lock"):
            with open(cls._QUEUE_DATA_FILE, "r+b") as queue_data_file:
                messages = pickle.load(queue_data_file)
            messages.appendleft(msg)
            with open(cls._QUEUE_DATA_FILE, "w+b") as queue_data_file:
                pickle.dump(messages, queue_data_file, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def _remove(cls, msg: FakeSQSMessage):
        with FileLock(cls._QUEUE_DATA_FILE + ".lock"):
            with open(cls._QUEUE_DATA_FILE, "r+b") as queue_data_file:
                messages = pickle.load(queue_data_file)
            messages.remove(msg)
            with open(cls._QUEUE_DATA_FILE, "w+b") as queue_data_file:
                pickle.dump(messages, queue_data_file, protocol=pickle.HIGHEST_PROTOCOL)

    @classmethod
    def _messages(cls) -> deque:
        with open(cls._QUEUE_DATA_FILE, "rb") as queue_data_file:
            messages = pickle.load(queue_data_file)
            return messages

    @classmethod
    def send_message(cls, MessageBody: str, MessageAttributes: dict = None):  # noqa
        msg = FakeSQSMessage(cls.FAKE_QUEUE_URL)
        msg.body = MessageBody
        msg.message_attributes = MessageAttributes
        cls._enqueue(msg)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    @classmethod
    def receive_messages(
        cls,
        WaitTimeSeconds,  # noqa
        AttributeNames=None,  # noqa
        MessageAttributeNames=None,  # noqa
        VisibilityTimeout=30,  # noqa
        MaxNumberOfMessages=1,  # noqa
    ) -> List[FakeSQSMessage]:
        # Limit returned messages by MaxNumberOfMessages: start=0, stop=MaxNumberOfMessages
        with open(cls._QUEUE_DATA_FILE, "rb") as queue_data_file:
            messages_to_recv = pickle.load(queue_data_file)
        messages_to_recv.reverse()
        return list(islice(messages_to_recv, 0, MaxNumberOfMessages))

    @classmethod
    def purge(cls):
        with open(cls._QUEUE_DATA_FILE, "w+b") as queue_data_file:
            pickle.dump(deque([]), queue_data_file, protocol=pickle.HIGHEST_PROTOCOL)

    @property
    def attributes(self):
        fake_redrive = '{{"deadLetterTargetArn": "FAKE_ARN:{}", "maxReceiveCount": {}}}'.format(
            FAKE_DEAD_LETTER_QUEUE.UNITTEST_FAKE_DEAD_LETTER_QUEUE, self.max_receive_count
        )
        return {"ReceiveMessageWaitTimeSeconds": "10", "RedrivePolicy": fake_redrive}


class _FakeStatelessLoggingSQSDeadLetterQueue:
    """A Fake implementation of portions of a boto3 SQS resource ``Queue`` object for uses as a Dead Letter Queue
    instance. This implementation is stateless, meaning it holds no enqueued messages, and actions on this queue do
    nothing but log that the action occurred.
    See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#queue"""

    UNITTEST_FAKE_DEAD_LETTER_QUEUE = "unittest-fake-dead-letter-queue"
    FAKE_DEAD_LETTER_QUEUE_URL = (
        f"https://fake-us-region.queue.amazonaws.com/{uuid.uuid4().hex}/{UNITTEST_FAKE_DEAD_LETTER_QUEUE}"
    )

    def __init__(self, queue_url=FAKE_DEAD_LETTER_QUEUE_URL):
        self.queue_url = queue_url

    logger = logging.getLogger(__name__)

    @staticmethod
    def send_message(MessageBody, MessageAttributes=None):  # noqa
        _FakeStatelessLoggingSQSDeadLetterQueue.logger.debug(
            "executing SQSMockDeadLetterQueue.send_message({}, {})".format(MessageBody, MessageAttributes)
        )
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}

    @staticmethod
    def receive_messages(
        WaitTimeSeconds,  # noqa
        AttributeNames=None,  # noqa
        MessageAttributeNames=None,  # noqa
        VisibilityTimeout=30,  # noqa
        MaxNumberOfMessages=1,  # noqa
    ):
        _FakeStatelessLoggingSQSDeadLetterQueue.logger.debug(
            "executing SQSMockDeadLetterQueue.receive_messages("
            "{}, {}, {}, {}, {})".format(
                WaitTimeSeconds, AttributeNames, MessageAttributeNames, VisibilityTimeout, MaxNumberOfMessages
            )
        )
        return []


# Pseudo-singleton instances of the Fake Queue and Fake Dead Letter Queue
FAKE_QUEUE = _FakeFileBackedSQSQueue()
FAKE_DEAD_LETTER_QUEUE = _FakeStatelessLoggingSQSDeadLetterQueue()


class FakeSQSMessage:
    """Fakes portions of a boto3 SQS resource ``Message`` object.
    See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#message"""

    def __init__(self, queue_url: str = FAKE_QUEUE.UNITTEST_FAKE_QUEUE):
        self.queue_url = queue_url
        self.receipt_handle = str(uuid.uuid4())
        self.body = None
        self.message_attributes = None

    def __hash__(self):
        return hash(self.receipt_handle)

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def delete(self):
        FAKE_QUEUE._remove(self)

    def change_visibility(self, VisibilityTimeout):  # noqa
        # Do nothing
        pass

    @property
    def attributes(self):
        # This is where more SQS message `attributes` can be mocked, e.g. ApproximateReceiveCount and others
        return {}


def get_sqs_queue(region_name=settings.USASPENDING_AWS_REGION, queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME):
    if settings.IS_LOCAL:
        # Ensure the "singleton" instances of the queue is returned, so that multiple consumers, possibly on
        # different processes or threads, are accessing the same queue data
        if queue_name == FAKE_DEAD_LETTER_QUEUE.UNITTEST_FAKE_DEAD_LETTER_QUEUE:
            return FAKE_DEAD_LETTER_QUEUE
        return FAKE_QUEUE
    else:
        # stuff that's in get_queue
        sqs = boto3.resource("sqs", region_name)
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        return queue
