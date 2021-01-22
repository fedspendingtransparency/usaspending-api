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

from django.conf import settings

LOCAL_FAKE_QUEUE_NAME = "local-fake-queue"
UNITTEST_FAKE_QUEUE_NAME = "unittest-fake-queue"
UNITTEST_FAKE_DEAD_LETTER_QUEUE_NAME = "unittest-fake-dead-letter-queue"
FAKE_QUEUE_DATA_PATH = pathlib.Path(settings.BULK_DOWNLOAD_LOCAL_PATH) / "local_queue"


class _FakeFileBackedSQSQueue:
    """
    Fakes portions of a boto3 SQS resource ``Queue`` object.
    See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#queue

    Queue data was chosen to be persisted in a backing filesystem file rather than in-memory to support the scenario
    of multiple processes/sub-processes accessing the queue. If queue data was in-memory, each process would have
    independent and divergent memory, and so synchronized activity with a single queue would not be possible. A
    single file backing the queue solves this, and pickle is used to read and write queue data state from/to the file.

    This implementation of the queue follows a Singleton pattern so that all objects of this class in the same
    process will be the same instance.
    """

    _FAKE_AWS_ACCT = "localfakeawsaccount"
    _FAKE_QUEUE_URL = f"https://fake-us-region.queue.amazonaws.com/{_FAKE_AWS_ACCT}/{LOCAL_FAKE_QUEUE_NAME}"
    _QUEUE_DATA_FILE = str(FAKE_QUEUE_DATA_PATH / f"{LOCAL_FAKE_QUEUE_NAME}.pickle")

    _instance_state_memento = None

    def __new__(cls, *args, **kwargs):
        """Implementation of the Singleton pattern from:
        https://www.python.org/download/releases/2.2.3/descrintro/#__new__
        """
        it = cls.__dict__.get("__it__")
        if it is not None:
            return it
        cls.__it__ = it = object.__new__(cls)
        it._init(*args, **kwargs)
        return it

    # NOTE: do not implement __init__, as it will get re-invoked on each call to the constructor.
    # In accordance with Singletons, we want instantiation only ONCE using the _init(...) function

    def __init__(self, *args, **kwargs):
        raise RuntimeError("Do not attempt to instantiate a new object. Acquire the Singleton instance via instance()")

    def _init(self, queue_url=None, max_receive_count=1):
        if not queue_url:
            queue_url = self._FAKE_QUEUE_URL
        self.max_receive_count = max_receive_count
        self.queue_url = queue_url
        FAKE_QUEUE_DATA_PATH.mkdir(parents=True, exist_ok=True)
        # The local queue can live on with data. Don't recreate unless it doesn't exist
        if not pathlib.Path(self._QUEUE_DATA_FILE).exists():
            with open(self._QUEUE_DATA_FILE, "w+b") as queue_data_file:
                pickle.dump(deque([]), queue_data_file, protocol=pickle.HIGHEST_PROTOCOL)
        # Make sure this stays last
        self._instance_state_memento = self.__dict__.copy()

    @classmethod
    def instance(cls, *args, **kwargs):
        return cls.__new__(cls, args, kwargs)

    def reset_instance_state(self):
        """Because multiple tests in a single test session may be using the same FAKE_QUEUE instance, this method can
        and should be used to reset any changed state/config on the queue back to its original state when it was
        instantiated.

        Using a loose implementation of the memento pattern.
        """
        if not self._instance_state_memento:
            raise ValueError("Prior instance state to restore was not saved. Saved instance state is None")
        self.__dict__.update(self._instance_state_memento)

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
        msg = FakeSQSMessage(cls._FAKE_QUEUE_URL)
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
            UNITTEST_FAKE_DEAD_LETTER_QUEUE_NAME, self.max_receive_count
        )
        return {"ReceiveMessageWaitTimeSeconds": "10", "RedrivePolicy": fake_redrive}

    @property
    def url(self):
        return self._FAKE_QUEUE_URL


class _FakeUnitTestFileBackedSQSQueue(_FakeFileBackedSQSQueue):
    """Subclass of the local file-backed queue used transiently during unit test sessions"""

    # NOTE: Must override ALL class attributes of base class
    _FAKE_AWS_ACCT = uuid.uuid4().hex
    _FAKE_QUEUE_URL = f"https://fake-us-region.queue.amazonaws.com/{_FAKE_AWS_ACCT}/{UNITTEST_FAKE_QUEUE_NAME}"
    _QUEUE_DATA_FILE = str(FAKE_QUEUE_DATA_PATH / f"{_FAKE_AWS_ACCT}_{UNITTEST_FAKE_QUEUE_NAME}.pickle")


class _FakeStatelessLoggingSQSDeadLetterQueue:
    """A Fake implementation of portions of a boto3 SQS resource ``Queue`` object for uses as a Dead Letter Queue
    instance. This implementation is stateless, meaning it holds no enqueued messages, and actions on this queue do
    nothing but log that the action occurred.
    See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#queue"""

    _FAKE_DEAD_LETTER_QUEUE_URL = (
        f"https://fake-us-region.queue.amazonaws.com/{uuid.uuid4().hex}/{UNITTEST_FAKE_DEAD_LETTER_QUEUE_NAME}"
    )
    logger = logging.getLogger(__name__)
    url = _FAKE_DEAD_LETTER_QUEUE_URL

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


class FakeSQSMessage:
    """Fakes portions of a boto3 SQS resource ``Message`` object.
    See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html#message"""

    def __init__(self, queue_url):
        self.queue_url = queue_url
        self.receipt_handle = str(uuid.uuid4())
        self.body = None
        self.message_attributes = None

    def __hash__(self):
        return hash(self.receipt_handle)

    def __eq__(self, other):
        return self.__hash__() == other.__hash__()

    def __str__(self):
        return (
            f"FakeSQSMessage(queue_url={self.queue_url}, receipt_handle={self.receipt_handle}, "
            f"body={self.body}, attributes={self.attributes}, message_attributes={self.message_attributes})"
        )

    def delete(self):
        if self.queue_url == _FakeStatelessLoggingSQSDeadLetterQueue.url:
            pass  # not a persistent queue
        elif self.queue_url == _FakeFileBackedSQSQueue.instance().url:
            _FakeFileBackedSQSQueue.instance()._remove(self)
        elif self.queue_url == _FakeUnitTestFileBackedSQSQueue.instance().url:
            _FakeUnitTestFileBackedSQSQueue.instance()._remove(self)
        else:
            raise ValueError(
                f"Cannot locate queue instance with url = {self.queue_url}, from which to delete the message"
            )

    def change_visibility(self, VisibilityTimeout):  # noqa
        # Do nothing
        pass

    @property
    def attributes(self):
        # This is where more SQS message `attributes` can be mocked
        return {"ApproximateReceiveCount": "1"}  # NOTE: Not an accurate count of try attempts. Just returning a number


def get_sqs_queue(region_name=settings.USASPENDING_AWS_REGION, queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME):
    if settings.IS_LOCAL:
        # Ensure the "singleton" instances of the queue is returned, so that multiple consumers, possibly on
        # different processes or threads, are accessing the same queue data
        if queue_name == UNITTEST_FAKE_DEAD_LETTER_QUEUE_NAME:
            return _FakeStatelessLoggingSQSDeadLetterQueue()
        return _FakeFileBackedSQSQueue.instance()
    else:
        # stuff that's in get_queue
        sqs = boto3.resource("sqs", endpoint_url=f"https://sqs.{region_name}.amazonaws.com", region_name=region_name)
        queue = sqs.get_queue_by_name(QueueName=queue_name)
        return queue
