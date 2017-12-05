from django.conf import settings

import boto3


# Used by bulk and baby download
def sqs_queue(region_name=settings.CSV_AWS_REGION, QueueName=settings.CSV_SQS_QUEUE_NAME):
    # stuff that's in get_queue
    sqs = boto3.resource('sqs', region_name=region_name)
    queue = sqs.get_queue_by_name(QueueName=QueueName)
    return queue

