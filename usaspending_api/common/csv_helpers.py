from django.conf import settings

import boto3


# Used by bulk and baby download
def sqs_queue(region_name=settings.USASPENDING_AWS_REGION, queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME):
    # stuff that's in get_queue
    sqs = boto3.resource('sqs', region_name=region_name)
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    return queue
