from django.conf import settings

import boto3
import csv


# Used by bulk and baby download
def sqs_queue(region_name=settings.USASPENDING_AWS_REGION, queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME):
    # stuff that's in get_queue
    sqs = boto3.resource('sqs', region_name=region_name)
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    return queue


def csv_row_count(filename, has_header=True):
    """
        Simple and efficient utility function to provide the rows in a vald CSV file
        If a header is not present, set head_header parameter to False
    """
    with open(filename, "r") as f:
        row_count = sum(1 for row in csv.reader(f))
    if has_header:
        row_count -= 1

    return row_count if row_count >= 0 else 0
