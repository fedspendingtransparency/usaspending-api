import boto3
import codecs
import csv

from django.conf import settings


# Used by non-local downloads
def sqs_queue(region_name=settings.USASPENDING_AWS_REGION, queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME):
    # stuff that's in get_queue
    sqs = boto3.resource('sqs', region_name=region_name)
    queue = sqs.get_queue_by_name(QueueName=queue_name)
    return queue


def count_rows_in_csv_file(filename, has_header=True, safe=True):
    """
        Simple and efficient utility function to provide the rows in a vald CSV file
        If a header is not present, set head_header parameter to False

        Added "safe" mode which will handle any NUL BYTES in CSV files
        It does increase the function runtime by approx 10%.
            Example: using safe mode, it now takes 10s for ~1 million lines in a CSV

    """
    with codecs.open(filename, "r") as f:
        if safe:
            row_count = sum(1 for row in csv.reader((line.replace(r"\0", "") for line in f)))
        else:
            row_count = sum(1 for row in csv.reader(f))
    if has_header and row_count > 0:
        row_count -= 1

    return row_count
