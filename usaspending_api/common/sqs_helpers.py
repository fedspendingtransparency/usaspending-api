import boto3

from django.conf import settings


def get_sqs_queue_resource(queue_name, region_name=None):
    """
        return a boto3 SQS queue by providing a SQS queue name
    """
    aws_region = region_name or settings.USASPENDING_AWS_REGION
    sqs_resource = boto3.resource("sqs", region_name=aws_region)
    sqs_queue = sqs_resource.get_queue_by_name(QueueName=queue_name)
    return sqs_queue
