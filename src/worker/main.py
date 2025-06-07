import os
import time
from typing import Any

import boto3
from botocore.exceptions import ClientError
from mypy_boto3_s3 import S3Client
from mypy_boto3_sqs import SQSClient
from mypy_boto3_sqs.type_defs import MessageTypeDef

sqs: SQSClient = boto3.client("sqs")
s3: S3Client = boto3.client("s3")

SQS_URL: str = os.environ["SQS_QUEUE_URL"]
S3_BUCKET: str = os.environ["S3_BUCKET"]


def poll_messages() -> None:
    try:
        messages: list[MessageTypeDef] = sqs.receive_message(
            QueueUrl=SQS_URL, MaxNumberOfMessages=5, WaitTimeSeconds=10
        ).get("Messages", [])

        for msg in messages:
            body: str = msg["Body"]
            key: str = f"message-{int(time.time())}.json"
            s3.put_object(Bucket=S3_BUCKET, Key=key, Body=body)

            sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=msg["ReceiptHandle"])
            print(f"Processed message to {key}")

    except ClientError as e:
        print(f"AWS error: {e}")


def handler(event: Any | None = None, context: Any | None = None) -> None:
    while True:
        poll_messages()
        time.sleep(5)
