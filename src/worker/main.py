import logging
import os
import sys
import time
from typing import Any

import boto3
from botocore.exceptions import BotoCoreError, ClientError
from mypy_boto3_s3 import S3Client
from mypy_boto3_s3.type_defs import PutObjectOutputTypeDef
from mypy_boto3_sqs import SQSClient
from mypy_boto3_sqs.type_defs import (
    MessageTypeDef,
    ReceiveMessageResultTypeDef,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# Create debug logger with function names
debug_logger = logging.getLogger("worker.debug")
debug_logger.setLevel(logging.DEBUG)
debug_handler = logging.StreamHandler(sys.stdout)
debug_handler.setFormatter(
    logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(funcName)s:%(lineno)d - %(message)s"
    )
)
debug_logger.addHandler(debug_handler)
debug_logger.propagate = False

# Main logger
logger = logging.getLogger("worker")

# Configuration
AWS_REGION: str = os.environ.get("AWS_DEFAULT_REGION", "us-east-1")
SQS_URL: str = os.environ["SQS_QUEUE_URL"]
S3_BUCKET: str = os.environ["S3_BUCKET"]
MAX_CONSECUTIVE_FAILURES = 5
POLL_INTERVAL = 5
MAX_BACKOFF = 60

logger.info(f"Initializing AWS clients for region: {AWS_REGION}")
debug_logger.debug(f"SQS URL: {SQS_URL}")
debug_logger.debug(f"S3 Bucket: {S3_BUCKET}")

try:
    sqs: SQSClient = boto3.client("sqs", region_name=AWS_REGION)
    s3: S3Client = boto3.client("s3", region_name=AWS_REGION)
    logger.info("AWS clients initialized successfully")
except (BotoCoreError, ClientError) as e:
    logger.error(f"Failed to initialize AWS clients: {e}")
    raise SystemExit(1) from e


def poll_messages() -> None:
    """Poll messages from SQS queue and process them."""
    debug_logger.debug("Starting to poll messages from SQS")

    try:
        response: ReceiveMessageResultTypeDef = sqs.receive_message(
            QueueUrl=SQS_URL, MaxNumberOfMessages=5, WaitTimeSeconds=10
        )
        messages: list[MessageTypeDef] = response.get("Messages", [])

        if not messages:
            debug_logger.debug("No messages received from SQS")
            return

        logger.info(f"Received {len(messages)} messages from SQS")

        for msg in messages:
            try:
                body: str = msg["Body"]
                receipt_handle: str = msg["ReceiptHandle"]
                key: str = (
                    f"message-{int(time.time())}-{msg.get('MessageId', 'unknown')}.json"
                )

                debug_logger.debug(
                    f"Processing message with ID: {msg.get('MessageId', 'unknown')}"
                )

                # Upload to S3
                _s3_response: PutObjectOutputTypeDef = s3.put_object(
                    Bucket=S3_BUCKET, Key=key, Body=body
                )
                logger.info(f"Successfully uploaded message to S3: {key}")

                # Delete from SQS
                sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=receipt_handle)
                logger.info("Successfully deleted message from SQS")

            except (KeyError, ClientError, BotoCoreError) as e:
                logger.error(f"Failed to process individual message: {e}")
                # Continue processing other messages instead of failing completely
                continue

    except (ClientError, BotoCoreError) as e:
        logger.error(f"AWS error during message polling: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during message polling: {e}")
        raise


def handler(event: Any | None = None, context: Any | None = None) -> None:
    """Main handler function that runs the worker service."""
    logger.info("Starting worker service handler")

    consecutive_failures = 0

    while True:
        try:
            poll_messages()
            consecutive_failures = 0  # Reset counter on success
            debug_logger.debug(f"Sleeping for {POLL_INTERVAL} seconds before next poll")
            time.sleep(POLL_INTERVAL)

        except (ClientError, BotoCoreError) as e:
            consecutive_failures += 1
            logger.error(
                f"AWS error in handler (attempt {consecutive_failures}/{MAX_CONSECUTIVE_FAILURES}): {e}"
            )

            if consecutive_failures >= MAX_CONSECUTIVE_FAILURES:
                logger.critical(
                    f"Too many consecutive AWS failures ({consecutive_failures}). Shutting down service."
                )
                raise SystemExit(1) from e

            # Exponential backoff
            sleep_time = min(MAX_BACKOFF, 2**consecutive_failures)
            logger.warning(f"Sleeping {sleep_time} seconds before retry")
            time.sleep(sleep_time)

        except Exception as e:
            logger.critical(f"Unexpected error in handler: {e}")
            raise SystemExit(1) from e


if __name__ == "__main__":
    logger.info("Starting ECS worker service...")
    try:
        handler()
    except KeyboardInterrupt:
        logger.info("Service stopped by user (KeyboardInterrupt)")
    except SystemExit as e:
        logger.error(f"Service exiting with code: {e.code}")
        raise
    except Exception as e:
        logger.critical(f"Service error: {e}")
        raise
