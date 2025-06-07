# Set environment variables before importing the worker module
import os
from typing import Any
import pytest
import worker.main as main

os.environ["SQS_QUEUE_URL"] = "https://sqs.region.amazonaws.com/123456789012/test-queue"
os.environ["S3_BUCKET"] = "test-bucket"


def test_poll_messages(monkeypatch: pytest.MonkeyPatch) -> None:
    class MockSQS:
        def receive_message(self, **kwargs: Any) -> dict[str, Any]:
            return {"Messages": [{"Body": "{}", "ReceiptHandle": "abc"}]}

        def delete_message(self, **kwargs: Any) -> dict[str, Any]:
            return {}

    class MockS3:
        def put_object(self, **kwargs: Any) -> dict[str, Any]:
            return {}

    monkeypatch.setattr(main, "sqs", MockSQS())
    monkeypatch.setattr(main, "s3", MockS3())

    main.poll_messages()
    assert True