# Set environment variables before importing the worker module
import os

import pytest
from botocore.exceptions import ClientError
from pytest_mock import MockerFixture

# Set environment variables before importing
# Required for the worker module
os.environ["SQS_QUEUE_URL"] = "https://sqs.region.amazonaws.com/123456789012/test-queue"
os.environ["S3_BUCKET"] = "test-bucket"

# Required for boto3 to work in clean CI environments without AWS CLI
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test-access-key")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test-secret-key")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import worker.main as main


class TestPollMessages:
    """Test suite for the poll_messages function."""

    def test_poll_messages_with_single_message(self, mocker: MockerFixture) -> None:
        """Test processing a single message successfully."""
        # Create mocks using pytest-mock
        mock_sqs = mocker.MagicMock()
        mock_s3 = mocker.MagicMock()

        # Mock time.time() for predictable S3 key
        mock_time = 1234567890
        mocker.patch("time.time", return_value=mock_time)

        # Setup mocks in the main module
        mocker.patch.object(main, "sqs", mock_sqs)
        mocker.patch.object(main, "s3", mock_s3)

        # Setup SQS mock to return one message
        test_message = {
            "Body": '{"test": "data"}',
            "ReceiptHandle": "test-receipt-handle",
        }
        mock_sqs.receive_message.return_value = {"Messages": [test_message]}
        mock_sqs.delete_message.return_value = {}
        mock_s3.put_object.return_value = {}

        # Execute function
        main.poll_messages()

        # Verify SQS receive_message was called with correct parameters
        mock_sqs.receive_message.assert_called_once_with(
            QueueUrl="https://sqs.region.amazonaws.com/123456789012/test-queue",
            MaxNumberOfMessages=5,
            WaitTimeSeconds=10,
        )

        # Verify S3 put_object was called with correct parameters
        expected_key = f"message-{mock_time}.json"
        mock_s3.put_object.assert_called_once_with(
            Bucket="test-bucket", Key=expected_key, Body='{"test": "data"}'
        )

        # Verify SQS delete_message was called with correct parameters
        mock_sqs.delete_message.assert_called_once_with(
            QueueUrl="https://sqs.region.amazonaws.com/123456789012/test-queue",
            ReceiptHandle="test-receipt-handle",
        )

    def test_poll_messages_with_multiple_messages(self, mocker: MockerFixture) -> None:
        """Test processing multiple messages successfully."""
        # Create mocks using pytest-mock
        mock_sqs = mocker.MagicMock()
        mock_s3 = mocker.MagicMock()

        # Mock time.time() to return increasing values
        time_values = [1234567890, 1234567891, 1234567892]
        time_iter = iter(time_values)
        mocker.patch("time.time", side_effect=lambda: next(time_iter))

        # Setup mocks in the main module
        mocker.patch.object(main, "sqs", mock_sqs)
        mocker.patch.object(main, "s3", mock_s3)

        # Setup SQS mock to return multiple messages
        test_messages = [
            {"Body": '{"message": 1}', "ReceiptHandle": "handle-1"},
            {"Body": '{"message": 2}', "ReceiptHandle": "handle-2"},
            {"Body": '{"message": 3}', "ReceiptHandle": "handle-3"},
        ]
        mock_sqs.receive_message.return_value = {"Messages": test_messages}
        mock_sqs.delete_message.return_value = {}
        mock_s3.put_object.return_value = {}

        # Execute function
        main.poll_messages()

        # Verify all messages were processed
        assert mock_s3.put_object.call_count == 3
        assert mock_sqs.delete_message.call_count == 3

        # Verify S3 calls - using mocker.call instead of unittest.mock.call
        expected_s3_calls = [
            mocker.call(
                Bucket="test-bucket",
                Key="message-1234567890.json",
                Body='{"message": 1}',
            ),
            mocker.call(
                Bucket="test-bucket",
                Key="message-1234567891.json",
                Body='{"message": 2}',
            ),
            mocker.call(
                Bucket="test-bucket",
                Key="message-1234567892.json",
                Body='{"message": 3}',
            ),
        ]
        mock_s3.put_object.assert_has_calls(expected_s3_calls)

        # Verify SQS delete calls
        expected_delete_calls = [
            mocker.call(
                QueueUrl="https://sqs.region.amazonaws.com/123456789012/test-queue",
                ReceiptHandle="handle-1",
            ),
            mocker.call(
                QueueUrl="https://sqs.region.amazonaws.com/123456789012/test-queue",
                ReceiptHandle="handle-2",
            ),
            mocker.call(
                QueueUrl="https://sqs.region.amazonaws.com/123456789012/test-queue",
                ReceiptHandle="handle-3",
            ),
        ]
        mock_sqs.delete_message.assert_has_calls(expected_delete_calls)

    def test_poll_messages_with_no_messages(self, mocker: MockerFixture) -> None:
        """Test handling when no messages are available."""
        # Create mocks using pytest-mock
        mock_sqs = mocker.MagicMock()
        mock_s3 = mocker.MagicMock()

        # Setup mocks in the main module
        mocker.patch.object(main, "sqs", mock_sqs)
        mocker.patch.object(main, "s3", mock_s3)

        # Setup SQS mock to return no messages
        mock_sqs.receive_message.return_value = {}

        # Execute function
        main.poll_messages()

        # Verify receive_message was called
        mock_sqs.receive_message.assert_called_once()

        # Verify no S3 or delete operations were performed
        mock_s3.put_object.assert_not_called()
        mock_sqs.delete_message.assert_not_called()

    def test_poll_messages_with_empty_messages_list(
        self, mocker: MockerFixture
    ) -> None:
        """Test handling when Messages key exists but list is empty."""
        # Create mocks using pytest-mock
        mock_sqs = mocker.MagicMock()
        mock_s3 = mocker.MagicMock()

        # Setup mocks in the main module
        mocker.patch.object(main, "sqs", mock_sqs)
        mocker.patch.object(main, "s3", mock_s3)

        # Setup SQS mock to return empty messages list
        mock_sqs.receive_message.return_value = {"Messages": []}

        # Execute function
        main.poll_messages()

        # Verify receive_message was called
        mock_sqs.receive_message.assert_called_once()

        # Verify no S3 or delete operations were performed
        mock_s3.put_object.assert_not_called()
        mock_sqs.delete_message.assert_not_called()

    def test_poll_messages_with_aws_client_error(
        self, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test handling AWS ClientError exceptions."""
        # Create mocks using pytest-mock
        mock_sqs = mocker.MagicMock()
        mock_s3 = mocker.MagicMock()

        # Setup mocks in the main module
        mocker.patch.object(main, "sqs", mock_sqs)
        mocker.patch.object(main, "s3", mock_s3)

        # Setup SQS mock to raise ClientError
        mock_sqs.receive_message.side_effect = ClientError(
            error_response={
                "Error": {
                    "Code": "InvalidParameterValue",
                    "Message": "Invalid queue URL",
                }
            },
            operation_name="ReceiveMessage",
        )

        # Execute function
        main.poll_messages()

        # Verify error was printed
        captured = capsys.readouterr()
        assert "AWS error:" in captured.out
        assert "InvalidParameterValue" in captured.out

    def test_poll_messages_s3_error_continues_processing(
        self, mocker: MockerFixture, capsys: pytest.CaptureFixture[str]
    ) -> None:
        """Test that S3 errors are handled gracefully and processing continues."""
        # Create mocks using pytest-mock
        mock_sqs = mocker.MagicMock()
        mock_s3 = mocker.MagicMock()

        # Setup mocks in the main module
        mocker.patch.object(main, "sqs", mock_sqs)
        mocker.patch.object(main, "s3", mock_s3)

        # Setup SQS to return a message
        test_message = {"Body": '{"test": "data"}', "ReceiptHandle": "test-handle"}
        mock_sqs.receive_message.return_value = {"Messages": [test_message]}

        # Setup S3 to raise an error
        mock_s3.put_object.side_effect = ClientError(
            error_response={
                "Error": {"Code": "NoSuchBucket", "Message": "Bucket does not exist"}
            },
            operation_name="PutObject",
        )

        # Execute function
        main.poll_messages()

        # Verify error was handled and printed
        captured = capsys.readouterr()
        assert "AWS error:" in captured.out
        assert "NoSuchBucket" in captured.out


class TestHandler:
    """Test suite for the handler function."""

    def test_handler_calls_poll_messages(self, mocker: MockerFixture) -> None:
        """Test that handler calls poll_messages in a loop."""
        # Mock poll_messages and time.sleep to prevent infinite loop
        mock_poll = mocker.patch.object(main, "poll_messages")
        mock_sleep = mocker.patch("time.sleep")

        # Create a side effect that stops the loop after 2 iterations
        call_count = 0

        def stop_after_two() -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 2:
                raise KeyboardInterrupt("Stop test loop")

        mock_poll.side_effect = stop_after_two

        # Execute function and expect it to be interrupted
        with pytest.raises(KeyboardInterrupt):
            main.handler()

        # Verify poll_messages was called multiple times
        assert mock_poll.call_count == 2
        # Verify sleep was called (should be one less than poll_messages due to interrupt)
        assert mock_sleep.call_count >= 1
        mock_sleep.assert_called_with(5)

    def test_handler_with_event_and_context(self, mocker: MockerFixture) -> None:
        """Test that handler accepts event and context parameters."""
        # Mock poll_messages to stop after first call
        mock_poll = mocker.patch.object(main, "poll_messages")
        mocker.patch("time.sleep")  # Mock sleep to prevent delays

        # Stop after first call
        mock_poll.side_effect = KeyboardInterrupt("Stop test loop")

        # Execute function with parameters
        with pytest.raises(KeyboardInterrupt):
            main.handler({"test": "event"}, {"test": "context"})

        # Verify it still works with parameters
        mock_poll.assert_called_once()
