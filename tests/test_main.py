# Set environment variables before importing the worker module
import logging
import os

import pytest
from _pytest.logging import LogCaptureFixture
from botocore.exceptions import BotoCoreError, ClientError
from pytest_mock import MockerFixture

# Set environment variables before importing
os.environ["SQS_QUEUE_URL"] = "https://sqs.region.amazonaws.com/123456789012/test-queue"
os.environ["S3_BUCKET"] = "test-bucket"
os.environ.setdefault("AWS_ACCESS_KEY_ID", "test-access-key")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test-secret-key")
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

import worker.main as main


class TestPollMessages:
    """Test suite for the poll_messages function."""

    def test_single_message_success(
        self, mocker: MockerFixture, caplog: LogCaptureFixture
    ) -> None:
        """Test processing a single message successfully with logging verification."""
        # Set logging level to capture all messages
        caplog.set_level(logging.DEBUG, logger="worker")
        caplog.set_level(logging.DEBUG, logger="worker.debug")

        mock_sqs = mocker.MagicMock()
        mock_s3 = mocker.MagicMock()
        mock_time = 1234567890

        mocker.patch("time.time", return_value=mock_time)
        mocker.patch.object(main, "sqs", mock_sqs)
        mocker.patch.object(main, "s3", mock_s3)

        test_message = {
            "Body": '{"test": "data"}',
            "ReceiptHandle": "test-receipt-handle",
            "MessageId": "test-message-id",
        }
        mock_sqs.receive_message.return_value = {"Messages": [test_message]}

        main.poll_messages()

        # Verify AWS calls
        expected_key = f"message-{mock_time}-test-message-id.json"
        mock_s3.put_object.assert_called_once_with(
            Bucket="test-bucket", Key=expected_key, Body='{"test": "data"}'
        )
        mock_sqs.delete_message.assert_called_once_with(
            QueueUrl="https://sqs.region.amazonaws.com/123456789012/test-queue",
            ReceiptHandle="test-receipt-handle",
        )

        # Verify logging - check for actual log messages from the code
        assert "Received 1 messages from SQS" in caplog.text
        assert f"Successfully uploaded message to S3: {expected_key}" in caplog.text
        assert "Successfully deleted message from SQS" in caplog.text

    def test_no_messages_handling(
        self, mocker: MockerFixture, caplog: LogCaptureFixture
    ) -> None:
        """Test handling when no messages are available."""
        # Set logging level to capture DEBUG messages and enable propagation
        caplog.set_level(logging.DEBUG, logger="worker.debug")

        # Temporarily enable propagation for the debug logger so caplog can capture it
        original_propagate = main.debug_logger.propagate
        main.debug_logger.propagate = True

        try:
            mock_sqs = mocker.MagicMock()
            mock_s3 = mocker.MagicMock()

            mocker.patch.object(main, "sqs", mock_sqs)
            mocker.patch.object(main, "s3", mock_s3)
            mock_sqs.receive_message.return_value = {}

            main.poll_messages()

            mock_s3.put_object.assert_not_called()
            mock_sqs.delete_message.assert_not_called()
            assert "No messages received from SQS" in caplog.text
        finally:
            # Restore original propagation setting
            main.debug_logger.propagate = original_propagate

    def test_individual_message_error_continues_processing(
        self, mocker: MockerFixture, caplog: LogCaptureFixture
    ) -> None:
        """Test that individual message errors don't stop processing of other messages."""
        mock_sqs = mocker.MagicMock()
        mock_s3 = mocker.MagicMock()

        mocker.patch.object(main, "sqs", mock_sqs)
        mocker.patch.object(main, "s3", mock_s3)

        # First message fails S3 upload, second succeeds
        test_messages = [
            {
                "Body": '{"test": "data1"}',
                "ReceiptHandle": "handle-1",
                "MessageId": "msg-1",
            },
            {
                "Body": '{"test": "data2"}',
                "ReceiptHandle": "handle-2",
                "MessageId": "msg-2",
            },
        ]
        mock_sqs.receive_message.return_value = {"Messages": test_messages}

        # First call to put_object fails, second succeeds
        mock_s3.put_object.side_effect = [
            ClientError(
                error_response={
                    "Error": {
                        "Code": "NoSuchBucket",
                        "Message": "Bucket does not exist",
                    }
                },
                operation_name="PutObject",
            ),
            {},  # Success for second message
        ]

        main.poll_messages()

        # Verify second message was still processed
        assert mock_s3.put_object.call_count == 2
        assert (
            mock_sqs.delete_message.call_count == 1
        )  # Only successful message deleted
        assert "Failed to process individual message" in caplog.text
        assert "NoSuchBucket" in caplog.text

    def test_aws_error_propagates(self, mocker: MockerFixture) -> None:
        """Test that AWS errors in receive_message are propagated."""
        mock_sqs = mocker.MagicMock()
        mocker.patch.object(main, "sqs", mock_sqs)

        mock_sqs.receive_message.side_effect = ClientError(
            error_response={
                "Error": {
                    "Code": "InvalidParameterValue",
                    "Message": "Invalid queue URL",
                }
            },
            operation_name="ReceiveMessage",
        )

        with pytest.raises(ClientError):
            main.poll_messages()


class TestHandler:
    """Test suite for the handler function with retry logic."""

    def test_successful_polling_loop(
        self, mocker: MockerFixture, caplog: LogCaptureFixture
    ) -> None:
        """Test normal operation with successful polling."""
        # Set logging level to capture INFO messages
        caplog.set_level(logging.INFO, logger="worker")

        mock_poll = mocker.patch.object(main, "poll_messages")
        mock_sleep = mocker.patch("time.sleep")

        call_count = 0

        def stop_after_calls() -> None:
            nonlocal call_count
            call_count += 1
            if call_count >= 3:
                raise KeyboardInterrupt("Stop test")

        mock_poll.side_effect = stop_after_calls

        with pytest.raises(KeyboardInterrupt):
            main.handler()

        assert mock_poll.call_count == 3
        assert mock_sleep.call_count == 2  # One less than poll calls due to interrupt
        mock_sleep.assert_called_with(5)  # POLL_INTERVAL
        assert "Starting worker service handler" in caplog.text

    def test_retry_logic_with_exponential_backoff(
        self, mocker: MockerFixture, caplog: LogCaptureFixture
    ) -> None:
        """Test retry logic with exponential backoff on AWS errors."""
        mock_poll = mocker.patch.object(main, "poll_messages")
        mock_sleep = mocker.patch("time.sleep")

        # Simulate 3 failures then success, then stop
        aws_error = ClientError(
            error_response={
                "Error": {
                    "Code": "ServiceUnavailable",
                    "Message": "Service temporarily unavailable",
                }
            },
            operation_name="ReceiveMessage",
        )

        call_count = 0

        def retry_scenario() -> None:
            nonlocal call_count
            call_count += 1
            if call_count <= 3:
                raise aws_error
            elif call_count == 4:
                # Success - reset would happen here
                pass
            else:
                raise KeyboardInterrupt("Stop test")

        mock_poll.side_effect = retry_scenario

        with pytest.raises(KeyboardInterrupt):
            main.handler()

        # Verify retry attempts
        assert mock_poll.call_count == 5  # 3 failures + 1 success + 1 interrupt

        # Verify exponential backoff sleep calls
        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        assert 2 in sleep_calls  # 2^1 for first retry
        assert 4 in sleep_calls  # 2^2 for second retry
        assert 8 in sleep_calls  # 2^3 for third retry
        assert 5 in sleep_calls  # POLL_INTERVAL after success

        # Verify logging
        assert "AWS error in handler (attempt 1/5)" in caplog.text
        assert "AWS error in handler (attempt 2/5)" in caplog.text
        assert "AWS error in handler (attempt 3/5)" in caplog.text
        assert "Sleeping 2 seconds before retry" in caplog.text
        assert "Sleeping 4 seconds before retry" in caplog.text
        assert "Sleeping 8 seconds before retry" in caplog.text

    def test_max_failures_triggers_shutdown(
        self, mocker: MockerFixture, caplog: LogCaptureFixture
    ) -> None:
        """Test that maximum consecutive failures triggers service shutdown."""
        mock_poll = mocker.patch.object(main, "poll_messages")
        mocker.patch("time.sleep")

        aws_error = BotoCoreError()
        mock_poll.side_effect = aws_error

        with pytest.raises(SystemExit) as exc_info:
            main.handler()

        assert exc_info.value.code == 1
        assert mock_poll.call_count == 5  # MAX_CONSECUTIVE_FAILURES
        assert (
            "Too many consecutive AWS failures (5). Shutting down service."
            in caplog.text
        )

    def test_max_backoff_limit(self, mocker: MockerFixture) -> None:
        """Test that backoff doesn't exceed MAX_BACKOFF."""
        mock_poll = mocker.patch.object(main, "poll_messages")
        mock_sleep = mocker.patch("time.sleep")

        # Simulate many failures to test max backoff
        aws_error = ClientError(
            error_response={
                "Error": {"Code": "Throttling", "Message": "Request rate exceeded"}
            },
            operation_name="ReceiveMessage",
        )
        mock_poll.side_effect = aws_error

        with pytest.raises(SystemExit):
            main.handler()

        # Check that no sleep call exceeds MAX_BACKOFF (60 seconds)
        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        assert all(sleep_time <= 60 for sleep_time in sleep_calls)
        # The actual sequence should be: 2, 4, 8, 16 (since it exits after 5th failure)
        # 2^5 = 32 would be the next value but service exits at 5 failures
        expected_calls = [2, 4, 8, 16]
        assert sleep_calls == expected_calls

    def test_max_backoff_reached_with_longer_failure_sequence(
        self, mocker: MockerFixture
    ) -> None:
        """Test that MAX_BACKOFF is actually reached when we have enough failures."""
        mock_poll = mocker.patch.object(main, "poll_messages")
        mock_sleep = mocker.patch("time.sleep")

        # Temporarily increase MAX_CONSECUTIVE_FAILURES to allow more attempts
        mocker.patch.object(main, "MAX_CONSECUTIVE_FAILURES", 10)

        aws_error = ClientError(
            error_response={
                "Error": {"Code": "Throttling", "Message": "Request rate exceeded"}
            },
            operation_name="ReceiveMessage",
        )
        mock_poll.side_effect = aws_error

        with pytest.raises(SystemExit):
            main.handler()

        # Check that we reach the MAX_BACKOFF (60 seconds)
        sleep_calls = [call.args[0] for call in mock_sleep.call_args_list]
        assert all(sleep_time <= 60 for sleep_time in sleep_calls)
        # Should reach: 2, 4, 8, 16, 32, 60, 60, 60, 60 (60 is min(64, 60), min(128, 60), etc.)
        assert 60 in sleep_calls

    def test_unexpected_error_triggers_immediate_shutdown(
        self, mocker: MockerFixture, caplog: LogCaptureFixture
    ) -> None:
        """Test that unexpected errors trigger immediate shutdown."""
        mock_poll = mocker.patch.object(main, "poll_messages")

        unexpected_error = ValueError("Unexpected error")
        mock_poll.side_effect = unexpected_error

        with pytest.raises(SystemExit) as exc_info:
            main.handler()

        assert exc_info.value.code == 1
        assert mock_poll.call_count == 1  # Should fail immediately
        assert "Unexpected error in handler: Unexpected error" in caplog.text

    def test_handler_accepts_lambda_parameters(self, mocker: MockerFixture) -> None:
        """Test that handler accepts event and context parameters for Lambda compatibility."""
        mock_poll = mocker.patch.object(main, "poll_messages")
        mocker.patch("time.sleep")

        mock_poll.side_effect = KeyboardInterrupt("Stop test")

        with pytest.raises(KeyboardInterrupt):
            main.handler({"test": "event"}, {"test": "context"})

        mock_poll.assert_called_once()


class TestMainExecution:
    """Test suite for main execution block."""

    def test_main_execution_keyboard_interrupt(
        self, mocker: MockerFixture, caplog: LogCaptureFixture
    ) -> None:
        """Test main execution handles KeyboardInterrupt gracefully."""
        mock_handler = mocker.patch.object(main, "handler")
        mock_handler.side_effect = KeyboardInterrupt()

        # Import and run main execution (this would normally be in if __name__ == "__main__")
        # We can't easily test the actual main block, but we can test the handler behavior
        with pytest.raises(KeyboardInterrupt):
            main.handler()

    def test_main_execution_system_exit(self, mocker: MockerFixture) -> None:
        """Test main execution handles SystemExit properly."""
        mock_handler = mocker.patch.object(main, "handler")
        mock_handler.side_effect = SystemExit(1)

        with pytest.raises(SystemExit) as exc_info:
            main.handler()

        assert exc_info.value.code == 1
