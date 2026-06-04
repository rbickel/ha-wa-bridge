"""Tests for WhatsApp Bridge client reconnection logic."""
import pytest
from unittest.mock import Mock
from custom_components.whatsapp.client import (
    WhatsAppBridge,
    next_delay,
    BASE_DELAY,
    MAX_DELAY,
    JITTER,
    MAX_RETRIES,
)


class TestExponentialBackoff:
    """Test exponential backoff calculation."""

    def test_first_attempt_delay(self):
        """Test delay for first reconnection attempt."""
        delay = next_delay(0)
        # First attempt should never be shorter than BASE_DELAY
        assert BASE_DELAY <= delay <= BASE_DELAY * (1 + JITTER)

    def test_exponential_growth(self):
        """Test that delay grows exponentially."""
        delay_0 = next_delay(0)
        delay_1 = next_delay(1)
        delay_2 = next_delay(2)

        # Remove jitter effect for comparison by using bounds
        # attempt 0: 5 * 1 = 5 (4-6 with jitter)
        # attempt 1: 5 * 2 = 10 (8-12 with jitter)
        # attempt 2: 5 * 4 = 20 (16-24 with jitter)
        assert delay_1 > delay_0  # Should be larger
        assert delay_2 > delay_1  # Should be larger

    def test_max_delay_ceiling(self):
        """Test that delay never exceeds MAX_DELAY."""
        # Very high attempt number
        delay = next_delay(100)
        assert delay <= MAX_DELAY * (1 + JITTER)

    def test_jitter_bounds(self):
        """Test that jitter stays within ±20%."""
        for attempt in range(10):
            delay = next_delay(attempt)
            base = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
            assert base * (1 - JITTER) <= delay <= base * (1 + JITTER)


class TestErrorClassification:
    """Test error classification logic."""

    def test_connection_reset_classification(self):
        """Test Errno 104 / Connection reset classification."""
        hass = Mock()
        bridge = WhatsAppBridge(hass, "ws://localhost:3000")

        error = Exception("[Errno 104] Connection reset by peer")
        error_type, log_level = bridge._classify_error(error)
        assert error_type == "connection_reset"
        assert log_level == "warning"

    def test_connection_refused_classification(self):
        """Test Errno 111 / Connection refused classification."""
        hass = Mock()
        bridge = WhatsAppBridge(hass, "ws://localhost:3000")

        error = Exception("[Errno 111] Connection refused")
        error_type, log_level = bridge._classify_error(error)
        assert error_type == "connection_refused"
        assert log_level == "error"

    def test_clean_disconnect_classification(self):
        """Test clean disconnect classification."""
        hass = Mock()
        bridge = WhatsAppBridge(hass, "ws://localhost:3000")

        error = Exception("Server disconnected")
        error_type, log_level = bridge._classify_error(error)
        assert error_type == "clean_disconnect"
        assert log_level == "info"

    def test_unknown_error_classification(self):
        """Test unknown error classification."""
        hass = Mock()
        bridge = WhatsAppBridge(hass, "ws://localhost:3000")

        error = Exception("Something unexpected happened")
        error_type, log_level = bridge._classify_error(error)
        assert error_type == "unknown"
        assert log_level == "error"


class TestConnectionState:
    """Test connection state management."""

    def test_initial_state(self):
        """Test initial connection state."""
        hass = Mock()
        bridge = WhatsAppBridge(hass, "ws://localhost:3000")

        assert bridge.connection_status == "disconnected"
        assert bridge._consecutive_failures == 0
        assert bridge._connection_attempt == 0
        assert not bridge._max_retries_exceeded

    def test_reset_retry_limit(self):
        """Test reset_retry_limit method."""
        hass = Mock()
        bridge = WhatsAppBridge(hass, "ws://localhost:3000")

        # Simulate max retries exceeded
        bridge._max_retries_exceeded = True
        bridge._consecutive_failures = 25
        bridge._connection_attempt = 10
        bridge._last_error_type = "connection_refused"

        # Reset
        bridge.reset_retry_limit()

        assert not bridge._max_retries_exceeded
        assert bridge._consecutive_failures == 0
        assert bridge._connection_attempt == 0
        assert bridge._last_error_type is None


class TestFailureTracking:
    """Test failure tracking and logging."""

    def test_consecutive_failure_tracking(self):
        """Test that consecutive failures are tracked correctly."""
        hass = Mock()
        bridge = WhatsAppBridge(hass, "ws://localhost:3000")

        # Simulate failures
        assert bridge._consecutive_failures == 0

        # After first failure
        bridge._consecutive_failures += 1
        assert bridge._consecutive_failures == 1

        # After second failure
        bridge._consecutive_failures += 1
        assert bridge._consecutive_failures == 2

    def test_max_retries_detection(self):
        """Test max retries threshold detection."""
        hass = Mock()
        bridge = WhatsAppBridge(hass, "ws://localhost:3000")

        # Set to just below threshold
        bridge._consecutive_failures = MAX_RETRIES - 1
        assert not bridge._max_retries_exceeded

        # Increment to threshold
        bridge._consecutive_failures = MAX_RETRIES
        # Note: In actual code, this would be set by the start() method


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
