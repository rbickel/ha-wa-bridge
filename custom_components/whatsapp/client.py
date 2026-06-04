import asyncio
import json
import logging
import random
import aiohttp
from typing import Callable, Optional

from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

# Reconnection constants
BASE_DELAY = 5  # seconds
MAX_DELAY = 300  # 5 minutes ceiling
JITTER = 0.2  # ±20%
MAX_RETRIES = 20  # Max consecutive failures before stopping


def next_delay(attempt: int) -> float:
    """Calculate next reconnection delay with exponential backoff and jitter."""
    delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
    return delay * (1 + random.uniform(-JITTER, JITTER))


class WhatsAppBridge:
    def __init__(self, hass: HomeAssistant, host: str):
        self.hass = hass
        self.host = host
        self._session = None
        self._ws = None
        self._running = False
        self.connection_status = "disconnected"
        self._connection_attempt = 0
        self._consecutive_failures = 0
        self._last_error_type = None
        self._max_retries_exceeded = False
        self._state_callback: Optional[Callable] = None

    def set_state_callback(self, callback: Callable):
        """Set callback for connection state changes."""
        self._state_callback = callback

    def _classify_error(self, error: Exception) -> tuple[str, str]:
        """Classify connection errors and return (error_type, log_level)."""
        error_str = str(error)

        if "104" in error_str or "Connection reset by peer" in error_str:
            return ("connection_reset", "warning")  # Bridge crashed/restarting
        elif "111" in error_str or "Connection refused" in error_str:
            return ("connection_refused", "error")  # Bridge not running
        elif "Server disconnected" in error_str or "disconnected" in error_str.lower():
            return ("clean_disconnect", "info")  # Intentional restart
        else:
            return ("unknown", "error")  # Unknown error

    async def _notify_state_change(self):
        """Notify state callback if registered."""
        if self._state_callback:
            await self._state_callback()

    async def start(self, event_callback=None):
        self._running = True

        while self._running:
            if self._max_retries_exceeded:
                # Stop retrying if max retries exceeded
                _LOGGER.debug("Max retries exceeded, waiting for manual intervention")
                await asyncio.sleep(30)  # Check periodically if reset
                continue

            try:
                if not self._session:
                    self._session = aiohttp.ClientSession()

                _LOGGER.info("Connecting to WhatsApp Bridge at %s", self.host)
                async with self._session.ws_connect(self.host) as ws:
                    self._ws = ws

                    # Connection successful - reset failure tracking
                    attempts_used = self._consecutive_failures
                    self._consecutive_failures = 0
                    self._connection_attempt = 0
                    self._last_error_type = None
                    self._max_retries_exceeded = False

                    self.connection_status = "connected"
                    await self._notify_state_change()

                    if attempts_used > 0:
                        _LOGGER.info("Connected to WhatsApp Bridge after %d attempts", attempts_used + 1)
                    else:
                        _LOGGER.info("Connected to WhatsApp Bridge")

                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if event_callback:
                                await event_callback(data)
                        elif msg.type == aiohttp.WSMsgType.ERROR:
                            _LOGGER.error("WhatsApp Bridge connection error: %s", ws.exception())
                            break

            except Exception as e:
                self._consecutive_failures += 1
                error_type, log_level = self._classify_error(e)

                # Log first occurrence at appropriate level, subsequent at DEBUG
                is_first_in_streak = (error_type != self._last_error_type or self._consecutive_failures == 1)
                self._last_error_type = error_type

                if is_first_in_streak:
                    if log_level == "error":
                        _LOGGER.error("Error connecting to WhatsApp Bridge: %s", e)
                    elif log_level == "warning":
                        _LOGGER.warning("Error connecting to WhatsApp Bridge: %s", e)
                    else:
                        _LOGGER.info("Error connecting to WhatsApp Bridge: %s", e)
                else:
                    _LOGGER.debug("Error connecting to WhatsApp Bridge (attempt %d): %s",
                                 self._consecutive_failures, e)

                self.connection_status = "error"

                # Check if max retries exceeded
                if self._consecutive_failures >= MAX_RETRIES:
                    _LOGGER.error(
                        "Max retry limit (%d) exceeded. Stopping automatic reconnection. "
                        "Please check the bridge service and restart the integration.",
                        MAX_RETRIES
                    )
                    self._max_retries_exceeded = True
                    self.connection_status = "unavailable"
                    await self._notify_state_change()
                    # Create repair issue
                    from homeassistant.helpers import issue_registry as ir
                    ir.async_create_issue(
                        self.hass,
                        "whatsapp",
                        "bridge_connection_failed",
                        is_fixable=False,
                        severity=ir.IssueSeverity.ERROR,
                        translation_key="bridge_connection_failed",
                        translation_placeholders={
                            "host": self.host,
                            "attempts": str(MAX_RETRIES),
                        },
                    )
                    continue

            if self._running and not self._max_retries_exceeded:
                self.connection_status = "reconnecting"
                await self._notify_state_change()

                # Calculate delay with exponential backoff
                delay = next_delay(self._connection_attempt)
                _LOGGER.debug("Reconnecting in %.1f seconds...", delay)
                self._connection_attempt += 1

                await asyncio.sleep(delay)

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()

    def reset_retry_limit(self):
        """Reset the max retry exceeded flag to allow reconnection attempts."""
        if self._max_retries_exceeded:
            _LOGGER.info("Resetting retry limit, will resume reconnection attempts")
            self._max_retries_exceeded = False
            self._consecutive_failures = 0
            self._connection_attempt = 0
            self._last_error_type = None
            # Dismiss repair issue
            from homeassistant.helpers import issue_registry as ir
            ir.async_delete_issue(self.hass, "whatsapp", "bridge_connection_failed")

    async def send_message(self, number: str | None, message: str, group_name: str | None = None, group_id: str | None = None, media: dict | None = None):
        """Send a message via the bridge."""
        if not self._ws or self._ws.closed:
            _LOGGER.warning("Bridge not connected, cannot send message")
            return

        payload = {
            "type": "send_message",
            "message": message
        }

        if number:
            payload["number"] = number

        if group_name:
            payload["group_name"] = group_name

        if group_id:
            payload["group_id"] = group_id

        if media:
            payload["media"] = media

        if not number and not group_name and not group_id:
             _LOGGER.error("Neither number, group_name, nor group_id provided")
             return

        await self._ws.send_json(payload)

    async def send_broadcast(self, targets: list[str], message: str, media: dict | None = None):
        """Send a broadcast message via the bridge."""
        if not self._ws or self._ws.closed:
            _LOGGER.warning("Bridge not connected, cannot send broadcast")
            return

        payload = {
            "type": "broadcast",
            "targets": targets,
            "message": message
        }
        
        if media:
            payload["media"] = media
        
        await self._ws.send_json(payload)

    async def send_poll(self, number: str | None, group_name: str | None, message: str, options: list[str], allow_multiple_answers: bool, group_id: str | None = None):
        """Send a poll via the bridge."""
        if not self._ws or self._ws.closed:
            _LOGGER.warning("Bridge not connected, cannot send poll")
            return

        payload = {
            "type": "send_poll",
            "message": message,
            "options": options,
            "allow_multiple_answers": allow_multiple_answers
        }

        if number:
            payload["number"] = number

        if group_name:
            payload["group_name"] = group_name

        if group_id:
            payload["group_id"] = group_id

        if not number and not group_name and not group_id:
             _LOGGER.error("Neither number, group_name, nor group_id provided for poll")
             return

        await self._ws.send_json(payload)

    async def send_event(self, number: str | None, group_name: str | None, group_id: str | None, name: str, description: str | None = None, location: str | None = None, start_time: str = None, end_time: str | None = None, call_type: str | None = None):
        """Send an event via the bridge."""
        if not self._ws or self._ws.closed:
            _LOGGER.warning("Bridge not connected, cannot send event")
            return

        payload = {
            "type": "send_event",
            "name": name,
            "start_time": start_time,
        }

        if number:
            payload["number"] = number
        if group_name:
            payload["group_name"] = group_name
        if group_id:
            payload["group_id"] = group_id
        if description:
            payload["description"] = description
        if location:
            payload["location"] = location
        if end_time:
            payload["end_time"] = end_time
        if call_type:
            payload["call_type"] = call_type

        if not number and not group_name and not group_id:
            _LOGGER.error("Neither number, group_name, nor group_id provided for event")
            return

        await self._ws.send_json(payload)

    async def get_groups(self):
        """Request the list of groups from the bridge."""
        if not self._ws or self._ws.closed:
            _LOGGER.warning("Bridge not connected, cannot get groups")
            return

        await self._ws.send_json({"type": "get_groups"})

    async def set_group_subject(self, group_id: str, subject: str):
        """Set a group's subject (name) via the bridge."""
        if not self._ws or self._ws.closed:
            _LOGGER.warning("Bridge not connected, cannot set group subject")
            return

        await self._ws.send_json({
            "type": "set_group_subject",
            "group_id": group_id,
            "subject": subject
        })

    async def set_group_picture(self, group_id: str, media: dict):
        """Set a group's picture via the bridge."""
        if not self._ws or self._ws.closed:
            _LOGGER.warning("Bridge not connected, cannot set group picture")
            return

        await self._ws.send_json({
            "type": "set_group_picture",
            "group_id": group_id,
            "media": media
        })
