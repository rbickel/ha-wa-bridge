import asyncio
import json
import logging
import aiohttp
from datetime import datetime

from homeassistant.core import HomeAssistant

_LOGGER = logging.getLogger(__name__)

# Connection retry configuration
MIN_RETRY_DELAY = 5  # seconds
MAX_RETRY_DELAY = 300  # 5 minutes max
BACKOFF_MULTIPLIER = 2.0

class WhatsAppBridge:
    def __init__(self, hass: HomeAssistant, host: str):
        self.hass = hass
        self.host = host
        self._session = None
        self._ws = None
        self._running = False
        self.connection_status = "disconnected"
        self._retry_delay = MIN_RETRY_DELAY
        self._consecutive_failures = 0
        self._last_successful_connection = None
        self._total_reconnects = 0
        self._connection_start_time = None

    async def start(self, event_callback=None):
        self._running = True

        while self._running:
            try:
                if not self._session:
                    self._session = aiohttp.ClientSession()

                _LOGGER.info("Connecting to WhatsApp Bridge at %s (attempt after %d failures)",
                           self.host, self._consecutive_failures)

                self._connection_start_time = datetime.now()
                async with self._session.ws_connect(
                    self.host,
                    heartbeat=30,  # Send ping every 30 seconds to keep connection alive
                    timeout=aiohttp.ClientTimeout(total=None, connect=10, sock_read=60)
                ) as ws:
                    self._ws = ws
                    self.connection_status = "connected"

                    # Reset failure tracking on successful connection
                    self._consecutive_failures = 0
                    self._retry_delay = MIN_RETRY_DELAY
                    self._last_successful_connection = datetime.now()

                    if self._total_reconnects > 0:
                        _LOGGER.info("Reconnected to WhatsApp Bridge (total reconnects: %d)",
                                   self._total_reconnects)
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
                        elif msg.type == aiohttp.WSMsgType.CLOSED:
                            connection_duration = (datetime.now() - self._connection_start_time).total_seconds()
                            _LOGGER.warning("WebSocket closed by server after %.1f seconds", connection_duration)
                            break
                        elif msg.type == aiohttp.WSMsgType.CLOSING:
                            _LOGGER.info("WebSocket is closing")
                            break

            except aiohttp.ClientConnectorError as e:
                self._consecutive_failures += 1
                _LOGGER.error("Failed to connect to WhatsApp Bridge at %s (connection refused): %s",
                            self.host, e)
                self.connection_status = "error"

            except asyncio.TimeoutError:
                self._consecutive_failures += 1
                _LOGGER.error("Connection to WhatsApp Bridge timed out")
                self.connection_status = "error"

            except Exception as e:
                self._consecutive_failures += 1
                _LOGGER.error("Error connecting to WhatsApp Bridge: %s", e, exc_info=True)
                self.connection_status = "error"

            if self._running:
                self._total_reconnects += 1
                self.connection_status = "reconnecting"

                # Exponential backoff with max delay cap
                if self._consecutive_failures > 1:
                    self._retry_delay = min(
                        self._retry_delay * BACKOFF_MULTIPLIER,
                        MAX_RETRY_DELAY
                    )

                _LOGGER.info(
                    "Reconnecting in %d seconds... (consecutive failures: %d, total reconnects: %d)",
                    int(self._retry_delay), self._consecutive_failures, self._total_reconnects
                )
                await asyncio.sleep(self._retry_delay)

    async def stop(self):
        self._running = False
        if self._ws:
            await self._ws.close()
        if self._session:
            await self._session.close()

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
