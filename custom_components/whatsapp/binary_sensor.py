"""Binary sensor platform for WhatsApp Integration."""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from homeassistant.components.binary_sensor import (
    BinarySensorDeviceClass,
    BinarySensorEntity,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN

if TYPE_CHECKING:
    from .client import WhatsAppBridge

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up WhatsApp binary sensor based on a config entry."""
    bridge: WhatsAppBridge = hass.data[DOMAIN][entry.entry_id]

    async_add_entities([WhatsAppBridgeConnectionSensor(bridge, entry)], True)


class WhatsAppBridgeConnectionSensor(BinarySensorEntity):
    """Representation of WhatsApp Bridge connection status."""

    _attr_device_class = BinarySensorDeviceClass.CONNECTIVITY
    _attr_has_entity_name = True
    _attr_name = "Connected"

    def __init__(self, bridge: WhatsAppBridge, entry: ConfigEntry) -> None:
        """Initialize the binary sensor."""
        self._bridge = bridge
        self._entry = entry
        self._attr_unique_id = f"{entry.entry_id}_connected"

        # Register callback for state changes
        bridge.set_state_callback(self._handle_state_change)

    async def _handle_state_change(self) -> None:
        """Handle state changes from the bridge."""
        self.async_write_ha_state()

    @property
    def is_on(self) -> bool | None:
        """Return true if the bridge is connected."""
        status = self._bridge.connection_status
        if status == "connected":
            return True
        elif status in ("reconnecting", "error", "disconnected"):
            return False
        elif status == "unavailable":
            # Max retries exceeded
            return None
        return False

    @property
    def available(self) -> bool:
        """Return if entity is available."""
        # Entity is unavailable only when max retries exceeded
        return self._bridge.connection_status != "unavailable"

    @property
    def extra_state_attributes(self) -> dict[str, object]:
        """Return additional state attributes."""
        return {
            "connection_status": self._bridge.connection_status,
            "consecutive_failures": self._bridge._consecutive_failures,
            "host": self._bridge.host,
        }

    @property
    def device_info(self):
        """Return device information."""
        return {
            "identifiers": {(DOMAIN, self._entry.entry_id)},
            "name": "WhatsApp Bridge",
            "manufacturer": "WhatsApp Bridge",
            "model": "WebSocket Bridge",
        }
