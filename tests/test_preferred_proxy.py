"""A lamp marked as preferred carries the link whenever it is reachable."""

from types import SimpleNamespace
from unittest.mock import patch

from custom_components.flinq_orion.session import MeshSession


def _lamp(address, node, seen, preferred=False):
    return SimpleNamespace(
        address=address,
        node_address=node,
        last_monotonic=seen,
        entry=SimpleNamespace(options={"preferred_proxy": preferred} if preferred else {}),
    )


def _session(lamps):
    session = object.__new__(MeshSession)
    session.hass = object()
    session.lamps = lamps
    return session


def _target(session, lamp, reachable, direct=False):
    with patch(
        "custom_components.flinq_orion.session.bluetooth.async_ble_device_from_address",
        side_effect=lambda hass, address, connectable: object() if address in reachable else None,
    ):
        return session._target(lamp, direct)


def test_preferred_lamp_carries_the_link_for_the_other_lamp():
    lamp1 = _lamp("AA:01", 16, 1.0, preferred=True)
    lamp2 = _lamp("AA:02", 24, 2.0)
    session = _session([lamp1, lamp2])
    # Without a preference the commanded lamp itself would carry the link.
    assert _target(session, lamp2, {"AA:01", "AA:02"}) is lamp1


def test_without_preference_the_commanded_lamp_carries_the_link():
    lamp1 = _lamp("AA:01", 16, 1.0)
    lamp2 = _lamp("AA:02", 24, 2.0)
    assert _target(_session([lamp1, lamp2]), lamp2, {"AA:01", "AA:02"}) is lamp2


def test_unreachable_preferred_lamp_falls_back():
    lamp1 = _lamp("AA:01", 16, 1.0, preferred=True)
    lamp2 = _lamp("AA:02", 24, 2.0)
    assert _target(_session([lamp1, lamp2]), lamp2, {"AA:02"}) is lamp2


def test_direct_connection_ignores_the_preference():
    lamp1 = _lamp("AA:01", 16, 1.0, preferred=True)
    lamp2 = _lamp("AA:02", 24, 2.0)
    assert _target(_session([lamp1, lamp2]), lamp2, {"AA:01", "AA:02"}, direct=True) is lamp2
