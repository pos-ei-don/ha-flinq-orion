"""Short retries for SIG set commands (first requests on a fresh link left unanswered)."""

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from custom_components.flinq_orion.device import SIG_SET_RETRIES, SIG_SET_TIMEOUT, OrionDevice
from custom_components.flinq_orion.device_profiles import DeviceCapabilities
from custom_components.flinq_orion.protocol.access import AccessMessage
from custom_components.flinq_orion.protocol.commands import CommandChannel
from custom_components.flinq_orion.protocol.controller import Response
from custom_components.flinq_orion.protocol.errors import MeshTimeoutError


def _device():
    c = SimpleNamespace(
        entry=SimpleNamespace(options={"transport_mode": "sig", "node_address": 24}),
        capabilities=DeviceCapabilities(),
        data={},
        record=Mock(),
        report_received=Mock(),
        async_set_updated_data=Mock(),
    )
    d = OrionDevice(c)
    d.decode_sig(0x8204, b"\x00")
    d.decode_sig(0x824E, bytes.fromhex("0080"))
    d.decode_sig(0x8263, b"\x00" + (2000).to_bytes(2, "little") + (6500).to_bytes(2, "little"))
    d.decode_sig(0x8278, bytes.fromhex("00800000ffff"))
    d.ensure_connection = AsyncMock()
    d.next_tid = AsyncMock(return_value=9)
    return d, c


@pytest.mark.asyncio
async def test_sig_sets_use_short_attempts_with_retries():
    d, _ = _device()
    seen = []

    async def request(payload, matcher, **kwargs):
        seen.append(kwargs)
        status = {"8202": 0x8204, "824c": 0x824E, "8264": 0x8266, "8276": 0x8278}[payload[:2].hex()]
        body = {
            0x8204: payload[2:3],
            0x824E: payload[2:4],
            0x8266: payload[2:6],
            0x8278: payload[2:8],
        }[status]
        return Response(24, 1, 0, 10, False, AccessMessage(status, None, body))

    d.controller = SimpleNamespace(node=24, request=request)
    await d.set_power(True)
    await d.set_brightness(128)
    await d.set_temperature(3000)
    await d.set_color((120, 100), 255)
    assert len(seen) == 4
    for kwargs in seen:
        assert kwargs["timeout"] == SIG_SET_TIMEOUT
        assert kwargs["retries"] == SIG_SET_RETRIES
        # At least one resend, and all attempts together stay below the old single 15 s wait.
        assert kwargs["retries"] >= 1
        assert kwargs["timeout"] * (kwargs["retries"] + 1) < 15


@pytest.mark.asyncio
async def test_channel_resends_the_same_payload_and_reports_each_retry():
    sent = []
    channel = CommandChannel(None)

    async def send(payload):
        sent.append(payload)
        if len(sent) == 3:  # the lamp answers the third attempt only
            asyncio.get_running_loop().call_soon(channel.receive, "status")

    channel.send = send
    retries = []
    channel.on_retry = retries.append
    result = await channel.request(
        b"\x82\x02\x01\x09\x00\x00",
        lambda response: response == "status",
        timeout=0.05,
        retries=2,
        idempotent=True,
    )
    assert result == "status"
    assert sent == [b"\x82\x02\x01\x09\x00\x00"] * 3  # same TID every time
    assert retries == [1, 2]


@pytest.mark.asyncio
async def test_set_power_failure_is_recorded_after_all_attempts():
    d, c = _device()

    async def request(payload, matcher, **kwargs):
        raise MeshTimeoutError("No matching response")

    d.controller = SimpleNamespace(node=24, request=request)
    with pytest.raises(MeshTimeoutError):
        await d.set_power(True)
    kinds = [call.args[0]["kind"] for call in c.record.call_args_list]
    assert "request_timeout" in kinds


@pytest.mark.asyncio
async def test_discarded_answer_is_logged_only_while_a_request_waits():
    from test_active_mesh import make_system

    from custom_components.flinq_orion.protocol.transport import ReplayWindow

    peer, controller, device, allocator, replay = await make_system()
    await device.query()
    old = list(peer.responses)
    controller.log.reset_mock()

    def dropped():
        return [
            call.args[0]
            for call in controller.log.call_args_list
            if call.args[0].get("kind") == "rx_dropped"
        ]

    controller.network_dedup = ReplayWindow(limit=256)
    controller.completed.clear()
    for kind, pdu in old:  # nobody waits: replayed messages stay out of the history
        await controller.receive(kind, pdu)
    assert dropped() == []

    # A fresh link starts with an empty network dedup window, while replay protection is
    # persistent: an answer the replay window rejects would vanish without a trace.
    controller.network_dedup = ReplayWindow(limit=256)
    controller.completed.clear()  # a new link also starts without cached segment ACKs
    controller.channel.pending = (
        lambda response: False,
        asyncio.get_running_loop().create_future(),
    )
    for kind, pdu in old:
        await controller.receive(kind, pdu)
    controller.channel.pending = None
    reasons = {record["reason"] for record in dropped()}
    assert reasons and reasons <= {"replay_access", "replay_segment", "replay_control"}
    assert all(record["src"] == 176 for record in dropped())
    await peer.finish()
