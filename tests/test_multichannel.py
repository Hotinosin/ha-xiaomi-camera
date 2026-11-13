"""Dual lenses share a connection, never a media queue or snapshot source."""

import asyncio
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from bridge.cameras import CameraRegistry
from bridge.config import AccessMode
from bridge.restream import Restreamer, build_config
from bridge.settings import SettingsStore
from bridge.streaming import CameraSession
from miot.types import MIoTCameraStatus, MIoTCameraVideoQuality
from test_restream import make_cameras, make_options

PARAMETERS = b"\x00\x00\x00\x01\x40\x01\x00\x00\x00\x01\x42\x01"


async def test_two_lenses_share_connection_and_isolate_media():
    callbacks = {}
    instance = SimpleNamespace(
        start_async=AsyncMock(),
        stop_async=AsyncMock(),
        get_status_async=AsyncMock(return_value=MIoTCameraStatus.CONNECTED),
    )

    async def register(callback, channel=0):
        callbacks[channel] = callback
        await callback("42", PARAMETERS, 0, 0, channel)

    instance.register_raw_video_async = register
    client = SimpleNamespace(
        create_camera_instance_async=AsyncMock(return_value=instance)
    )
    session = CameraSession(
        client,
        SimpleNamespace(did="42", name="Dual", channel_count=2),
        MIoTCameraVideoQuality.LOW,
        False,
    )
    try:
        async with session.subscribe() as first:
            async with session.channel(1).subscribe() as second:
                assert set(callbacks) == {0, 1}
                client.create_camera_instance_async.assert_awaited_once()
                await callbacks[1]("42", b"lens-two", 123, 1, 1)
                assert first.queue.empty()
                assert (
                    b"lens-two"
                    in (await asyncio.wait_for(second.queue.get(), 1)).payload
                )
            assert not instance.stop_async.await_count
            with pytest.raises(ValueError):
                session.channel(2)
        await session.async_stop()
        instance.stop_async.assert_awaited_once()
        assert session.channel(1).stats.frames == 0
    finally:
        await session.async_stop()


def test_second_lens_is_published_without_changing_existing_urls():
    options = make_options(AccessMode.LOCAL)
    config = build_config(options, make_cameras(["42", "43"]), channels={"42": 2})
    assert config["streams"]["camera_42"] == "http://127.0.0.1:8099/api/stream/42"
    assert (
        config["streams"]["camera_42_original_ch2"]
        == "http://127.0.0.1:8099/api/stream/42?channel=1"
    )
    assert "camera_43_original_ch2" not in config["streams"]
    described = Restreamer(options).stream_descriptions("42", channel_count=2)
    second = next(s for s in described if s["key"] == "original_ch2")
    assert second["channel"] == 1
    assert second["url"].endswith("/camera_42_original_ch2")


async def test_visual_feeder_is_discovered_without_power_properties(tmp_path):
    fields = dict(
        did="feeder",
        name="Feeder",
        model="xiaomi.feeder.iv2002",
        manufacturer="xiaomi",
        online=True,
        lan_online=False,
    )
    device = SimpleNamespace(**fields, model_dump=lambda: fields)
    http = SimpleNamespace(
        get_props_async=AsyncMock(return_value=[]), set_prop_async=AsyncMock()
    )
    client = SimpleNamespace(
        get_devices_async=AsyncMock(return_value={"feeder": device}),
        get_cameras_async=AsyncMock(return_value={}),
        _http_client=http,
    )
    registry = CameraRegistry(client, SettingsStore(tmp_path / "settings.json"))
    cameras = await registry.async_refresh()
    assert [c.did for c in cameras] == ["feeder"]
    assert cameras[0].publishable
    assert cameras[0].powered_on is None
    assert cameras[0].as_dict()["power_supported"] is False
    await registry.async_read_power_state("feeder")
    http.get_props_async.assert_not_awaited()
    with pytest.raises(ValueError):
        await registry.async_set_power("feeder", True)
    http.set_prop_async.assert_not_awaited()


async def test_snapshot_sources_and_caches_are_separate_per_lens(monkeypatch):
    from bridge.stills import Stills, _Source

    async def start(source):
        source._frame = source._url.encode()
        source._frame_at = time.monotonic()
        source._seq = 1

    monkeypatch.setattr(_Source, "async_start", start)
    restreamer = Restreamer(make_options(AccessMode.LOCAL))
    stills = Stills(restreamer.internal_rtsp_url)
    try:
        assert await stills.async_still("42") == b"rtsp://127.0.0.1:8554/camera_42"
        assert await stills.async_still("42", channel=1) == (
            b"rtsp://127.0.0.1:8554/camera_42_original_ch2"
        )
        assert len(stills._sources) == 2
        stills.drop(set())
        assert not stills._sources
    finally:
        await stills.async_shutdown()


@pytest.mark.enable_socket
async def test_channel_requests_are_validated_before_starting_media(tmp_path):
    from aiohttp.test_utils import TestClient, TestServer
    from test_routes import _build_bridge

    api = _build_bridge(tmp_path)
    api._registry_provider = lambda: SimpleNamespace(
        get=lambda did: SimpleNamespace(channel_count=2),
        is_publishable=lambda did: True,
        power_state=lambda did: True,
    )
    api._previews = SimpleNamespace(async_still=AsyncMock(return_value=b"jpeg"))
    async with TestClient(TestServer(api.build_control_app())) as client:
        response = await client.get("/api/snapshot/42?channel=1")
        assert response.status == 200
        assert await response.read() == b"jpeg"
        api._previews.async_still.assert_awaited_once_with("42", None, channel=1)
        for channel in ("-1", "2", "invalid"):
            for route in ("snapshot", "stream"):
                response = await client.get(f"/api/{route}/42?channel={channel}")
                assert response.status == 400
        assert api._previews.async_still.await_count == 1


async def test_preview_sources_are_separate_per_lens(monkeypatch):
    from bridge.stills import Stills, _Source

    async def start(source):
        source._frame = source._url.encode()
        source._frame_at = time.monotonic()
        source._seq = 1

    monkeypatch.setattr(_Source, "async_start", start)
    stills = Stills(Restreamer(make_options(AccessMode.LOCAL)).internal_rtsp_url)
    try:
        _, first = await stills.async_frame("42", 12, "medium")
        _, second = await stills.async_frame("42", 12, "medium", channel=1)
        assert first == b"rtsp://127.0.0.1:8554/camera_42"
        assert second == b"rtsp://127.0.0.1:8554/camera_42_original_ch2"
        assert len(stills._sources) == 2
    finally:
        await stills.async_shutdown()


@pytest.mark.enable_socket
async def test_preview_websocket_routes_and_validates_lenses(tmp_path):
    import asyncio

    from aiohttp import WSServerHandshakeError, web
    from aiohttp.test_utils import TestClient, TestServer
    from test_routes import _build_bridge

    api = _build_bridge(tmp_path)
    api._registry_provider = lambda: SimpleNamespace(
        get=lambda did: SimpleNamespace(channel_count=2),
        is_publishable=lambda did: True,
        power_state=lambda did: True,
    )
    calls = []

    async def frame(did, fps, quality, after=0, channel=0):
        calls.append((did, channel))
        if after:
            await asyncio.Event().wait()
        return 1, f"lens-{channel}".encode()

    api._previews = SimpleNamespace(async_frame=frame)
    app = web.Application()
    app.router.add_get("/api/preview/{did}/ws", api._preview_ws)
    async with TestClient(TestServer(app)) as client:
        async with (
            client.ws_connect("/api/preview/42/ws") as first,
            client.ws_connect("/api/preview/42/ws?channel=1") as second,
        ):
            assert (await first.receive()).data == b"lens-0"
            assert (await second.receive()).data == b"lens-1"
        for channel in ("-1", "2", "invalid"):
            with pytest.raises(WSServerHandshakeError) as error:
                await client.ws_connect(f"/api/preview/42/ws?channel={channel}")
            assert error.value.status == 400
        assert {channel for _, channel in calls} == {0, 1}
