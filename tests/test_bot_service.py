"""Real private Unix-socket acceptance for the runtime-access provider."""
import asyncio
import json
import os
from pathlib import Path
import socket

import pytest

from bot_v1_support import valid_config, ControlledFeed, VENUES, eventually, strategy_config


async def connect(service):
    return await asyncio.open_unix_connection(str(service.socket_path))


async def rpc(reader, writer, op='overview', *, service_id=None, session_id=None,
              params=None, request_id='test'):
    writer.write((json.dumps(dict(version=1, request_id=request_id, op=op,
        service_id=service_id, session_id=session_id, params=params or {}))+'\n').encode())
    await writer.drain()
    return json.loads(await asyncio.wait_for(reader.readline(), 2))


def service_type():
    from src.operator.service import ControlService
    return ControlService


@pytest.mark.asyncio
async def test_idle_attach_is_inert_and_private(tmp_path, monkeypatch):
    import dotenv
    monkeypatch.setattr(dotenv, 'load_dotenv', lambda **kwargs: pytest.fail('idle credential read'))
    service = service_type()(valid_config(strategies=[], source='public', observe_only=True),
        output_root=tmp_path / 'runs')
    await service.start(tmp_path / 'private/control.sock')
    try:
        reader, writer = await connect(service)
        response = await rpc(reader, writer)
        assert response['ok'] and response['data']['lifecycle'] == 'idle'
        assert response['session_id'] is None
        assert response['data']['live_execution_supported'] is False
        assert not (tmp_path / 'runs').exists()
        assert service.socket_path.stat().st_mode & 0o777 == 0o600
        assert service.socket_path.parent.stat().st_mode & 0o777 == 0o700
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()
    assert not service.socket_path.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize('payload', [
    b'{"version":1,"version":1}\n',
    b'{"version":NaN}\n',
    b'not-json\n',
    b'\xff\n',
    json.dumps(dict(version=True, request_id='x', op='overview', params={},
        service_id=None, session_id=None)).encode()+b'\n',
    json.dumps(dict(version=1, request_id='x', op='overview', params={'shell':'bad'},
        service_id=None, session_id=None)).encode()+b'\n',
])
async def test_malformed_wire_request_is_rejected_without_starting_a_run(tmp_path, payload):
    service = service_type()(valid_config(), output_root=tmp_path / 'runs')
    await service.start(tmp_path / 'private/control.sock')
    try:
        reader, writer = await connect(service)
        writer.write(payload)
        await writer.drain()
        response = json.loads(await asyncio.wait_for(reader.readline(), 2))
        assert response['ok'] is False
        assert response['error']['code'] == 'invalid_request'
        assert service.session_id is None
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_existing_socket_symlink_or_insecure_directory_is_never_taken_over(tmp_path):
    cls = service_type()
    private = tmp_path / 'private'
    private.mkdir(mode=0o700)
    path = private / 'control.sock'
    original = socket.socket(socket.AF_UNIX)
    original.bind(str(path))
    try:
        with pytest.raises(ValueError):
            await cls(valid_config(), output_root=tmp_path/'runs').start(path)
        assert path.exists()
    finally:
        original.close()
        path.unlink()
    path.symlink_to(tmp_path/'elsewhere')
    with pytest.raises(ValueError):
        await cls(valid_config(), output_root=tmp_path/'runs').start(path)
    assert path.is_symlink()
    path.unlink()
    private.chmod(0o755)
    with pytest.raises(ValueError):
        await cls(valid_config(), output_root=tmp_path/'runs').start(path)


@pytest.mark.asyncio
async def test_close_does_not_unlink_replacement_entry(tmp_path):
    service = service_type()(valid_config(), output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    service.socket_path.unlink()
    service.socket_path.write_text('foreign replacement')
    await service.close()
    assert service.socket_path.read_text() == 'foreign replacement'


@pytest.mark.asyncio
async def test_peer_uid_policy_rejects_foreign_peer(tmp_path, monkeypatch):
    cls = service_type()
    service = cls(valid_config(), output_root=tmp_path/'runs')
    monkeypatch.setattr(service, '_peer_uid', lambda writer: os.getuid()+1)
    await service.start(tmp_path/'private/control.sock')
    try:
        reader, writer = await connect(service)
        assert await asyncio.wait_for(reader.read(), 2) == b''
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()


def controlled_service(tmp_path):
    from src.bot import BotRunner
    from src.paper_trading.clock import VirtualClock
    from bot_v1_support import START
    clock = VirtualClock(START)
    feeds = [ControlledFeed(v, clock=clock) for v in VENUES]
    config = valid_config(strategies=[strategy_config('alpha', factory='quiet')])

    class Quiet:
        def __init__(self, context):
            pass
        async def on_snapshot(self, snapshot):
            return None

    def factory(config, **kwargs):
        return BotRunner(config, **kwargs, feeds=feeds, clock=clock,
            strategy_factories={'quiet': Quiet})
    return service_type()(config, output_root=tmp_path/'runs', runner_factory=factory), feeds, clock


@pytest.mark.asyncio
async def test_one_run_survives_detach_and_rejects_stale_targets(tmp_path):
    service, feeds, clock = controlled_service(tmp_path)
    await service.start(tmp_path/'private/control.sock')
    try:
        reader, writer = await connect(service)
        sid = (await rpc(reader, writer))['service_id']
        first = await rpc(reader, writer, 'start', service_id=sid)
        assert first['ok'], first
        session = first['session_id']
        duplicate = await rpc(reader, writer, 'start', service_id=sid)
        assert duplicate['error']['code'] == 'conflict'
        await eventually(lambda: all(f.started.is_set() for f in feeds))
        writer.close()
        await writer.wait_closed()
        for feed in feeds:
            await feed.push_book()
        await eventually(lambda: service.runner.status['phase'] == 'ready')
        reader, writer = await connect(service)
        resumed_view = await rpc(reader, writer, service_id=sid, session_id=session)
        assert resumed_view['session_id'] == session
        assert resumed_view['data']['lifecycle'] == 'ready'
        wrong = await rpc(reader, writer, 'stop', service_id=sid, session_id='foreign')
        assert wrong['error']['code'] == 'conflict'
        assert not service.runner._stop.is_set()
        paused = await rpc(reader, writer, 'pause', service_id=sid, session_id=session)
        assert paused['ok'], paused
        await eventually(lambda: service.runner.operator_state == 'paused')
        resumed = await rpc(reader, writer, 'resume', service_id=sid, session_id=session)
        assert resumed['ok']
        await eventually(lambda: service.runner.operator_state == 'running')
        stopped = await rpc(reader, writer, 'stop', service_id=sid, session_id=session)
        assert stopped['ok']
        await eventually(lambda: service.session_task.done())
        terminal = await rpc(reader, writer, service_id=sid, session_id=session)
        assert terminal['data']['lifecycle'] == 'stopped'
        assert (service.runner.output_directory/'manifest.json').exists()
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_two_concurrent_smoke_starts_admit_one_and_next_run_is_fresh(tmp_path):
    service = service_type()(output_root=tmp_path/'runs', smoke=True)
    await service.start(tmp_path/'private/control.sock')
    try:
        pairs = [await connect(service) for _ in range(2)]
        results = await asyncio.gather(*(rpc(r,w,'start',service_id=service.service_id)
            for r,w in pairs))
        assert sum(r['ok'] for r in results) == 1
        first = service.session_id
        await eventually(lambda: service.session_task.done())
        assert service.runner.status['phase'] == 'stopped'
        r,w = pairs[0]
        second = await rpc(r,w,'start',service_id=service.service_id,session_id=first)
        assert second['ok'], second
        assert service.session_id != first
        await eventually(lambda: service.session_task.done())
        assert len(list((tmp_path/'runs').iterdir())) == 2
        for r,w in pairs:
            w.close()
            await w.wait_closed()
    finally:
        await service.close()
