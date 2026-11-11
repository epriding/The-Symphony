"""Real private Unix-socket acceptance for the runtime-access provider."""
import asyncio
from contextlib import suppress
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


@pytest.mark.asyncio
@pytest.mark.parametrize('field,value', [('request_id','\ud800'),('request_id','\x1b[0m')])
async def test_invalid_unicode_and_terminal_control_identifiers_have_structured_errors(tmp_path,field,value):
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    try:
        reader,writer=await connect(service)
        request=dict(version=1,request_id='x',op='overview',params={},service_id=None,session_id=None)
        request[field]=value
        writer.write((json.dumps(request)+'\n').encode())
        await writer.drain()
        response=json.loads(await asyncio.wait_for(reader.readline(),2))
        assert response['ok'] is False
        assert response['error']['code']=='invalid_request'
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()


@pytest.mark.asyncio
async def test_eight_clients_are_bounded_and_read_leases_are_released(tmp_path):
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    pairs=[]
    try:
        pairs=[await connect(service) for _ in range(8)]
        extra_reader,extra_writer=await connect(service)
        assert await asyncio.wait_for(extra_reader.read(),2)==b''
        extra_writer.close()
        await extra_writer.wait_closed()
        results=await asyncio.gather(*(rpc(r,w) for r,w in pairs))
        assert any(result['ok'] for result in results)
        assert all(result['ok'] or result['error']['code']=='busy' for result in results)
        assert service.memory_highwater<=16*1024*1024
        assert service.client_highwater==8 and service.queue_highwater<=32
        for r,w in pairs:
            w.close()
            await w.wait_closed()
        pairs=[]
        await eventually(lambda: service._query_leases==0 and not service._clients)
    finally:
        for r,w in pairs:
            w.close()
        await service.close()


@pytest.mark.asyncio
async def test_slow_real_socket_readers_do_not_starve_risk_or_stop(tmp_path):
    service,feeds,clock=controlled_service(tmp_path)
    await service.start(tmp_path/'private/control.sock')
    pairs=[]
    try:
        control=await connect(service)
        pairs.append(control)
        r,w=control
        await rpc(r,w,'start',service_id=service.service_id)
        await eventually(lambda: all(f.started.is_set() for f in feeds))
        for feed in feeds:
            await feed.push_book()
        await eventually(lambda: service.runner.status['phase']=='ready')
        target=dict(service_id=service.service_id,session_id=service.session_id)
        for i in range(55):
            result=await rpc(r,w,'publish_finding',params={
                'author':'agent','severity':'info','message':'x'*2000,'evidence':'y'*2000},**target)
            assert result['ok'],result
        slow=[await connect(service) for _ in range(2)]
        pairs+=slow
        await eventually(lambda: len(service._clients)==3)
        # Real AF_UNIX backpressure: reduce the actual server send buffers and
        # leave these client read halves idle during risk/stop assertions.
        for writer in service._clients.values():
            writer.get_extra_info('socket').setsockopt(socket.SOL_SOCKET,socket.SO_SNDBUF,4096)
            writer.transport.set_write_buffer_limits(high=4096,low=1024)
        for reader,writer in slow:
            request=dict(version=1,request_id='slow',op='findings',params={'limit':50},**target)
            writer.write((json.dumps(request)+'\n').encode())
            await writer.drain()
        await eventually(lambda: service._query_leases==2)
        prior=service.observations.event_sequence
        await eventually(lambda: service.observations.event_sequence>prior)
        stop=await rpc(r,w,'stop',**target)
        assert stop['ok'],stop
        await eventually(lambda: service.session_task.done())
        assert service.memory_highwater<=16*1024*1024
    finally:
        for r,w in pairs:
            w.close()
            with suppress(OSError):
                await w.wait_closed()
        await service.close()


@pytest.mark.asyncio
async def test_pending_finding_disk_io_does_not_block_stop(tmp_path,monkeypatch):
    import threading
    service,feeds,clock=controlled_service(tmp_path)
    await service.start(tmp_path/'private/control.sock')
    entered,release=threading.Event(),threading.Event()
    original=service._append_finding
    def blocked(row):
        entered.set()
        release.wait(10)
        return original(row)
    monkeypatch.setattr(service,'_append_finding',blocked)
    publishing=None
    pairs=[]
    try:
        pairs=[await connect(service) for _ in range(2)]
        r,w=pairs[0]
        await rpc(r,w,'start',service_id=service.service_id)
        await eventually(lambda: all(f.started.is_set() for f in feeds))
        target=dict(service_id=service.service_id,session_id=service.session_id)
        publishing=asyncio.create_task(rpc(r,w,'publish_finding',params={
            'author':'agent','severity':'info','message':'pending'},**target))
        await eventually(entered.is_set)
        stop=await rpc(*pairs[1],'stop',**target)
        assert stop['ok'],stop
        await eventually(lambda: service.session_task.done())
        assert service._publication and not service._publication.done()
    finally:
        release.set()
        if publishing:
            await asyncio.gather(publishing,return_exceptions=True)
        for r,w in pairs:
            w.close()
            with suppress(OSError):
                await w.wait_closed()
        await service.close()


@pytest.mark.asyncio
async def test_close_reports_pending_diagnostic_io_even_after_runner_cleanup(tmp_path,monkeypatch):
    import threading
    import src.operator.service as module
    service,feeds,clock=controlled_service(tmp_path)
    await service.start(tmp_path/'private/control.sock')
    entered,release=threading.Event(),threading.Event()
    original=service._append_finding
    def blocked(row):
        entered.set()
        release.wait(10)
        return original(row)
    monkeypatch.setattr(service,'_append_finding',blocked)
    publishing=None
    pair=None
    try:
        pair=await connect(service)
        await rpc(*pair,'start',service_id=service.service_id)
        await eventually(lambda: all(f.started.is_set() for f in feeds))
        for feed in feeds:
            await feed.push_book()
        await eventually(lambda: service.runner.status['phase']=='ready')
        publishing=asyncio.create_task(rpc(*pair,'publish_finding',params={
            'author':'agent','severity':'info','message':'pending'},
            service_id=service.service_id,session_id=service.session_id))
        await eventually(entered.is_set)
        monkeypatch.setattr(module,'DEADLINE',.03)
        with pytest.raises(RuntimeError,match='cleanup'):
            await service.close()
        assert service.session_task.done()
    finally:
        release.set()
        if publishing:
            await asyncio.gather(publishing,return_exceptions=True)
        if pair:
            pair[1].close()
        if service._publication:
            await asyncio.gather(service._publication,return_exceptions=True)
        with suppress(RuntimeError):
            await service.close()


@pytest.mark.asyncio
async def test_service_closes_idle_connections_before_waiting_for_server_close(tmp_path):
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    reader,writer=await connect(service)
    try:
        assert (await rpc(reader,writer))['ok']
        await asyncio.wait_for(service.close(),1)
        assert await asyncio.wait_for(reader.read(),1)==b''
    finally:
        writer.close()
        await writer.wait_closed()
        await service.close()


@pytest.mark.asyncio
async def test_oversized_and_incomplete_wire_frames_close_without_creating_work(tmp_path,monkeypatch):
    import src.operator.service as module
    from src.operator.protocol import MAX_REQUEST
    service=service_type()(valid_config(),output_root=tmp_path/'runs')
    await service.start(tmp_path/'private/control.sock')
    try:
        for frame in (b'x'*(MAX_REQUEST+1)+b'\n', b'{"incomplete":'):
            reader,writer=await connect(service)
            monkeypatch.setattr(module,'DEADLINE',.05)
            writer.write(frame)
            await writer.drain()
            assert await asyncio.wait_for(reader.read(),1)==b''
            writer.close()
            with suppress(OSError):
                await writer.wait_closed()
        assert service.session_id is None and service.queue_highwater==0
    finally:
        await service.close()


def test_response_limit_is_enforced_for_repeated_references_without_a_full_unbounded_dump():
    import tracemalloc
    from src.operator.protocol import encode_response,ProtocolError,QUERY_RESERVE
    row={'message':'a'*2000}
    data={'items':[row]*10000}
    tracemalloc.start()
    try:
        with pytest.raises(ProtocolError,match='limit'):
            encode_response(data)
        _,peak=tracemalloc.get_traced_memory()
        assert peak<QUERY_RESERVE
    finally:
        tracemalloc.stop()


@pytest.mark.asyncio
async def test_resolved_metadata_must_fit_control_identifiers_before_activation(tmp_path,monkeypatch):
    from dataclasses import replace
    service,feeds,clock=controlled_service(tmp_path)
    original=feeds[0].get_market
    async def oversized(identifier):
        return replace(await original(identifier),market_id='x'*257)
    monkeypatch.setattr(feeds[0],'get_market',oversized)
    await service.start(tmp_path/'private/control.sock')
    try:
        reader,writer=await connect(service)
        assert (await rpc(reader,writer,'start',service_id=service.service_id))['ok']
        await eventually(lambda: service.session_task.done())
        assert service.runner.status['phase']=='failed'
        assert 'projection' in service.runner.status['reason'].lower()
        assert all(slot.strategy is None for slot in service.runner.slots)
        writer.close()
        await writer.wait_closed()
    finally:
        await service.close()
